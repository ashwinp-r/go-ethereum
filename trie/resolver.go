package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

var emptyHash [32]byte

// Verifies that hashes loaded from the hashfile match with the root
func (t *Trie) rebuildFromHashes(dbr DatabaseReader) (root node, roothash hashNode, err error) {
	startTime := time.Now()
	var vertical [6]*fullNode
	var fillCount [6]int // couting number of children for determining whether we need fullNode, shortNode, or nothing
	var lastFill [6]node
	var lastFillIdx [6]byte
	var lastFull [6]bool
	var shorts [6]*shortNode
	hasher := newHasher(t.encodeToBytes)
	for i := 0; i < 1024*1024; i++ {
		hashBytes := dbr.GetHash(uint32(i))
		var hash node
		hash = hashNode(hashBytes)
		var short *shortNode
		fullNodeHash := false
		for level := 4; level >= 0; level-- {
			var v int
			switch level {
			case 4:
				v = i&0xf
			case 3:
				v = (i>>4)&0xf
			case 2:
				v = (i>>8)&0xf
			case 1:
				v = (i>>12)&0xf
			case 0:
				v = (i>>16)&0xf
			}
			if vertical[level] == nil {
				vertical[level] = &fullNode{}
			}
			if h, ok := hash.(hashNode); ok && bytes.Equal(h, emptyHash[:]) {
				vertical[level].Children[v] = nil
			} else {
				vertical[level].Children[v] = hash
				lastFill[level], hash = hash, nil
				lastFillIdx[level] = byte(v)
				lastFull[level], fullNodeHash = fullNodeHash, false
				shorts[level], short = short, nil
				fillCount[level]++
			}
			if v != 15 {
				break
			}
			// We filled up 16 cells, check how many are not empty
			if fillCount[level] == 0 {
				hash = hashNode(emptyHash[:])
				short = nil
				fullNodeHash = false
			} else if fillCount[level] == 1 {
				if lastFull[level] {
					// lastFill was a fullNode
					short = &shortNode{Key: hexToCompact([]byte{lastFillIdx[level]}), Val: lastFill[level]}
					hash = short
				} else if shorts[level] != nil {
					// lastFill was a short node which needs to be extended
					short = &shortNode{Key: hexToCompact(append([]byte{lastFillIdx[level]}, compactToHex(shorts[level].Key)...)), Val: shorts[level].Val}
					hash = short
				} else {
					hash = lastFill[level]
				}
				fullNodeHash = false
			} else {
				short = nil
				shorts[level] = nil
				hash = vertical[level]
				fullNodeHash = true
			}
			lastFill[level] = nil
			lastFull[level] = false
			fillCount[level] = 0
			vertical[level] = nil
			if level == 0 {
				root = hash
			}
		}
	}
	if root != nil {
		hn, err := hasher.hash(root, true)
		if err != nil {
			return root, nil, err
		}
		roothash, _ = hn.(hashNode)
	}
	log.Debug("rebuildFromHashes took %v\n", time.Since(startTime))
	return root, roothash, nil
}

func (t *Trie) Rebuild(db ethdb.Database, blockNr uint64) hashNode {
	if t.root == nil {
		return nil
	}
	n, ok := t.root.(hashNode)
	if !ok {
		panic("Expected hashNode")
	}
	root, roothash, err := t.rebuildFromHashes(db)
	if err != nil {
		panic(err)
	}
	if bytes.Equal(roothash, n) {
		t.relistNodes(root)
		t.root = root
		log.Info("Successfuly loaded from hashfile", "nodes", t.nodeList.Len())
	} else {
		_, hn, err := t.rebuildHashes(db, nil, 0, blockNr, true)
		if err != nil {
			panic(err)
		}
		root, roothash, err = t.rebuildFromHashes(db)
		if err != nil {
			panic(err)
		}
		if bytes.Equal(roothash, hn) {
			t.relistNodes(root)
			t.root = root
			log.Info("Rebuilt hashfile and verified", "nodes", t.nodeList.Len())
		} else {
			log.Error(fmt.Sprintf("Could not rebuild %s vs %s\n", roothash, hn))
		}
	}
	return roothash
}

const Levels = 64

type rebuidData struct {
	dbw ethdb.Putter
	key [32]byte
	value []byte
	resolveHex []byte
	pos int
	hashes bool
	key_set bool
	startLevel int
	nodeStack [Levels+1]node
	vertical [Levels+1]fullNode
	fillCount [Levels+1]int
}

func (r *rebuidData) rebuildInsert(k, v []byte, h *hasher) error {
	if k == nil || len(v) > 0 {
		// First, finish off the previous key
		if r.key_set {
			pLen := prefixLen(k, r.key[:])
			stopLevel := 2*pLen
			if k != nil && (k[pLen]^r.key[pLen])&0xf0 == 0 {
				stopLevel++
			}
			startLevel := r.startLevel
			if startLevel < stopLevel {
				startLevel = stopLevel
			}
			hex := keybytesToHex(r.key[:])
			r.nodeStack[startLevel+1] = &shortNode{Key: hexToCompact(hex[startLevel+1:]), Val: valueNode(r.value)}
			r.fillCount[startLevel+1] = 1
			rhIndex := prefixLen(hex, r.resolveHex)
			for level := startLevel; level >= stopLevel; level-- {
				keynibble := hex[level]
				onResolvingPath := level < rhIndex
				var hashIdx uint32
				if r.hashes && level <= 4 {
					hashIdx = binary.BigEndian.Uint32(r.key[:4]) >> 12
				}
				if r.fillCount[level+1] == 1 {
					// Short node, needs to be promoted to the level above
					short, ok := r.nodeStack[level+1].(*shortNode)
					var newShort *shortNode
					if ok {
						newShort = &shortNode{Key: hexToCompact(append([]byte{keynibble}, compactToHex(short.Key)...)), Val: short.Val}
					} else {
						// r.nodeStack[level+1] is a value node
						newShort = &shortNode{Key: hexToCompact([]byte{keynibble, 16}), Val: r.nodeStack[level+1]}
					}
					var hn node
					var err error
					if short != nil && (!onResolvingPath || r.hashes && level <= 4 && compactLen(short.Key) + level >= 4) {
						short.setcache(nil)
						hn, err = h.hash(short, false)
						if err != nil {
							return err
						}
					}
					if onResolvingPath {
						r.vertical[level].Children[keynibble] = short
					} else {
						r.vertical[level].Children[keynibble] = hn
					}
					r.nodeStack[level] = newShort
					r.fillCount[level]++
					if short != nil && r.hashes && level <= 4 && compactLen(short.Key) + level >= 4 {
						hash, ok := hn.(hashNode)
						if !ok {
							return fmt.Errorf("trie.rebuildInsert: Expected hashNode")
						}
						r.dbw.PutHash(hashIdx, hash)
					}
					if level >= r.pos {
						r.nodeStack[level+1] = nil
						r.fillCount[level+1] = 0
						for i := 0; i < 17; i++ {
							r.vertical[level+1].Children[i] = nil
						}
					}
					continue
				}
				r.vertical[level+1].setcache(nil)
				hn, err := h.hash(&r.vertical[level+1], false)
				if err != nil {
					return err
				}
				if r.hashes && level == 4 {
					hash, ok := hn.(hashNode)
					if !ok {
						return fmt.Errorf("trie.rebuildInsert: hashNode expected")
					}
					r.dbw.PutHash(hashIdx, hash)
				}
				if onResolvingPath {
					c := r.vertical[level+1].copy()
					r.vertical[level].Children[keynibble] = c
					r.nodeStack[level] = &shortNode{Key: hexToCompact([]byte{keynibble}), Val: c}
				} else {
					r.vertical[level].Children[keynibble] = hn
					r.nodeStack[level] = &shortNode{Key: hexToCompact([]byte{keynibble}), Val: hn}
				}
				r.fillCount[level]++
				if level >= r.pos {
					r.nodeStack[level+1] = nil
					r.fillCount[level+1] = 0
					for i := 0; i < 17; i++ {
						r.vertical[level+1].Children[i] = nil
					}
				}
			}
			r.startLevel = stopLevel
			//if r.startLevel < r.pos {
			//	r.startLevel = r.pos
			//}
		}
		if k != nil {
			// Remember the current key and value
			copy(r.key[:], k[:32])
			r.value = common.CopyBytes(v)
			r.key_set = true
		}
	}
	return nil
}

type ResolveHexes [][]byte

// ResolveHexes implements sort.Interface
func (rh ResolveHexes) Len() int {
	return len(rh)
}

func (rh ResolveHexes) Less(i, j int) bool {
	return bytes.Compare(rh[i], rh[j]) < 0
}

func (rh ResolveHexes) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
}

/* One resolver per trie (prefix) */
type TrieResolver struct {
	t *Trie
	dbw ethdb.Putter // For updating hashes
	hashes bool
	continuations []*TrieContinuation
	resolveHexes ResolveHexes
	rhIndexLte int // index in resolveHexes with resolve key less or equal to the current key
	               // if the current key is less than the first resolve key, this index is -1
	rhIndexGt int // index in resolveHexes with resolve key greater than the current key
	              // if the current key is greater than the last resolve key, this index is len(resolveHexes)
	contIndices []int // Indices pointing back to continuation array from arrays retured by PrepareResolveParams
	key [32]byte
	value []byte
	key_set bool
	nodeStack [Levels+1]node
	vertical [Levels+1]fullNode
	fillCount [Levels+1]int
	startLevel int
	h *hasher
}

func (t *Trie) NewResolver(dbw ethdb.Putter) *TrieResolver {
	tr := TrieResolver{
		t: t,
		dbw: dbw,
		continuations: []*TrieContinuation{},
		resolveHexes: [][]byte{},
		rhIndexLte: -1,
		rhIndexGt: 0,
		contIndices: []int{},
		h: newHasher(t.encodeToBytes),
	}
	return &tr
}

// TrieResolver implements sort.Interface
func (tr *TrieResolver) Len() int {
	return len(tr.continuations)
}

func min(a, b int) int {
	if a < b {
		return a
    }
    return b
}

func (tr *TrieResolver) Less(i, j int) bool {
	m := min(tr.continuations[i].resolvePos, tr.continuations[j].resolvePos)
	c := bytes.Compare(tr.continuations[i].resolveKey[:m], tr.continuations[j].resolveKey[:m])
	if c != 0 {
		return c < 0
	}
	return tr.continuations[i].resolvePos < tr.continuations[j].resolvePos
}

func (tr *TrieResolver) Swap(i, j int) {
	tr.continuations[i], tr.continuations[j] = tr.continuations[j], tr.continuations[i]
}

func (tr *TrieResolver) AddContinuation(c *TrieContinuation) {
	tr.continuations = append(tr.continuations, c)
	tr.resolveHexes = append(tr.resolveHexes, c.resolveKey)
}

// Prepares information for the MultiSuffixWalk
func (tr *TrieResolver) PrepareResolveParams() ([][]byte, []uint) {
	// Remove continuations strictly contained in the preceeding ones
	startkeys := [][]byte{}
	fixedbits := []uint{}
	if len(tr.continuations) == 0 {
		return startkeys, fixedbits
	}
	sort.Sort(tr)
	sort.Sort(tr.resolveHexes)
	var prevC *TrieContinuation
	for i, c := range tr.continuations {
		if prevC == nil || c.resolvePos < prevC.resolvePos || !bytes.HasPrefix(c.resolveKey[:c.resolvePos], prevC.resolveKey[:prevC.resolvePos]) {
			tr.contIndices = append(tr.contIndices, i)
			key := make([]byte, 32)
			decodeNibbles(c.resolveKey[:c.resolvePos], key)
			startkeys = append(startkeys, key)
			fixedbits = append(fixedbits, uint(4*c.resolvePos))
			prevC = c
		}
	}
	tr.startLevel = tr.continuations[0].resolvePos
	return startkeys, fixedbits
}

func (tr *TrieResolver) Walker(keyIdx int, k []byte, v []byte) (bool, error) {
	if k == nil || len(v) > 0 {
		// First, finish off the previous key
		if tr.key_set {
			pLen := prefixLen(k, tr.key[:])
			stopLevel := 2*pLen
			if k != nil && (k[pLen]^tr.key[pLen])&0xf0 == 0 {
				stopLevel++
			}
			startLevel := tr.startLevel
			if startLevel < stopLevel {
				startLevel = stopLevel
			}
			hex := keybytesToHex(tr.key[:])
			tr.nodeStack[startLevel+1] = &shortNode{Key: hexToCompact(hex[startLevel+1:]), Val: valueNode(tr.value)}
			tr.fillCount[startLevel+1] = 1
			// Adjust rhIndices if needed
			if tr.rhIndexGt < tr.resolveHexes.Len() {
				resComp := bytes.Compare(hex, tr.resolveHexes[tr.rhIndexGt])
				for tr.rhIndexGt < tr.resolveHexes.Len() && resComp == 1 {
					tr.rhIndexGt++
					tr.rhIndexLte++
					if tr.rhIndexGt < tr.resolveHexes.Len() {
						resComp = bytes.Compare(hex, tr.resolveHexes[tr.rhIndexGt])
					}
				}
			}
			var rhPrefixLenLte, rhPrefixLenGt int
			if tr.rhIndexLte >= 0 {
				rhPrefixLenLte = prefixLen(hex, tr.resolveHexes[tr.rhIndexLte])
			}
			if tr.rhIndexGt < tr.resolveHexes.Len() {
				rhPrefixLenGt = prefixLen(hex, tr.resolveHexes[tr.rhIndexGt])
			}
			rhPrefixLen := rhPrefixLenLte
			if rhPrefixLenGt > rhPrefixLen {
				rhPrefixLen = rhPrefixLenGt
			}
			var tc *TrieContinuation
			if k != nil {
				tc = tr.continuations[tr.contIndices[keyIdx]]
			}
			for level := startLevel; level >= stopLevel; level-- {
				keynibble := hex[level]
				onResolvingPath := level < rhPrefixLen
				//onResolvingPath := true
				var hashIdx uint32
				if tr.hashes && level <= 4 {
					hashIdx = binary.BigEndian.Uint32(tr.key[:4]) >> 12
				}
				if tr.fillCount[level+1] == 1 {
					// Short node, needs to be promoted to the level above
					short, ok := tr.nodeStack[level+1].(*shortNode)
					var newShort *shortNode
					if ok {
						newShort = &shortNode{Key: hexToCompact(append([]byte{keynibble}, compactToHex(short.Key)...)), Val: short.Val}
					} else {
						// r.nodeStack[level+1] is a value node
						newShort = &shortNode{Key: hexToCompact([]byte{keynibble, 16}), Val: tr.nodeStack[level+1]}
					}
					var hn node
					var err error
					if short != nil && (!onResolvingPath || tr.hashes && level <= 4 && compactLen(short.Key) + level >= 4) {
						//short.setcache(nil)
						hn, err = tr.h.hash(short, false)
						if err != nil {
							return false, err
						}
					}
					if onResolvingPath {
						tr.vertical[level].Children[keynibble] = short
					} else {
						tr.vertical[level].Children[keynibble] = hn
					}
					tr.nodeStack[level] = newShort
					tr.fillCount[level]++
					if short != nil && tr.hashes && level <= 4 && compactLen(short.Key) + level >= 4 {
						hash, ok := hn.(hashNode)
						if !ok {
							return false, fmt.Errorf("trie.rebuildInsert: Expected hashNode")
						}
						tr.dbw.PutHash(hashIdx, hash)
					}
					if tc != nil && level >= tc.resolvePos {
						tr.nodeStack[level+1] = nil
						tr.fillCount[level+1] = 0
						for i := 0; i < 17; i++ {
							tr.vertical[level+1].Children[i] = nil
						}
					}
					continue
				}
				tr.vertical[level+1].setcache(nil)
				hn, err := tr.h.hash(&tr.vertical[level+1], false)
				if err != nil {
					return false, err
				}
				// TODO: Write the hash with index (r.key>>4) if level+1 == 5
				if tr.hashes && level == 4 {
					hash, ok := hn.(hashNode)
					if !ok {
						return false, fmt.Errorf("trie.rebuildInsert: hashNode expected")
					}
					tr.dbw.PutHash(hashIdx, hash)
				}
				if onResolvingPath {
					c := tr.vertical[level+1].copy()
					c.setcache(nil)
					tr.vertical[level].Children[keynibble] = c
					tr.nodeStack[level] = &shortNode{Key: hexToCompact([]byte{keynibble}), Val: c}
				} else {
					tr.vertical[level].Children[keynibble] = hn
					tr.nodeStack[level] = &shortNode{Key: hexToCompact([]byte{keynibble}), Val: hn}
				}
				tr.fillCount[level]++
				if tc != nil && level >= tc.resolvePos {
					tr.nodeStack[level+1] = nil
					tr.fillCount[level+1] = 0
					tr.vertical[level+1].setcache(nil)
					for i := 0; i < 17; i++ {
						tr.vertical[level+1].Children[i] = nil
					}
				}
			}
			tr.startLevel = stopLevel
			if tc != nil && tr.startLevel < tc.resolvePos {
				tr.startLevel = tc.resolvePos
			}
		}
		if k != nil {
			// Remember the current key and value
			copy(tr.key[:], k[:32])
			tr.value = common.CopyBytes(v)
			tr.key_set = true
		}
	}
	return true, nil
}

func (tr *TrieResolver) ResolveWithDb(db ethdb.Database, blockNr uint64) error {

	startkeys, fixedbits := tr.PrepareResolveParams()
	//if len(tr.continuations) != len(startkeys) {
		//fmt.Printf("ResolveWithDb, len(continuations)=%d\n", len(tr.continuations))
		//fmt.Printf("ResolveWithDb, groups=%d\n", len(startkeys))
	//}
	//for i := 0; i < tr.resolveHexes.Len(); i++ {
	//	fmt.Printf("resolveHexes[%d]=%x\n", i, tr.resolveHexes[i])
	//}
	for idx, startkey := range startkeys {
		//if len(tr.continuations) != len(startkeys) {
		//	fmt.Printf("%d: startkey %x, fixedbits %d, contIndex %d, rhIndexLte %d, tr.rhIndexGt %d\n",
		//		idx, startkey, fixedbits[idx], tr.contIndices[idx], tr.rhIndexLte, tr.rhIndexGt)
		//}
		tc := tr.continuations[tr.contIndices[idx]]
		tr.key_set = false
		tr.value = nil
		tr.startLevel = tc.resolvePos
		for i := 0; i <= Levels; i++ {
			tr.nodeStack[i] = nil
			tr.vertical[i].setcache(nil)
			for j := 0; j<17; j++ {
				tr.vertical[i].Children[j] = nil
			}
			tr.fillCount[i] = 0
		}
		err := db.WalkAsOf(tr.t.prefix, startkey, fixedbits[idx], blockNr, func(k, v []byte) (bool, error) {
			return tr.Walker(idx, k, v)
		})
		if err != nil {
			return err
		}
		if _, err := tr.Walker(-1, nil, nil); err != nil {
			return err
		}
		var root node
		if tr.fillCount[tc.resolvePos] == 1 {
			root = tr.nodeStack[tc.resolvePos]
		} else if tr.fillCount[tc.resolvePos] > 1 {
			root = tr.vertical[tc.resolvePos].copy()
		}
		if root == nil {
			return fmt.Errorf("Resolve returned nil root")
		}
		hash, err := tr.h.hash(root, tc.resolvePos == 0)
		if err != nil {
			return err
		}
		gotHash, ok := hash.(hashNode)
		if ok {
			if !bytes.Equal(tc.resolveHash, gotHash) {
				return fmt.Errorf("Resolving wrong hash for prefix %x, trie prefix %x\nexpected %s, got %s\n",
					tc.resolveKey[:tc.resolvePos], tc.t.prefix, tc.resolveHash, gotHash)
			}
		} else {
			if tc.resolveHash != nil {
				return fmt.Errorf("Resolving wrong hash for prefix %x, trie prefix %x\nexpected %s, got embedded node\n",
					tc.resolveKey[:tc.resolvePos], tc.t.prefix, tc.resolveHash)
			}
		}
		tc.resolved = root
	}
	return nil
}

func (tc *TrieContinuation) ResolveWithDb(db ethdb.Database, blockNr uint64) error {
	var start [32]byte
	decodeNibbles(tc.resolveKey[:tc.resolvePos], start[:])
	r := rebuidData{dbw: nil, pos: tc.resolvePos, resolveHex: tc.resolveKey, hashes: false, startLevel: tc.resolvePos}
	h := newHasher(tc.t.encodeToBytes)
	err := db.WalkAsOf(tc.t.prefix, start[:], uint(4*tc.resolvePos), blockNr, func(k, v []byte) (bool, error) {
		if len(v) > 0 {
			if err := r.rebuildInsert(k, v, h); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if err := r.rebuildInsert(nil, nil, h); err != nil {
		return err
	}
	var root node
	if r.fillCount[tc.resolvePos] == 1 {
		root = r.nodeStack[tc.resolvePos]
	} else if r.fillCount[tc.resolvePos] > 1 {
		root = &r.vertical[tc.resolvePos]
	}
	if root == nil {
		return fmt.Errorf("Resolve returned nil root")
	}
	hash, err := h.hash(root, tc.resolvePos == 0)
	if err != nil {
		return err
	}
	gotHash, ok := hash.(hashNode)
	if ok {
		if !bytes.Equal(tc.resolveHash, gotHash) {
			return fmt.Errorf("Resolving wrong hash for prefix %x, trie prefix %x\nexpected %s, got %s\n",
				tc.resolveKey[:tc.resolvePos], tc.t.prefix, tc.resolveHash, gotHash)
		}
	} else {
		if tc.resolveHash != nil {
			return fmt.Errorf("Resolving wrong hash for prefix %x, trie prefix %x\nexpected %s, got embedded node\n",
				tc.resolveKey[:tc.resolvePos], tc.t.prefix, tc.resolveHash)
		}
	}
	tc.resolved = root
	return nil
}

func (t *Trie) rebuildHashes(db ethdb.Database, key []byte, pos int, blockNr uint64, hashes bool) (node, hashNode, error) {
	var start [32]byte
	decodeNibbles(key[:pos], start[:])
	r := rebuidData{dbw: db, pos:pos, resolveHex: key, hashes: hashes}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	err := db.WalkAsOf(t.prefix, start[:], uint(4*pos), blockNr, func(k, v []byte) (bool, error) {
		if len(v) > 0 {
			return true, r.rebuildInsert(k, v, h)
		}
		return true, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if err :=r.rebuildInsert(nil, nil, h); err != nil {
		return nil, nil, err
	}
	var root node
	if r.fillCount[pos] == 1 {
		root = r.nodeStack[pos]
	} else if r.fillCount[pos] > 1 {
		root = &r.vertical[pos]
	}
	if err == nil {
		var gotHash hashNode
		if root != nil {
			hash, _ := h.hash(root, pos == 0)
			gotHash = hash.(hashNode)
			return root, gotHash, nil
		}
		return root, gotHash, nil
	} else {
		fmt.Printf("Error resolving hash: %s\n", err)
	}
	return root, nil, err
}
