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
	resolvingKey []byte
	pos int
	hashes bool
	key_set bool
	startLevel int
	nodeStack [Levels+1]node
	vertical [Levels+1]fullNode
	fillCount [Levels+1]int
	//newway bool
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
			//if r.newway {
				hex := keybytesToHex(r.key[:])
				r.nodeStack[startLevel+1] = &shortNode{Key: hexToCompact(hex[startLevel+1:]), Val: valueNode(r.value)}
				r.fillCount[startLevel+1]++
				r.vertical[startLevel+1].Children[hex[startLevel]] = &shortNode{Key: hexToCompact(hex[startLevel+1:]), Val: valueNode(r.value)}
			//} else {
			//	startLevel = Levels-1
			//	r.nodeStack[Levels] = valueNode(r.value)
			//	r.fillCount[Levels] = 1				
			//}
			//if r.newway {
				//fmt.Printf("startLevel: %d, stopLevel: %d, pos: %d\n", startLevel, stopLevel, r.pos)
			//}
			for level := startLevel; level >= stopLevel; level-- {
				idx := level >> 1
				var keynibble byte
				if level&1 == 0 {
					keynibble = r.key[idx]>>4
				} else {
					keynibble = r.key[idx]&0xf
				}
				keybytes := int((4*(level+1) + 7)/8)
				shiftbits := uint((4*(level+1))&7)
				mask := byte(0xff)
				if shiftbits != 0 {
					mask = 0xff << (8-shiftbits)
				}
				onResolvingPath := false
				if r.resolvingKey != nil {
					onResolvingPath =
						bytes.Equal(r.key[:keybytes-1], r.resolvingKey[:keybytes-1]) &&
						(r.key[keybytes-1]&mask)==(r.resolvingKey[keybytes-1]&mask)
				}
				var hashIdx uint32
				if r.hashes && level <= 4 {
					hashIdx = binary.BigEndian.Uint32(r.key[:4]) >> 12
				}
				//if r.newway {
					//fmt.Printf("level %d, keynibble %d, r.fillCount[level+1] %d\n", level, keynibble, r.fillCount[level+1])
				//}
				if r.fillCount[level+1] == 1 {
					// Short node, needs to be promoted to the level above
					short, ok := r.nodeStack[level+1].(*shortNode)
					var newShort *shortNode
					if ok {
						//if r.newway {
							//fmt.Printf("shortnode: %s\n", short.fstring(""))
						//}
						newShort = &shortNode{Key: hexToCompact(append([]byte{keynibble}, compactToHex(short.Key)...)), Val: short.Val}
					} else {
						// r.nodeStack[level+1] is a value node
						newShort = &shortNode{Key: hexToCompact([]byte{keynibble, 16}), Val: r.nodeStack[level+1]}
					}
					//if r.newway {
						//fmt.Printf("newShortNode: %s\n", newShort.fstring(""))
					//}
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
				//if r.newway {
					//fmt.Printf("fullNode: %s\n", r.vertical[level+1].fstring(""))
				//}
				r.vertical[level+1].setcache(nil)
				hn, err := h.hash(&r.vertical[level+1], false)
				if err != nil {
					return err
				}
				// TODO: Write the hash with index (r.key>>4) if level+1 == 5
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
			if r.startLevel < r.pos {
				r.startLevel = r.pos
			}
		}
		if k != nil {
			// Insert the current key
			//r.nodeStack[Levels] = valueNode(common.CopyBytes(v))
			//r.fillCount[Levels] = 1
			copy(r.key[:], k[:32])
			r.value = common.CopyBytes(v)
			r.key_set = true
		}
	}
	return nil
}

/* One resolver per trie (prefix) */
type TrieResolver struct {
	t *Trie
	dbw ethdb.Putter // For updating hashes
	hashes bool
	continuations []*TrieContinuation
	walkstarts []int  // For each walk key, it contains the index in the continuations array where it starts
	walkends []int    // For each walk key, it contains the index in the continuations array where it ends
	key [32]byte
	key_set bool
	nodeStack [Levels+1]node
	vertical [Levels+1]fullNode
	fillCount [Levels+1]int
	walkidx int // Index of the subwalk
	h *hasher
}

func (t *Trie) NewResolver(dbw ethdb.Putter) *TrieResolver {
	tr := TrieResolver{
		t: t,
		dbw: dbw,
		continuations: []*TrieContinuation{},
		walkstarts: []int{},
		walkends: []int{},
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
}

// Prepares information for the MultiSuffixWalk
func (tr *TrieResolver) PrepareResolveParams() ([][]byte, []uint) {
	// Remove continuations strictly contained in the preceeding ones
	keys := [][]byte{}
	keybits := []uint{}
	if len(tr.continuations) == 0 {
		return keys, keybits
	}
	sort.Sort(tr)
	var prevC *TrieContinuation
	for i, c := range tr.continuations {
		if prevC == nil || c.resolvePos < prevC.resolvePos || !bytes.HasPrefix(c.resolveKey[:c.resolvePos], prevC.resolveKey[:prevC.resolvePos]) {
			if len(tr.walkstarts) > 0 {
				tr.walkends = append(tr.walkends, i)
			}
			tr.walkstarts = append(tr.walkstarts, i)
			key := make([]byte, 32)
			decodeNibbles(c.resolveKey[:c.resolvePos], key)
			keys = append(keys, key)
			keybits = append(keybits, uint(4*c.resolvePos))
			prevC = c
		}
	}
	tr.walkends = append(tr.walkends, len(tr.continuations))
	return keys, keybits
}

func (tr *TrieResolver) Walker(keyIdx int, k []byte, v []byte) (bool, error) {
/*
	if k == nil || len(v) > 0 {
		// First, finish off the previous key
		if tr.key_set {
			pLen := prefixLen(k, tr.key[:])
			stopLevel := 2*pLen
			if k != nil && (k[pLen]^tr.key[pLen])&0xf0 == 0 {
				stopLevel++
			}
			idx := tr.walkstarts[keyIdx]+tr.walkidx
			resolvingKey := tr.continuations[idx].resolveKey
			for level := Levels-1; level >= stopLevel; level-- {
				idx := level >> 1
				var keynibble byte
				if level&1 == 0 {
					keynibble = tr.key[idx]>>4
				} else {
					keynibble = tr.key[idx]&0xf
				}
				keybytes := int((4*(level+1) + 7)/8)
				shiftbits := uint((4*(level+1))&7)
				mask := byte(0xff)
				if shiftbits != 0 {
					mask = 0xff << (8-shiftbits)
				}
				onResolvingPath := false
				if resolvingKey != nil {
					onResolvingPath =
						bytes.Equal(tr.key[:keybytes-1], resolvingKey[:keybytes-1]) &&
						(tr.key[keybytes-1]&mask)==(resolvingKey[keybytes-1]&mask)
				}
				var hashIdx uint32
				if tr.hashes && level <= 4 {
					hashIdx = binary.BigEndian.Uint32(tr.key[:4]) >> 12
				}
				//fmt.Printf("level %d, keynibble %d\n", level, keynibble)
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
						short.setcache(nil)
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
					if level >= r.pos {
						tr.nodeStack[level+1] = nil
						tr.fillCount[level+1] = 0
						for i := 0; i < 17; i++ {
							tr.vertical[level+1].Children[i] = nil
						}
					}
					continue
				}
				tr.vertical[level+1].setcache(nil)
				hn, err := h.hash(&tr.vertical[level+1], false)
				if err != nil {
					return false, err
				}
				if tr.hashes && level == 4 {
					hash, ok := hn.(hashNode)
					if !ok {
						return false, fmt.Errorf("trie.rebuildInsert: hashNode expected")
					}
					tr.dbw.PutHash(hashIdx, hash)
				}
				if onResolvingPath {
					c := tr.vertical[level+1].copy()
					tr.vertical[level].Children[keynibble] = c
					tr.nodeStack[level] = &shortNode{Key: hexToCompact([]byte{keynibble}), Val: c}
				} else {
					tr.vertical[level].Children[keynibble] = hn
					tr.nodeStack[level] = &shortNode{Key: hexToCompact([]byte{keynibble}), Val: hn}
				}
				r.fillCount[level]++
				if level >= r.pos {
					tr.nodeStack[level+1] = nil
					tr.fillCount[level+1] = 0
					for i := 0; i < 17; i++ {
						tr.vertical[level+1].Children[i] = nil
					}
				}
			}
		}
		if k != nil {
			// Insert the current key
			tr.nodeStack[Levels] = valueNode(common.CopyBytes(v))
			tr.fillCount[Levels] = 1
			copy(tr.key[:], k[:32])
			tr.key_set = true
		}
	}
*/
	return true, nil
}

func (tr *TrieResolver) FinaliseWalk() error {
	return nil
}

func (tc *TrieContinuation) ResolveWithDb(db ethdb.Database, blockNr uint64) error {
	var start [32]byte
	decodeNibbles(tc.resolveKey[:tc.resolvePos], start[:])
	var resolving [32]byte
	decodeNibbles(tc.resolveKey, resolving[:])
	r := rebuidData{dbw: nil, pos: tc.resolvePos, resolvingKey: resolving[:], hashes: false, startLevel: tc.resolvePos}
	//rold := rebuidData{dbw: nil, pos: tc.resolvePos, resolvingKey: resolving[:], hashes: false, startLevel: tc.resolvePos, newway: false}
	h := newHasher(tc.t.encodeToBytes)
	err := db.WalkAsOf(tc.t.prefix, start[:], uint(4*tc.resolvePos), blockNr, func(k, v []byte) (bool, error) {
		if len(v) > 0 {
			if err := r.rebuildInsert(k, v, h); err != nil {
				return false, err
			}
			//if err := rold.rebuildInsert(k, v, h); err != nil {
			//	return false, err
			//}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if err := r.rebuildInsert(nil, nil, h); err != nil {
		return err
	}
	//if err := rold.rebuildInsert(nil, nil, h); err != nil {
	//	return err
	//}
	var root node
	//var rootold node
	if r.fillCount[tc.resolvePos] == 1 {
		root = r.nodeStack[tc.resolvePos]
	} else if r.fillCount[tc.resolvePos] > 1 {
		root = &r.vertical[tc.resolvePos]
	}
	//if rold.fillCount[tc.resolvePos] == 1 {
	//	rootold = rold.nodeStack[tc.resolvePos]
	//} else if rold.fillCount[tc.resolvePos] > 1 {
	//	rootold = &rold.vertical[tc.resolvePos]
	//}
	if root == nil {
		return fmt.Errorf("Resolve returned nil root")
	}
	//if rootold == nil {
	//	return fmt.Errorf("Resolve returned nil rootold")
	//}
	hash, err := h.hash(root, tc.resolvePos == 0)
	if err != nil {
		return err
	}
	//hashold, err := h.hash(rootold, tc.resolvePos == 0)
	//if err != nil {
	//	return err
	//}
	gotHash, ok := hash.(hashNode)
	//gotHashOld, okold := hashold.(hashNode)
	//fmt.Printf("Old way node: %s\n", rootold.fstring(""))
	//fmt.Printf("New way node: %s\n", root.fstring(""))
	//if okold {
	//	if !bytes.Equal(tc.resolveHash, gotHashOld) {
	//		return fmt.Errorf("(old)Resolving wrong hash for prefix %x, trie prefix %x\nexpected %s, got %s\n",
	//			tc.resolveKey[:tc.resolvePos], tc.t.prefix, tc.resolveHash, gotHashOld)
	//	}
	//} else {
	//	if tc.resolveHash != nil {
	//		return fmt.Errorf("(old)Resolving wrong hash for prefix %x, trie prefix %x\nexpected %s, got embedded node\n",
	//			tc.resolveKey[:tc.resolvePos], tc.t.prefix, tc.resolveHash)			
	//	}
	//}
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
	var resolving [32]byte
	decodeNibbles(key, resolving[:])
	r := rebuidData{dbw: db, pos:pos, resolvingKey: resolving[:], hashes: hashes}
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
