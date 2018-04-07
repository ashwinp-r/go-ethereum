// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)
)

var (
	cacheMissCounter   = metrics.NewRegisteredCounter("trie/cachemiss", nil)
	cacheUnloadCounter = metrics.NewRegisteredCounter("trie/cacheunload", nil)
)

// CacheMisses retrieves a global counter measuring the number of cache misses
// the trie had since process startup. This isn't useful for anything apart from
// trie debugging purposes.
func CacheMisses() int64 {
	return cacheMissCounter.Count()
}

// CacheUnloads retrieves a global counter measuring the number of cache unloads
// the trie did since process startup. This isn't useful for anything apart from
// trie debugging purposes.
func CacheUnloads() int64 {
	return cacheUnloadCounter.Count()
}

// LeafCallback is a callback type invoked when a trie operation reaches a leaf
// node. It's used by state sync and commit to allow handling external references
// between account and storage tries.
type LeafCallback func(leaf []byte, parent common.Hash) error

// Trie is a Merkle Patricia Trie.
// The zero value is an empty trie with no database.
// Use New to create a trie that sits on top of a database.
//
// Trie is not safe for concurrent use.
type Trie struct {
	root         	node
	originalRoot 	common.Hash

	// Prefix to form the database key
	prefix 			[]byte
	encodeToBytes 	bool

	nodeList        *List
}

func (t *Trie) PrintTrie() {
	fmt.Printf("%s\n", t.root.fstring(""))
}

// newFlag returns the cache flag value for a newly created node.
func (t *Trie) newFlag() nodeFlag {
	return nodeFlag{next: nil, prev: nil}
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root common.Hash, prefix []byte, encodeToBytes bool) *Trie {
	trie := &Trie{originalRoot: root, prefix: prefix, encodeToBytes: encodeToBytes}
	if (root != common.Hash{}) && root != emptyRoot {
		rootcopy := make([]byte, len(root[:]))
		copy(rootcopy, root[:])
		trie.root = hashNode(rootcopy)
	}
	return trie
}

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
	resolvingKey []byte
	pos int
	hashes bool
	key_set bool
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
			for level := Levels-1; level >= stopLevel; level-- {
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
				//fmt.Printf("level %d, keynibble %d\n", level, keynibble)
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
						for i := 0; i < 16; i++ {
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
					for i := 0; i < 16; i++ {
						r.vertical[level+1].Children[i] = nil
					}
				}
			}
		}
		if k != nil {
			// Insert the current key
			r.nodeStack[Levels] = valueNode(common.CopyBytes(v))
			r.fillCount[Levels] = 1
			copy(r.key[:], k[:32])
			r.key_set = true
		}
	}
	return nil
}

/* One resolver per trie (prefix) */
type TrieResolver struct {
	t *Trie
	continuations []*TrieContinuation
	walkstarts []int  // For each walk key, it contains the index in the continuations array where it starts
	walkends []int    // For each walk key, it contains the index in the continuations array where it ends
}

func (t *Trie) NewResolver() *TrieResolver {
	tr := TrieResolver{
		t: t,
		continuations: []*TrieContinuation{},
		walkstarts: []int{},
		walkends: []int{},
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
				tr.walkends = append(tr.walkends, tr.walkstarts[len(tr.walkstarts)-1])
			}
			tr.walkstarts = append(tr.walkstarts, i)
			key := make([]byte, 32)
			decodeNibbles(c.resolveKey[:c.resolvePos], key)
			keys = append(keys, key)
			keybits = append(keybits, uint(4*c.resolvePos))
			prevC = c
		}
	}
	tr.walkends = append(tr.walkends, tr.walkstarts[len(tr.walkstarts)-1])
	return keys, keybits
}

func (tr *TrieResolver) Walker(keyIdx int, key []byte, value []byte) (bool, error) {
	return false, nil
}

func (t *Trie) Resolve(keys [][]byte, values [][]byte, c *TrieContinuation) error {
	r := rebuidData{dbw: nil, pos: c.resolvePos, resolvingKey: c.resolveKey, hashes: false}
	h := newHasher(t.encodeToBytes)
	for i, key := range keys {
		value := values[i]
		if err := r.rebuildInsert(key, value, h); err != nil {
			return err
		}
	}
	if err := r.rebuildInsert(nil, nil, h); err != nil {
		return err
	}
	var root node
	if r.fillCount[c.resolvePos] == 1 {
		root = r.nodeStack[c.resolvePos]
	} else if r.fillCount[c.resolvePos] > 1 {
		root = &r.vertical[c.resolvePos]
	}
	if root == nil {
		return fmt.Errorf("Resolve returned nil root")
	}
	hash, err := h.hash(root, c.resolvePos == 0)
	if err != nil {
		return err
	}
	gotHash := hash.(hashNode)
	if !bytes.Equal(c.resolveHash, gotHash) {
		return fmt.Errorf("Resolving wrong hash for prefix %x, trie prefix %x\nexpected %s, got %s\n",
			c.resolveKey[:c.resolvePos], t.prefix, c.resolveHash, gotHash)
	}
	c.resolved = root
	return nil
}

func (tc *TrieContinuation) ResolveWithDb(db ethdb.Database, blockNr uint64) error {
	suffix := ethdb.CreateBlockSuffix(blockNr)
	endSuffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	var start [32]byte
	decodeNibbles(tc.resolveKey[:tc.resolvePos], start[:])
	var resolving [32]byte
	decodeNibbles(tc.resolveKey, resolving[:])
	keys := [][]byte{}
	values := [][]byte{}
	err := ethdb.SuffixWalk(db, tc.t.prefix, start[:], uint(4*tc.resolvePos), suffix, endSuffix, func(k, v []byte) (bool, error) {
		if len(v) > 0 {
			keys = append(keys, common.CopyBytes(k))
			values = append(values, common.CopyBytes(v))
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	return tc.t.Resolve(keys, values, tc)
}

func (t *Trie) rebuildHashes(db ethdb.Database, key []byte, pos int, blockNr uint64, hashes bool) (node, hashNode, error) {
	suffix := ethdb.CreateBlockSuffix(blockNr)
	endSuffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	var start [32]byte
	decodeNibbles(key[:pos], start[:])
	var resolving [32]byte
	decodeNibbles(key, resolving[:])
	r := rebuidData{dbw: db, pos:pos, resolvingKey: resolving[:], hashes: hashes}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	err := ethdb.SuffixWalk(db, t.prefix, start[:], uint(4*pos), suffix, endSuffix, func(k, v []byte) (bool, error) {
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

func (t *Trie) MakeListed(nodeList *List) {
	t.nodeList = nodeList
	t.relistNodes(t.root)
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *Trie) NodeIterator(db ethdb.Database, start []byte, blockNr uint64) NodeIterator {
	return newNodeIterator(db, t, start, blockNr)
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *Trie) Get(db ethdb.Database, key []byte, blockNr uint64) []byte {
	res, err := t.TryGet(db, key, blockNr)
	if err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
	return res
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryGet(db ethdb.Database, key []byte, blockNr uint64) ([]byte, error) {
	if t.nodeList != nil {
		// We want t.root to be evaluated on exit, not now
		defer func() { t.relistNodes(t.root) }()
	}
	k := keybytesToHex(key)
	value, gotValue, err := t.tryGet1(db, t.root, k, 0, blockNr)
	if err != nil {
		return value, err
	}
	if !gotValue {
		value, err = t.tryGet(db, t.root, k, 0, blockNr)
	}
	return value, err
}

func (t *Trie) emptyShortHash(db ethdb.Database, n *shortNode, level int, index uint32) {
	if compactLen(n.Key) + level < 5 {
		return
	}
	hexKey := compactToHex(n.Key)
	hashIdx := index
	for i := 0; i < 5-level; i++ {
		hashIdx = (hashIdx<<4)+uint32(hexKey[i])
	}
	//fmt.Printf("emptyShort %d %x\n", level, hashIdx)
	db.PutHash(hashIdx, emptyHash[:])
}

func (t *Trie) emptyFullHash(db ethdb.Database, level int, index uint32) {
	if level != 5 {
		return
	}
	//fmt.Printf("emptyFull %d %x\n", level, index)
	db.PutHash(index, emptyHash[:])
}

// Touching the node removes it from the nodeList
func (t *Trie) touch(db ethdb.Database, np nodep, key []byte, pos int) {
	if t.nodeList == nil {
		return
	}
	if np == nil {
		return
	}
	if np.next() != nil && np.prev() != nil {
		t.nodeList.Remove(np)
	}
	// Zeroing out the places in the hashes
	if db == nil {
		return
	}
	if key == nil {
		return
	}
	if pos > 5 {
		return
	}
	if !bytes.Equal(t.prefix, []byte("AT")) {
		return
	}
	var index uint32
	for i := 0; i < pos; i++ {
		index = (index << 4) + uint32(key[i])
	}
	switch n := (np).(type) {
	case *shortNode:
		t.emptyShortHash(db, n, pos, index)
	case *duoNode:
		t.emptyFullHash(db, pos, index)
	case *fullNode:
		t.emptyFullHash(db, pos, index)
	}
}

func (t *Trie) saveShortHash(db ethdb.Database, n *shortNode, level int, index uint32) bool {
	if !bytes.Equal(t.prefix, []byte("AT")) {
		return false
	}
	if level > 5 {
		return false
	}
	//fmt.Printf("saveShort(pre) %d\n", level)
	if compactLen(n.Key) + level < 5 {
		return true
	}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	hn, err := h.hash(n, false)
	if err != nil {
		panic(err)
	}
	hash, ok := hn.(hashNode)
	if !ok {
		panic("Expected hashNode")
	}
	hexKey := compactToHex(n.Key)
	hashIdx := index
	for i := 0; i < 5-level; i++ {
		hashIdx = (hashIdx<<4)+uint32(hexKey[i])
	}
	//fmt.Printf("saveShort %d %x %s\n", level, hashIdx, hash)
	db.PutHash(hashIdx, hash)
	return false
}

func (t *Trie) saveFullHash(db ethdb.Database, n node, level int, hashIdx uint32) bool {
	if !bytes.Equal(t.prefix, []byte("AT")) {
		return false
	}
	if level > 5 {
		return false
	}
	if level < 5 {
		return true
	}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	hn, err := h.hash(n, false)
	if err != nil {
		panic(err)
	}
	hash, ok := hn.(hashNode)
	if !ok {
		panic("Expected hashNode")
	}
	//fmt.Printf("saveFull %d %x %s\n", level, hashIdx, hash)
	db.PutHash(hashIdx, hash)
	return false
}

func (t *Trie) saveHashes(db ethdb.Database, n node, level int, index uint32) {
	switch n := (n).(type) {
	case *shortNode:
		if !n.unlisted() {
			return
		}
		// First re-add the child, then self
		if !t.saveShortHash(db, n, level, index) {
			return
		}
		index1 := index
		level1 := level
		for _, i := range compactToHex(n.Key) {
			if i < 16 {
				index1 = (index1<<4)+uint32(i)
				level1++
			}
		}
		t.saveHashes(db, n.Val, level1, index1)
	case *duoNode:
		if !n.unlisted() {
			return
		}
		if !t.saveFullHash(db, n, level, index) {
			return
		}
		i1, i2 := n.childrenIdx()
		t.saveHashes(db, n.child1, level+1, (index<<4)+uint32(i1))
		t.saveHashes(db, n.child2, level+1, (index<<4)+uint32(i2))
	case *fullNode:
		if !n.unlisted() {
			return
		}
		// First re-add children, then self
		if !t.saveFullHash(db, n, level, index) {
			return
		}
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				t.saveHashes(db, n.Children[i], level+1, (index<<4)+uint32(i))
			}
		}
	case hashNode:
		if level == 5 {
			//fmt.Printf("saveHash %x %s\n", index, n)
			db.PutHash(index, n)
		}
	}
}

// Re-adds nodes to the nodeList after being touched (and therefore removed from the list)
func (t *Trie) relistNodes(n node) {
	if n == nil {
		return
	}
	if !n.unlisted() {
		// Reached the node that has not been touched
		return
	}
	switch n := (n).(type) {
	case *shortNode:
		// First re-add the child, then self
		t.relistNodes(n.Val)
		t.nodeList.PushToBack(n)
	case *duoNode:
		t.relistNodes(n.child1)
		t.relistNodes(n.child2)
		t.nodeList.PushToBack(n)
	case *fullNode:
		// First re-add children, then self
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				t.relistNodes(n.Children[i])
			}
		}
		t.nodeList.PushToBack(n)
	}
}

func (t *Trie) tryGet(dbr DatabaseReader, origNode node, key []byte, pos int, blockNr uint64) (value []byte, err error) {
	suffix := ethdb.CreateBlockSuffix(blockNr)
	val, err := dbr.First(t.prefix, hexToKeybytes(key), suffix)
	if err != nil || val == nil {
		return nil, nil
	}
	return val, err
}

func (t *Trie) tryGet1(db ethdb.Database, origNode node, key []byte, pos int, blockNr uint64) (value []byte, gotValue bool, err error) {
	if np, ok := origNode.(nodep); ok {
		t.touch(nil, np, key, pos)
	}
	switch n := (origNode).(type) {
	case nil:
		return nil, true, nil
	case valueNode:
		return n, true, nil
	case *shortNode:
		nKey := compactToHex(n.Key)
		if len(key)-pos < len(nKey) || !bytes.Equal(nKey, key[pos:pos+len(nKey)]) {
			return nil, true, nil
		}
		return t.tryGet1(db, n.Val, key, pos+len(nKey), blockNr)
	case *duoNode:
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			return t.tryGet1(db, n.child1, key, pos+1, blockNr)
		case i2:
			return t.tryGet1(db, n.child2, key, pos+1, blockNr)
		default:
			return nil, true, nil
		}
	case *fullNode:
		return t.tryGet1(db, n.Children[key[pos]], key, pos+1, blockNr)
	case hashNode:
		return nil, false, nil
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *Trie) Update(db ethdb.Database, key, value []byte, blockNr uint64) {
	if err := t.TryUpdate(db, key, value, blockNr); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryUpdate associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
//
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryUpdate(db ethdb.Database, key, value []byte, blockNr uint64) error {
	tc := t.UpdateAction(key, value)
	for !tc.RunWithDb(db) {
		if err := tc.ResolveWithDb(db, blockNr); err != nil {
			return err
		}
	}
	t.SaveHashes(db)
	t.Relist()
	return nil
}

func (t *Trie) UpdateAction(key, value []byte) *TrieContinuation {
	var tc TrieContinuation
	tc.t = t
	tc.key = keybytesToHex(key)
	if len(value) != 0 {
		tc.action = TrieActionInsert
		tc.value = valueNode(value)
	} else {
		tc.action = TrieActionDelete
	}
	return &tc
}

func (t *Trie) SaveHashes(db ethdb.Database) {
	if bytes.Equal(t.prefix, []byte("AT")) {
		t.saveHashes(db, t.root, 0, 0)
	}
}

func (t *Trie) Relist() {
	if t.nodeList != nil {
		t.relistNodes(t.root)
	}
}

func (tc *TrieContinuation) RunWithDb(db ethdb.Database) bool {
	var done bool
	switch tc.action {
	case TrieActionInsert:
		done = tc.t.insert(tc.t.root, tc.key, 0, tc.value, tc)
	case TrieActionDelete:
		done = tc.t.delete(tc.t.root, tc.key, 0, tc)
	}
	if tc.updated {
		tc.t.root = tc.n
	}
	if !done {
		return done
	}
	for _, touch := range tc.touched {
		tc.t.touch(db, touch.np, touch.key, touch.pos)
	}
	return done
}

type TrieAction int

const (
	TrieActionInsert = iota
	TrieActionDelete
)

type Touch struct {
	np nodep
	key []byte
	pos int
}

type TrieContinuation struct {
	t *Trie              // trie to act upon
	action TrieAction    // insert of delete
	key []byte           // original key being inserted or deleted
	value node           // original value being inserted or deleted
	resolveKey []byte    // Key for which the resolution is requested
	resolvePos int       // Position in the key for which resolution is requested
	resolveHash hashNode // Expected hash of the resolved node (for correctness checking)
	resolved node        // Node that has been resolved via Database access
	n node               // Returned node after the operation is complete
	updated bool         // Whether the trie was updated
	touched []Touch      // Nodes touched during the operation, by level
}

func (tc *TrieContinuation) Print() {
	fmt.Printf("tc{t:%x,action:%d,key:%x}\n", tc.t.prefix, tc.action, tc.key)
}

func (t *Trie) insert(origNode node, key []byte, pos int, value node, c *TrieContinuation) bool {
	if np, ok := origNode.(nodep); ok {
		c.touched = append(c.touched, Touch{np: np, key: key, pos: pos})
	}
	if len(key) == pos {
		if v, ok := origNode.(valueNode); ok {
			c.updated = !bytes.Equal(v, value.(valueNode))
			if c.updated {
				c.n = value
			} else {
				c.n = v
			}
			return true
		}
		if vnp, ok := value.(nodep); ok {
			c.touched = append(c.touched, Touch{np: vnp, key: key, pos: pos})
		}
		c.updated = true
		c.n = value
		return true
	}
	switch n := origNode.(type) {
	case *shortNode:
		nKey := compactToHex(n.Key)
		matchlen := prefixLen(key[pos:], nKey)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(nKey) {
			done := t.insert(n.Val, key, pos+matchlen, value, c)
			if !c.updated {
				c.n = n
				return done
			}
			newnode := &shortNode{n.Key, c.n, t.newFlag()}
			c.updated = true
			c.n = newnode
			return done
		}
		// Otherwise branch out at the index where they differ.
		var c1, c2 node
		t.insert(nil, nKey, matchlen+1, n.Val, c) // Value already exists
		c1 = c.n
		t.insert(nil, key, pos+matchlen+1, value, c)
		c2 = c.n
		branch := &duoNode{flags: t.newFlag()}
		if nKey[matchlen] < key[pos+matchlen] {
			branch.child1 = c1
			branch.child2 = c2
		} else {
			branch.child1 = c2
			branch.child2 = c1
		}
		branch.mask = (1 << (nKey[matchlen])) | (1 << (key[pos+matchlen]))

		// Replace this shortNode with the branch if it occurs at index 0.
		if matchlen == 0 {
			c.updated = true
			c.n = branch
			return true
		}
		// Otherwise, replace it with a short node leading up to the branch.
		newnode := &shortNode{hexToCompact(key[pos:pos+matchlen]), branch, t.newFlag()}
		c.updated = true
		c.n = newnode
		return true

	case *duoNode:
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			done := t.insert(n.child1, key, pos+1, value, c)
			if !c.updated {
				c.n = n
				return done
			}
			newnode := n.copy()
			newnode.flags = t.newFlag()
			newnode.child1 = c.n
			c.updated = true
			c.n = newnode
			return done
		case i2:
			done := t.insert(n.child2, key, pos+1, value, c)
			if !c.updated {
				c.n = n
				return done
			}
			newnode := n.copy()
			newnode.flags = t.newFlag()
			newnode.child2 = c.n
			c.updated = true
			c.n = newnode
			return done
		default:
			done := t.insert(nil, key, pos+1, value, c)
			if !c.updated {
				c.n = n
				return done
			}
			newnode := &fullNode{flags: t.newFlag()}
			newnode.Children[i1] = n.child1
			newnode.Children[i2] = n.child2
			newnode.Children[key[pos]] = c.n
			c.updated = true
			c.n = newnode
			return done
		}

	case *fullNode:
		done := t.insert(n.Children[key[pos]], key, pos+1, value, c)
		if !c.updated {
			c.n = n
			return done
		}
		newnode := n.copy()
		newnode.flags = t.newFlag()
		newnode.Children[key[pos]] = c.n
		c.updated = true
		c.n = newnode
		return done

	case nil:
		newnode := &shortNode{hexToCompact(key[pos:]), value, t.newFlag()}
		c.updated = true
		c.n = newnode
		return true

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		if c.resolved == nil {
			c.resolveKey = key
			c.resolvePos = pos
			c.resolveHash = n
			c.updated = false
			return false // Need resolution
		}
		rn := c.resolved
		c.resolved = nil
		done := t.insert(rn, key, pos, value, c)
		if !c.updated {
			c.updated = true // Substitution of the hashNode with resolved node is an update
			c.n = rn
		}
		return done

	default:
		fmt.Printf("Key: %s, Prefix: %s\n", hex.EncodeToString(key[pos:]), hex.EncodeToString(key[:pos]))
		t.PrintTrie()
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete removes any existing value for key from the trie.
func (t *Trie) Delete(db ethdb.Database, key []byte, blockNr uint64) {
	if err := t.TryDelete(db, key, blockNr); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryDelete removes any existing value for key from the trie.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryDelete(db ethdb.Database, key []byte, blockNr uint64) error {
	tc := t.DeleteAction(key)
	for !tc.RunWithDb(db) {
		if err := tc.ResolveWithDb(db, blockNr); err != nil {
			return err
		}
	}
	t.Relist()
	return nil
}

func (t *Trie) DeleteAction(key []byte) *TrieContinuation {
	var tc TrieContinuation
	tc.t = t
	tc.key = keybytesToHex(key)
	tc.action = TrieActionDelete
	return &tc
}

func (t *Trie) convertToShortNode(key []byte, keyStart int, child node, pos uint, c *TrieContinuation, done bool) bool {
	cnode := child
	if pos != 16 {
		// If the remaining entry is a short node, it replaces
		// n and its key gets the missing nibble tacked to the
		// front. This avoids creating an invalid
		// shortNode{..., shortNode{...}}.  Since the entry
		// might not be loaded yet, resolve it just for this
		// check.
		rkey := make([]byte, len(key))
		copy(rkey, key)
		rkey[keyStart] = byte(pos)
		for i := keyStart + 1; i < len(key); i++ {
			if rkey[i] != 16 {
				rkey[i] = 0
			}
		}
		if childHash, ok := child.(hashNode); ok {
			if c.resolved == nil {
				c.resolveKey = rkey
				c.resolvePos = keyStart+1
				c.resolveHash = childHash
				return false // Need resolution
			}
			cnode, c.resolved = c.resolved, nil
		}
		if cnode, ok := cnode.(*shortNode); ok {
			c.touched = append(c.touched, Touch{np: cnode, key: rkey, pos: keyStart+1})
			k := append([]byte{byte(pos)}, compactToHex(cnode.Key)...)
			newshort := &shortNode{hexToCompact(k), cnode.Val, t.newFlag()}
			c.updated = true
			c.n = newshort
			return done
		}
	}
	// Otherwise, n is replaced by a one-nibble short node
	// containing the child.
	newshort := &shortNode{hexToCompact([]byte{byte(pos)}), cnode, t.newFlag()}
	c.updated = true
	c.n = newshort
	return done
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(origNode node, key []byte, keyStart int, c *TrieContinuation) bool {
	if np, ok := origNode.(nodep);ok {
		c.touched = append(c.touched, Touch{np: np, key: key, pos: keyStart})
	}
	switch n := origNode.(type) {
	case *shortNode:
		nKey := compactToHex(n.Key)
		matchlen := prefixLen(key[keyStart:], nKey)
		if matchlen < len(nKey) {
			c.updated = false
			c.n = n
			return true // don't replace n on mismatch
		}
		if matchlen == len(key) - keyStart {
			c.updated = true
			c.n = nil
			return true // remove n entirely for whole matches
		}
		// The key is longer than n.Key. Remove the remaining suffix
		// from the subtrie. Child can never be nil here since the
		// subtrie must contain at least two other values with keys
		// longer than n.Key.
		done := t.delete(n.Val, key, keyStart+len(nKey), c)
		if !c.updated {
			return done
		}
		child := c.n
		switch child := child.(type) {
		case *shortNode:
			// Deleting from the subtrie reduced it to another
			// short node. Merge the nodes to avoid creating a
			// shortNode{..., shortNode{...}}. Use concat (which
			// always creates a new slice) instead of append to
			// avoid modifying n.Key since it might be shared with
			// other nodes.
			childKey := compactToHex(child.Key)
			newnode := &shortNode{hexToCompact(concat(nKey, childKey...)), child.Val, t.newFlag()}
			c.updated = true
			c.n = newnode
			return done
		default:
			newnode := &shortNode{n.Key, child, t.newFlag()}
			c.updated = true
			c.n = newnode
			return done
		}

	case *duoNode:
		i1, i2 := n.childrenIdx()
		switch key[keyStart] {
		case i1:
			done := t.delete(n.child1, key, keyStart+1, c)
			nn := c.n
			if !c.updated && c.resolved == nil {
				c.n = n
				return done
			}
			if nn == nil {
				done = t.convertToShortNode(key, keyStart, n.child2, uint(i2), c, done)
				if done {
					return true
				}
			}
			newnode := n.copy()
			newnode.flags = t.newFlag()
			newnode.child1 = nn
			c.updated = true
			c.n = newnode
			return done
		case i2:
			done := t.delete(n.child2, key, keyStart+1, c)
			nn := c.n
			if !c.updated && c.resolved == nil {
				c.n = n
				return done
			}
			if nn == nil {
				done = t.convertToShortNode(key, keyStart, n.child1, uint(i1), c, done)
				if done {
					return true
				}
			}
			newnode := n.copy()
			newnode.flags = t.newFlag()
			newnode.child2 = nn
			c.updated = true
			c.n = newnode
			return done
		default:
			c.updated = false
			c.n = n
			return true
		}

	case *fullNode:
		done := t.delete(n.Children[key[keyStart]], key, keyStart+1, c)
		nn := c.n
		if !c.updated && c.resolved == nil {
			c.n = n
			return done
		}
		// Check how many non-nil entries are left after deleting and
		// reduce the full node to a short node if only one entry is
		// left. Since n must've contained at least two children
		// before deletion (otherwise it would not be a full node) n
		// can never be reduced to nil.
		//
		// When the loop is done, pos contains the index of the single
		// value that is left in n or -2 if n contains at least two
		// values.
		var pos1, pos2 int
		count := 0
		for i, cld := range n.Children {
			if i == int(key[keyStart]) && nn == nil {
				// Skip the child we are going to delete
				continue
			}
			if cld != nil {
				if count == 0 {
					pos1 = i
				}
				if count == 1 {
					pos2 = i
				}
				count++
				if count > 2 {
					break
				}
			}
		}
		if count == 1 {
			done = t.convertToShortNode(key, keyStart, n.Children[pos1], uint(pos1), c, done)
			if done {
				return true
			}
		}
		if count == 2 {
			duo := &duoNode{flags: t.newFlag()}
			if pos1 == int(key[keyStart]) {
				duo.child1 = nn
			} else {
				duo.child1 = n.Children[pos1]
			}
			if pos2 == int(key[keyStart]) {
				duo.child2 = nn
			} else {
				duo.child2 = n.Children[pos2]
			}
			duo.mask = (1 << uint(pos1)) | (1 << uint(pos2))
			c.updated = true
			c.n = duo
			return done
		}
		// n still contains at least three values and cannot be reduced.
		newnode := n.copy()
		newnode.flags = t.newFlag()
		newnode.Children[key[keyStart]] = nn
		c.updated = true
		c.n = newnode
		return done

	case valueNode:
		c.updated = true
		c.n = nil
		return true

	case nil:
		c.updated = false
		c.n = nil
		return true

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		if c.resolved == nil {
			c.resolveKey = key
			c.resolvePos = keyStart
			c.resolveHash = n
			c.updated = false
			return false // Need resolution
		}
		rn := c.resolved
		c.resolved = nil
		done := t.delete(rn, key, keyStart, c)
		if !c.updated {
			c.updated = true // Substitution is an update
			c.n = rn
		}
		return done

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key[:keyStart]))
	}
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func (t *Trie) resolveHash(db ethdb.Database, n hashNode, key []byte, pos int, blockNr uint64) (node, error) {
	root, gotHash, err := t.rebuildHashes(db, key, pos, blockNr, false)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(n, gotHash) {
		fmt.Printf("Resolving wrong hash for prefix %x, trie prefix %x block %d\n", key[:pos], t.prefix, blockNr)
		fmt.Printf("Expected hash %s\n", n)
		fmt.Printf("Got hash %s\n", gotHash)
		fmt.Printf("Stack: %s\n", debug.Stack())
		return nil, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: key[:pos]}
	}
	return root, err
}

func (t *Trie) resolveHashOld(db ethdb.Database, n hashNode, key []byte, pos int, blockNr uint64) (node, error) {
	suffix := ethdb.CreateBlockSuffix(blockNr)
	endSuffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	var start [32]byte
	decodeNibbles(key[:pos], start[:])
	var root node
	var tc TrieContinuation
	err := ethdb.SuffixWalk(db, t.prefix, start[:], uint(pos*4), suffix, endSuffix, func(k, v []byte) (bool, error) {
		if len(v) > 0 {
			tc.action = TrieActionInsert
			tc.key = keybytesToHex(k)
			tc.value = valueNode(common.CopyBytes(v))
			t.insert(root, tc.key, pos, tc.value, &tc)
			root = tc.n
		}
		return true, nil
	})
	if err == nil && n != nil {
		h := newHasher(t.encodeToBytes)
		defer returnHasherToPool(h)
		var gotHash hashNode
		if root != nil {
			hash, _ := h.hash(root, pos == 0)
			gotHash = hash.(hashNode)
		}
		if !bytes.Equal(n, gotHash) {
			fmt.Printf("Resolving wrong hash for prefix %x, trie prefix %x block %d\n", key[:pos], t.prefix, blockNr)
			fmt.Printf("Expected hash %s\n", n)
			fmt.Printf("Got hash %s\n", gotHash)
			fmt.Printf("Stack: %s\n", debug.Stack())
			return nil, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: key[:pos]}
		}
	} else {
		fmt.Printf("Error resolving hash: %s\n", err)
	}
	return root, err
}

// Root returns the root hash of the trie.
// Deprecated: use Hash instead.
func (t *Trie) Root() []byte { return t.Hash().Bytes() }

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
func (t *Trie) Hash() common.Hash {
	hash, _ := t.hashRoot()
	return common.BytesToHash(hash.(hashNode))
}

func (t *Trie) Unlink() {
	t.unlink(t.root)
}

func (t *Trie) unlink(n node) {
	if np, ok := n.(nodep); ok {
		t.touch(nil, np, nil, 0)
	}
	switch n := n.(type) {
	case *shortNode:
		t.unlink(n.Val)
	case *duoNode:
		t.unlink(n.child1)
		t.unlink(n.child2)
	case *fullNode:
		for _, child := range n.Children {
			if child != nil {
				t.unlink(child)
			}
		}
	}
}

// Return number of live nodes (not pruned)
// Returns true if the root became hash node
func (t *Trie) TryPrune() (int, bool, error) {
	newRoot, count, unloaded, err := t.tryPrune(t.root, 0)
	if err == nil && unloaded {
		t.root = newRoot
	}
	if t.nodeList != nil {
		t.relistNodes(t.root)
	}
	if _, ok := t.root.(hashNode); ok {
		return count, true, err
	} else {
		return count, false, err
	}
}

func (t *Trie) tryPrune(n node, depth int) (newnode node, livecount int, unloaded bool, err error) {
	if n == nil {
		return nil, 0, false, nil
	}
	if _, ok := n.(nodep); !ok {
		return n, 0, false, nil
	}
	if n.unlisted() {
		// Unload the node from cache. All of its subnodes will have a lower or equal
		// cache generation number.
		hash := n.cache()
		// If the node is dirty, we cannot unload, but instead moving to the back of the list
		// We also do not want to unload top of the trie
		if hash == nil || depth < 5 {
			if t.nodeList != nil {
				if np, ok := n.(nodep); ok {
					// Defering instead of calling to make sure parent nodes are added after their children and not before
					defer t.nodeList.PushToBack(np)
				}
			}
		} else {
			return hash, 0, true, nil
		}
	}
	switch n := (n).(type) {
	case *shortNode:
		newnode, livecount, unloaded, err = t.tryPrune(n.Val, depth+compactLen(n.Key))
		nn := n
		if err == nil && unloaded {
			t.touch(nil, n, nil, 0)
			nn = n.copy()
			nn.Val = newnode
		}
		return nn, livecount+1, unloaded, err

	case *duoNode:
		var nc *duoNode
		sumcount := 0
		newnode, livecount, unloaded, err = t.tryPrune(n.child1, depth+1)
		if err == nil && unloaded {
			if nc == nil {
				nc = n.copy()
			}
			nc.child1 = newnode
		}
		sumcount += livecount
		newnode, livecount, unloaded, err = t.tryPrune(n.child2, depth+1)
		if err == nil && unloaded {
			if nc == nil {
				nc = n.copy()
			}
			nc.child2 = newnode
		}
		sumcount += livecount
		if nc != nil {
			t.touch(nil, n, nil, 0)
			return nc, sumcount+1, true, err
		} else {
			return n, sumcount+1, false, err
		}

	case *fullNode:
		var nc *fullNode
		sumcount := 0
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				newnode, livecount, unloaded, err = t.tryPrune(n.Children[i], depth+1)
				if err == nil && unloaded {
					if nc == nil {
						nc = n.copy()
					}
					nc.Children[i] = newnode
				}
				sumcount += livecount
			}
		}
		if nc != nil {
			t.touch(nil, n, nil, 0)
			return nc, sumcount+1, true, err
		} else {
			return n, sumcount+1, false, err
		}
	}
	// Don't count hashNodes and valueNodes
	return n, 0, false, nil
}

func (t *Trie) CountOccupancies(db ethdb.Database, blockNr uint64, o map[int]map[int]int) {
	if hn, ok := t.root.(hashNode); ok {
		n, err := t.resolveHashOld(db, hn, []byte{}, 0, blockNr)
		if err != nil {
			panic(err)
		}
		t.root = n
	}
	t.countOccupancies(t.root, 0, o)
}

func (t *Trie) countOccupancies(n node, level int, o map[int]map[int]int) {
	if n == nil {
		return
	}
	switch n := (n).(type) {
	case *shortNode:
		t.countOccupancies(n.Val, level+1, o)
		if _, exists := o[level]; !exists {
			o[level] = make(map[int]int)
		}
		o[level][18] = o[level][18]+1
	case *duoNode:
		t.countOccupancies(n.child1, level+1, o)
		t.countOccupancies(n.child2, level+1, o)
		if _, exists := o[level]; !exists {
			o[level] = make(map[int]int)
		}
		o[level][2] = o[level][2]+1
	case *fullNode:
		count := 0
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				count++
				t.countOccupancies(n.Children[i], level+1, o)
			}
		}
		if _, exists := o[level]; !exists {
			o[level] = make(map[int]int)
		}
		o[level][count] = o[level][count]+1
	}
	return
}

func (t *Trie) hashRoot() (node, error) {
	if t.root == nil {
		return hashNode(emptyRoot.Bytes()), nil
	}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	return h.hash(t.root, true)
}
