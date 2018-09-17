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
	"encoding/hex"
	"fmt"
	"runtime/debug"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)
)

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

	// Bucket for the database access
	bucket 			[]byte
	// Prefix to form database key (for storage)
	prefix          []byte
	encodeToBytes 	bool
	accounts        bool

	nodeList        *List
	historical      bool
	joinGeneration  func(gen uint64)
	leftGeneration  func(gen uint64)
}

func (t *Trie) PrintTrie() {
	if t.root == nil {
		fmt.Printf("nil Trie\n")
	} else {
		fmt.Printf("%s\n", t.root.fstring(""))
	}
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root common.Hash, bucket []byte, prefix []byte, encodeToBytes bool) *Trie {
	trie := &Trie{
		originalRoot: root,
		bucket: bucket,
		prefix: prefix,
		encodeToBytes: encodeToBytes,
		accounts: bytes.Equal(bucket, []byte("AT")),
		joinGeneration: func(uint64) {},
		leftGeneration: func(uint64) {},
	}
	if (root != common.Hash{}) && root != emptyRoot {
		rootcopy := make([]byte, len(root[:]))
		copy(rootcopy, root[:])
		trie.root = hashNode(rootcopy)
	}
	return trie
}

func (t *Trie) SetHistorical(h bool) {
	t.historical = h
	if h && !bytes.HasPrefix(t.bucket, []byte("h")) {
		t.bucket = append([]byte("h"), t.bucket...)
	}
}

func (t *Trie) MakeListed(nodeList *List, joinGeneration, leftGeneration func (gen uint64)) {
	t.nodeList = nodeList
	t.joinGeneration = joinGeneration
	t.leftGeneration = leftGeneration
	t.relistNodes(t.root, 0)
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
func (t *Trie) TryGet(db ethdb.Database, key []byte, blockNr uint64) (value []byte, err error) {
	k := keybytesToHex(key)
	value, gotValue := t.tryGet1(t.root, k, 0, blockNr)
	if !gotValue {
		value, err = t.tryGet(db, t.root, key, 0, blockNr)
	}
	return value, err
}

func (t *Trie) emptyShortHash(db ethdb.Database, n *shortNode, level int, index uint32) {
	if compactLen(n.Key) + level < 6 {
		return
	}
	hexKey := compactToHex(n.Key)
	hashIdx := index
	for i := 0; i < 6-level; i++ {
		hashIdx = (hashIdx<<4)+uint32(hexKey[i])
	}
	//fmt.Printf("emptyShort %d %x\n", level, hashIdx)
	db.PutHash(hashIdx, emptyHash[:])
}

func (t *Trie) emptyFullHash(db ethdb.Database, level int, index uint32) {
	if level != 6 {
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
		if np.next() != np.prev() {
			t.nodeList.Remove(np)
		} else {
			np.setnext(nil)
			np.setprev(nil)
		} 
	}
	// Zeroing out the places in the hashes
	if db == nil {
		return
	}
	if key == nil {
		return
	}
	if pos > 6 {
		return
	}
	if !t.accounts {
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

func (t *Trie) saveShortHash(db ethdb.Database, n *shortNode, level int, index uint32, h *hasher) bool {
	if !t.accounts {
		return false
	}
	if level > 6 {
		return false
	}
	//fmt.Printf("saveShort(pre) %d\n", level)
	if compactLen(n.Key) + level < 6 {
		return true
	}
	hexKey := compactToHex(n.Key)
	hashIdx := index
	for i := 0; i < 6-level; i++ {
		hashIdx = (hashIdx<<4)+uint32(hexKey[i])
	}
	//fmt.Printf("saveShort %d %x %s\n", level, hashIdx, hash)
	db.PutHash(hashIdx, n.hash())
	return false
}

func (t *Trie) saveFullHash(db ethdb.Database, n node, level int, hashIdx uint32, h *hasher) bool {
	if !t.accounts {
		return false
	}
	if level > 6 {
		return false
	}
	if level < 6 {
		return true
	}
	//fmt.Printf("saveFull %d %x %s\n", level, hashIdx, hash)
	db.PutHash(hashIdx, n.hash())
	return false
}

func (t *Trie) saveHashes(db ethdb.Database, n node, level int, index uint32, h *hasher) {
	switch n := (n).(type) {
	case *shortNode:
		if !n.unlisted() {
			return
		}
		// First re-add the child, then self
		if !t.saveShortHash(db, n, level, index, h) {
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
		t.saveHashes(db, n.Val, level1, index1, h)
	case *duoNode:
		if !n.unlisted() {
			return
		}
		if !t.saveFullHash(db, n, level, index, h) {
			return
		}
		i1, i2 := n.childrenIdx()
		t.saveHashes(db, n.child1, level+1, (index<<4)+uint32(i1), h)
		t.saveHashes(db, n.child2, level+1, (index<<4)+uint32(i2), h)
	case *fullNode:
		if !n.unlisted() {
			return
		}
		// First re-add children, then self
		if !t.saveFullHash(db, n, level, index, h) {
			return
		}
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				t.saveHashes(db, n.Children[i], level+1, (index<<4)+uint32(i), h)
			}
		}
	case hashNode:
		if level == 6 {
			//fmt.Printf("saveHash %x %s\n", index, n)
			db.PutHash(index, n)
		}
	}
}

// Re-adds nodes to the nodeList after being touched (and therefore removed from the list)
func (t *Trie) relistNodes(n node, level int) {
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
		t.relistNodes(n.Val, level+compactLen(n.Key))
		if !t.accounts || level > 5 {
			t.nodeList.PushToBack(n)
		} else {
			n.setnext(n)
			n.setprev(n)
		}
	case *duoNode:
		t.relistNodes(n.child1, level+1)
		t.relistNodes(n.child2, level+1)
		if !t.accounts || level > 5 {
			t.nodeList.PushToBack(n)
		} else {
			n.setnext(n)
			n.setprev(n)
		}
	case *fullNode:
		// First re-add children, then self
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				t.relistNodes(n.Children[i], level+1)
			}
		}
		if !t.accounts || level > 5 {
			t.nodeList.PushToBack(n)
		} else {
			n.setnext(n)
			n.setprev(n)
		}
	}
}

func (t *Trie) tryGet(dbr DatabaseReader, origNode node, key []byte, pos int, blockNr uint64) (value []byte, err error) {
	if t.historical {
		value, err = dbr.GetAsOf(t.bucket[1:], t.bucket, append(t.prefix, key...), blockNr)
	} else {
		value, err = dbr.Get(t.bucket, append(t.prefix, key...))
	}
	if err != nil || value == nil {
		return nil, nil
	}
	return
}

func (t *Trie) tryGet1(origNode node, key []byte, pos int, blockNr uint64) (value []byte, gotValue bool) {
	switch n := (origNode).(type) {
	case nil:
		return nil, true
	case valueNode:
		return n, true
	case *shortNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var adjust bool
		nKey := compactToHex(n.Key)
		if len(key)-pos < len(nKey) || !bytes.Equal(nKey, key[pos:pos+len(nKey)]) {
			adjust = false
			value, gotValue = nil, true
		} else {
			adjust = true
			value, gotValue = t.tryGet1(n.Val, key, pos+len(nKey), blockNr)
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case *duoNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			adjust = n.tod(blockNr) == n.child1.tod(blockNr)
			value, gotValue = t.tryGet1(n.child1, key, pos+1, blockNr)
		case i2:
			adjust = n.tod(blockNr) == n.child2.tod(blockNr)
			value, gotValue = t.tryGet1(n.child2, key, pos+1, blockNr)
		default:
			adjust = false
			value, gotValue = nil, true
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case *fullNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[pos]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		value, gotValue = t.tryGet1(child, key, pos+1, blockNr)
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case hashNode:
		return nil, false
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
	for !tc.RunWithDb(db, blockNr) {
		r := NewResolver(db, false, t.accounts)
		r.AddContinuation(tc)
		if err := r.ResolveWithDb(db, blockNr); err != nil {
			return err
		}
	}
	t.Hash()
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
	if t.accounts {
		h := newHasher(t.encodeToBytes)
		defer returnHasherToPool(h)
		t.saveHashes(db, t.root, 0, 0, h)
	}
}

func (t *Trie) Relist() {
	if t.nodeList != nil {
		t.relistNodes(t.root, 0)
	}
}

func (tc *TrieContinuation) RunWithDb(db ethdb.Database, blockNr uint64) bool {
	var done bool
	switch tc.action {
	case TrieActionInsert:
		done = tc.t.insert(tc.t.root, tc.key, 0, tc.value, tc, blockNr)
	case TrieActionDelete:
		done = tc.t.delete(tc.t.root, tc.key, 0, tc, blockNr)
	}
	if tc.updated {
		for _, touch := range tc.touched {
			tc.t.touch(db, touch.np, touch.key, touch.pos)
		}
		tc.touched = []Touch{}
		tc.t.root = tc.n
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
	extResolvePos int
	resolveHash hashNode // Expected hash of the resolved node (for correctness checking)
	resolved node        // Node that has been resolved via Database access
	n node               // Returned node after the operation is complete
	updated bool         // Whether the trie was updated
	touched []Touch      // Nodes touched during the operation, by level
}

func (t *Trie) NewContinuation(key []byte, pos int, resolveHash []byte) *TrieContinuation {
	return &TrieContinuation{t: t, key: key, resolveKey: key, resolvePos: pos, resolveHash: hashNode(resolveHash)}
}

func (tc *TrieContinuation) Print() {
	fmt.Printf("tc{t:%x/%x,action:%d,key:%x,resolveKey:%x,resolvePos:%d}\n", tc.t.bucket, tc.t.prefix, tc.action, tc.key, tc.resolveKey, tc.resolvePos)
}

func (t *Trie) insert(origNode node, key []byte, pos int, value node, c *TrieContinuation, blockNr uint64) bool {
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
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		nKey := compactToHex(n.Key)
		matchlen := prefixLen(key[pos:], nKey)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		var done bool
		if matchlen == len(nKey) {
			done = t.insert(n.Val, key, pos+matchlen, value, c, blockNr)
			if c.updated {
				n.Val = c.n
				n.flags.dirty = true
			}
			c.n = n
			n.adjustTod(blockNr)
		} else {
			// Otherwise branch out at the index where they differ.
			var c1, c2 node
			t.insert(nil, nKey, matchlen+1, n.Val, c, blockNr) // Value already exists
			c1 = c.n
			t.insert(nil, key, pos+matchlen+1, value, c, blockNr)
			c2 = c.n
			branch := &duoNode{}
			if nKey[matchlen] < key[pos+matchlen] {
				branch.child1 = c1
				branch.child2 = c2
			} else {
				branch.child1 = c2
				branch.child2 = c1
			}
			branch.mask = (1 << (nKey[matchlen])) | (1 << (key[pos+matchlen]))
			branch.flags.dirty = true
			branch.flags.t = blockNr
			branch.flags.tod = blockNr

			// Replace this shortNode with the branch if it occurs at index 0.
			if matchlen == 0 {
				branch.adjustTod(blockNr)
				c.n = branch // current node leaves the generation, but new node branch joins it
			} else {
				// Otherwise, replace it with a short node leading up to the branch.
				n.Key = hexToCompact(key[pos:pos+matchlen])
				n.Val = branch
				t.joinGeneration(blockNr) // new branch node joins the generation
				n.flags.dirty = true
				c.n = n
				n.adjustTod(blockNr)
			}
			c.updated = true
			done = true
		}
		return done

	case *duoNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var done bool
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			adjust = n.tod(blockNr) == n.child1.tod(blockNr)
			done = t.insert(n.child1, key, pos+1, value, c, blockNr)
			if c.updated {
				n.child1 = c.n
				n.flags.dirty = true
			}
			c.n = n
		case i2:
			adjust = n.tod(blockNr) == n.child2.tod(blockNr)
			done = t.insert(n.child2, key, pos+1, value, c, blockNr)
			if c.updated {
				n.child2 = c.n
				n.flags.dirty = true
			}
			c.n = n
		default:
			adjust = true
			done = t.insert(nil, key, pos+1, value, c, blockNr)
			if !c.updated {
				c.n = n
			} else {
				newnode := &fullNode{}
				newnode.Children[i1] = n.child1
				newnode.Children[i2] = n.child2
				newnode.flags.dirty = true
				newnode.flags.t = blockNr
				newnode.adjustTod(blockNr)
				adjust = false
				newnode.Children[key[pos]] = c.n
				c.updated = true
				c.n = newnode // current node leaves the generation but newnode joins it
			}
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return done

	case *fullNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[pos]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		done := t.insert(child, key, pos+1, value, c, blockNr)
		if c.updated {
			n.Children[key[pos]] = c.n
			n.flags.dirty = true
		}
		c.n = n
		if adjust {
			n.adjustTod(blockNr)
		}
		return done

	case nil:
		newnode := &shortNode{Key: hexToCompact(key[pos:])}
		newnode.Val = value
		newnode.flags.dirty = true
		newnode.flags.t = blockNr
		newnode.adjustTod(blockNr)
		t.joinGeneration(blockNr)
		c.updated = true
		c.n = newnode
		return true

	case hashNode:
		var done bool
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		if c.resolved == nil || !bytes.Equal(key, c.resolveKey) || pos != c.resolvePos {
			c.resolved = nil
			c.resolveKey = key
			c.resolvePos = pos
			c.resolveHash = common.CopyBytes(n)
			c.updated = false
			done = false // Need resolution
		} else {
			rn := c.resolved
			t.timestampSubTree(rn, blockNr)
			c.resolved = nil
			c.resolveKey = nil
			c.resolvePos = 0
			done = t.insert(rn, key, pos, value, c, blockNr)
			if !c.updated {
				c.updated = true // Substitution of the hashNode with resolved node is an update
				c.n = rn
			}
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
	for !tc.RunWithDb(db, blockNr) {
		r := NewResolver(db, false, t.accounts)
		r.AddContinuation(tc)
		if err := r.ResolveWithDb(db, blockNr); err != nil {
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

func (t *Trie) convertToShortNode(key []byte, keyStart int, child node, pos uint, c *TrieContinuation, blockNr uint64, done bool) bool {
	cnode := child
	if pos != 16 {
		// If the remaining entry is a short node, it replaces
		// n and its key gets the missing nibble tacked to the
		// front. This avoids creating an invalid
		// shortNode{..., shortNode{...}}.  Since the entry
		// might not be loaded yet, resolve it just for this
		// check.
		//rkey := make([]byte, len(key))
		rkey := make([]byte, keyStart+1)
		copy(rkey, key[:keyStart])
		rkey[keyStart] = byte(pos)
		if childHash, ok := child.(hashNode); ok {
			if c.resolved == nil || !bytes.Equal(rkey, c.resolveKey) || keyStart+1 != c.resolvePos {
				// It is either unresolved or resolved by other request
				c.resolved = nil
				c.resolveKey = rkey
				c.resolvePos = keyStart+1
				c.resolveHash = common.CopyBytes(childHash)
				return false // Need resolution
			}
			cnode = c.resolved
			t.timestampSubTree(cnode, blockNr)
			c.resolved = nil
			c.resolveKey = nil
			c.resolvePos = 0
		}
		if cnode, ok := cnode.(*shortNode); ok {
			c.touched = append(c.touched, Touch{np: cnode, key: rkey, pos: keyStart+1})
			k := append([]byte{byte(pos)}, compactToHex(cnode.Key)...)
			newshort := &shortNode{Key: hexToCompact(k)}
			t.leftGeneration(blockNr)
			newshort.Val = cnode.Val
			newshort.flags.dirty = true
			newshort.flags.t = blockNr
			newshort.flags.tod = blockNr
			// cnode gets removed, but newshort gets added
			c.updated = true
			c.n = newshort
			return done
		}
	}
	// Otherwise, n is replaced by a one-nibble short node
	// containing the child.
	newshort := &shortNode{Key: hexToCompact([]byte{byte(pos)})}
	newshort.Val = cnode
	newshort.flags.dirty = true
	newshort.flags.t = blockNr
	newshort.flags.tod = blockNr
	c.updated = true
	c.n = newshort
	return done
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(origNode node, key []byte, keyStart int, c *TrieContinuation, blockNr uint64) bool {
	if np, ok := origNode.(nodep);ok {
		c.touched = append(c.touched, Touch{np: np, key: key, pos: keyStart})
	}
	switch n := origNode.(type) {
	case *shortNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var done bool
		nKey := compactToHex(n.Key)
		matchlen := prefixLen(key[keyStart:], nKey)
		if matchlen < len(nKey) {
			c.updated = false
			c.n = n
			done = true // don't replace n on mismatch
		} else if matchlen == len(key) - keyStart {
			t.leftGeneration(blockNr)
			c.updated = true
			c.n = nil
			done = true // remove n entirely for whole matches
		} else {
			// The key is longer than n.Key. Remove the remaining suffix
			// from the subtrie. Child can never be nil here since the
			// subtrie must contain at least two other values with keys
			// longer than n.Key.
			done = t.delete(n.Val, key, keyStart+len(nKey), c, blockNr)
			if !c.updated {
				c.n = n
			} else {
				child := c.n
				if child == nil {
					t.leftGeneration(blockNr)
					c.n = nil
					done = true
				} else {
					if shortChild, ok := child.(*shortNode); ok {
						// Deleting from the subtrie reduced it to another
						// short node. Merge the nodes to avoid creating a
						// shortNode{..., shortNode{...}}. Use concat (which
						// always creates a new slice) instead of append to
						// avoid modifying n.Key since it might be shared with
						// other nodes.
						childKey := compactToHex(shortChild.Key)
						newnode := &shortNode{Key: hexToCompact(concat(nKey, childKey...))}
						newnode.Val = shortChild.Val
						newnode.flags.dirty = true
						newnode.flags.t = blockNr
						newnode.adjustTod(blockNr)
						// We do not increase generation count here, because one short node comes, but another one 
						t.leftGeneration(blockNr) // But shortChild goes away
						c.touched = append(c.touched, Touch{np: shortChild, key: key, pos: keyStart+len(nKey)})
						c.n = newnode
					} else {
						n.Val = child
						n.flags.dirty = true
						n.adjustTod(blockNr)
						c.n = n
					}
				}
				c.updated = true
			}
		}
		return done

	case *duoNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var done bool
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[keyStart] {
		case i1:
			adjust = n.child1 != nil && n.tod(blockNr) == n.child1.tod(blockNr)
			done = t.delete(n.child1, key, keyStart+1, c, blockNr)
			if !c.updated && !done {
				c.n = n
				done = false
			} else {
				nn := c.n
				if nn == nil {
					if n.child2 == nil {
						adjust = false
						t.leftGeneration(blockNr)
						c.n = nil
						c.updated = true
					} else {
						done = t.convertToShortNode(key, keyStart, n.child2, uint(i2), c, blockNr, done)
					}
				}
				if nn != nil || (nn == nil && !done) {
					if c.updated {
						n.child1 = nn
						n.flags.dirty = true
					}
					c.n = n
				}
			}
		case i2:
			adjust = n.child2 != nil && n.tod(blockNr) == n.child2.tod(blockNr)
			done = t.delete(n.child2, key, keyStart+1, c, blockNr)
			if !c.updated && !done {
				c.n = n
				done = false
			} else {
				nn := c.n
				if nn == nil {
					if n.child1 == nil {
						adjust = false
						t.leftGeneration(blockNr)
						c.n = nil
						c.updated = true
					} else {
						done = t.convertToShortNode(key, keyStart, n.child1, uint(i1), c, blockNr, done)
					}
				}
				if nn != nil || (nn == nil && !done) {
					if c.updated {
						n.child2 = nn
						n.flags.dirty = true
					}
					c.n = n
				}
			}
		default:
			adjust = false
			c.updated = false
			c.n = n
			done = true
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return done

	case *fullNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[keyStart]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		done := t.delete(child, key, keyStart+1, c, blockNr)
		if !c.updated && !done {
			c.n = n
			done = false
		} else {
			nn := c.n
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
			for i, cld := range &n.Children {
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
			if count == 0 {
				t.leftGeneration(blockNr)
				c.n = nil
				c.updated = true
				done = true
			} else if count == 1 {
				done = t.convertToShortNode(key, keyStart, n.Children[pos1], uint(pos1), c, blockNr, done)
			} else if count == 2 {
				duo := &duoNode{}
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
				duo.flags.dirty = true
				duo.mask = (1 << uint(pos1)) | (uint32(1) << uint(pos2))
				duo.flags.t = blockNr
				duo.adjustTod(blockNr)
				adjust = false
				c.updated = true
				c.n = duo
			}
			if count > 2 || (count == 1 && !done) {
				if c.updated {
					// n still contains at least three values and cannot be reduced.
					n.Children[key[keyStart]] = nn
					n.flags.dirty = true
				}
				c.n = n
			}
		}
		if adjust {
			n.adjustTod(blockNr)
		}
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
		var done bool
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		if c.resolved == nil || !bytes.Equal(key, c.resolveKey) || keyStart != c.resolvePos {
			// It is either unresolved, or resolved by other request
			c.resolved = nil
			c.resolveKey = key
			c.resolvePos = keyStart
			c.resolveHash = common.CopyBytes(n)
			c.updated = false
			done = false // Need resolution
		} else {
			rn := c.resolved
			t.timestampSubTree(rn, blockNr)
			c.resolved = nil
			c.resolveKey = nil
			c.resolvePos = 0
			done = t.delete(rn, key, keyStart, c, blockNr)
			if !c.updated {
				c.updated = true // Substitution is an update
				c.n = rn
			}
		}
		return done

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key[:keyStart]))
	}
}

// Timestamp given node and all descendants
func (t *Trie) timestampSubTree(n node, blockNr uint64) {
	switch n := n.(type) {
	case *shortNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			t.timestampSubTree(n.Val, blockNr)
		}
	case *duoNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			t.timestampSubTree(n.child1, blockNr)
			t.timestampSubTree(n.child2, blockNr)
		}
	case *fullNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			for _, child := range n.Children {
				if child != nil {
					t.timestampSubTree(child, blockNr)
				}
			}
		}
	}
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func (t *Trie) resolveHash(db ethdb.Database, n hashNode, key []byte, pos int, blockNr uint64) (node, error) {
	root, gotHash, err := t.rebuildHashes(db, key, pos, blockNr, false, n)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(n, gotHash) {
		fmt.Printf("Resolving wrong hash for prefix %x, bucket %x prefix %x block %d\n", key[:pos], t.bucket, t.prefix, blockNr)
		fmt.Printf("Expected hash %s\n", n)
		fmt.Printf("Got hash %s\n", hashNode(gotHash))
		fmt.Printf("Stack: %s\n", debug.Stack())
		return nil, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: key[:pos]}
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

func (t *Trie) UnloadOlderThan(gen uint64) bool {
	hn, unloaded := t.unloadOlderThan(t.root, gen)
	if unloaded {
		t.root = hn
		return true
	}
	return false
}

func (t *Trie) unloadOlderThan(n node, gen uint64) (hn hashNode, unloaded bool) {
	if n == nil {
		return nil, false
	}
	switch n := (n).(type) {
	case *shortNode:
		if n.flags.tod >= gen {
		}
	case *duoNode:
	case *fullNode:
	}
	return nil, false
}

func (t *Trie) CountNodes() int {
	return t.countNodes(t.root)
}

func (t *Trie) countNodes(n node) int {
	if n == nil {
		return 0
	}
	switch n := (n).(type) {
	case *shortNode:
		return 1 + t.countNodes(n.Val)
	case *duoNode:
		return 1 + t.countNodes(n.child1) + t.countNodes(n.child2)
	case *fullNode:
		count := 1
		for _, child := range n.Children {
			if child != nil {
				count += t.countNodes(child)
			}
		}
		return count
	}
	return 0
}

// Return number of live nodes (not pruned)
// Returns true if the root became hash node
func (t *Trie) TryPrune() (int, bool) {
	hn, count, unloaded := t.tryPrune(t.root, 0)
	if unloaded {
		t.root = hn
		return count, true
	}
	return count, false
}

func (t *Trie) tryPrune(n node, level int) (hn hashNode, livecount int, unloaded bool) {
	if n == nil {
		return nil, 0, false
	}
	if _, ok := n.(nodep); !ok {
		return nil, 0, false
	}
	if n.unlisted() && (!t.accounts || level > 5) {
		// Unload the node from cache. All of its subnodes will have a lower or equal
		// cache generation number.
		if n.dirty() {
			if np, ok := n.(nodep); ok {
				t.nodeList.PushToBack(np)
			}
		} else {
			return hashNode(common.CopyBytes(n.hash())), 0, true
		}
	}
	switch n := (n).(type) {
	case *shortNode:
		hn, livecount, unloaded = t.tryPrune(n.Val, level+compactLen(n.Key))
		if unloaded {
			n.Val = hn
		}
		return nil, livecount+1, false

	case *duoNode:
		sumcount := 0
		hn, livecount, unloaded = t.tryPrune(n.child1, level+1)
		if unloaded {
			n.child1 = hn
		}
		sumcount += livecount
		hn, livecount, unloaded = t.tryPrune(n.child2, level+1)
		if unloaded {
			n.child2 = hn
		}
		sumcount += livecount
		return nil, sumcount+1, false

	case *fullNode:
		sumcount := 0
		for i, child := range n.Children {
			if child != nil {
				hn, livecount, unloaded = t.tryPrune(n.Children[i], level+1)
				if unloaded {
					n.Children[i] = hn
				}
				sumcount += livecount
			}
		}
		return nil, sumcount+1, false
	}
	// Don't count hashNodes and valueNodes
	return nil, 0, false
}

func (t *Trie) CountOccupancies(db ethdb.Database, blockNr uint64, o map[int]map[int]int) {
	if hn, ok := t.root.(hashNode); ok {
		n, err := t.resolveHash(db, hn, []byte{}, 0, blockNr)
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
	var hn common.Hash
	h.hash(t.root, true, hn[:])
	return hashNode(hn[:]), nil
}
