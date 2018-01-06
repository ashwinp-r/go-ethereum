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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/log"
	"github.com/rcrowley/go-metrics"
)

var (
	// This is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	// This is the known hash of an empty state trie entry.
	emptyState common.Hash
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

func init() {
	sha3.NewKeccak256().Sum(emptyState[:0])
}

// Database must be implemented by backing stores for the trie.
type Database interface {
	DatabaseReader
	DatabaseWriter
}

// DatabaseReader wraps the Get method of a backing store for the trie.
type DatabaseReader interface {
	Get(key []byte) (value []byte, err error)
	Resolve(start, limit []byte) ([]byte, error)
	Has(key []byte) (bool, error)
}

// DatabaseWriter wraps the Put method of a backing store for the trie.
type DatabaseWriter interface {
	// Put stores the mapping key->value in the database.
	// Implementations must not hold onto the value bytes, the trie
	// will reuse the slice across calls to Put.
	Put(key, value []byte) error
}

// Trie is a Merkle Patricia Trie.
// The zero value is an empty trie with no database.
// Use New to create a trie that sits on top of a database.
//
// Trie is not safe for concurrent use.
type Trie struct {
	root         node
	db           Database
	originalRoot common.Hash

	// Prefix to form the database key
	prefix []byte
	prefetchCh chan PrefetchRequest

	// Cache generation values.
	// cachegen increases by one with each commit operation.
	// new nodes are tagged with the current generation and unloaded
	// when their generation is older than than cachegen-cachelimit.
	cachegen, cachelimit uint16
}

type PrefetchResponse struct {
	ready bool
	value []byte
	err error
	mu sync.Mutex
	c *sync.Cond
}

type PrefetchRequest struct {
	key []byte
	prefixEnd int
	blockNr uint32
	responsePtr *PrefetchResponse
}

func (t *Trie) PrintTrie() {
	if fn, ok := t.root.(*fullNode); ok {
		fmt.Printf("%s\n", fn.String())
	}
}

// SetCacheLimit sets the number of 'cache generations' to keep.
// A cache generation is created by a call to Commit.
func (t *Trie) SetCacheLimit(l uint16) {
	t.cachelimit = l
}

// newFlag returns the cache flag value for a newly created node.
func (t *Trie) newFlag() nodeFlag {
	return nodeFlag{dirty: true, gen: t.cachegen}
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root common.Hash, db Database, prefix []byte, blockNr uint32) (*Trie, error) {
	trie := &Trie{db: db, originalRoot: root, prefix: prefix}
	trie.prefetchCh = make(chan PrefetchRequest, 1024)
	prefetch := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Printf("internal error: %v\n", p)
			}
		}()
		for request := range trie.prefetchCh {
			enc, err := trie.readResolve(request.key[:request.prefixEnd], request.blockNr)
			response := request.responsePtr
			response.mu.Lock()
			response.value, response.err = enc, err
			response.ready = true
			response.c.Signal()
			response.mu.Unlock()
		}
	}
	for i := 0; i < 16; i++ {
		go prefetch()
	}
	if (root != common.Hash{}) && root != emptyRoot {
		if db == nil {
			panic("trie.New: cannot use existing root without a database")
		}
		rootnode, err := trie.resolveHash(root[:], []byte{}, blockNr, nil)
		if err != nil {
			return nil, err
		}
		trie.root = rootnode
	}
	return trie, nil
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *Trie) NodeIterator(start []byte, blockNr uint32) NodeIterator {
	return newNodeIterator(t, start, blockNr)
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *Trie) Get(key []byte, blockNr uint32) []byte {
	res, err := t.TryGet(key, blockNr)
	if err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
	return res
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryGet(key []byte, blockNr uint32) ([]byte, error) {
	key = keybytesToHex(key)
	return t.tryGet(t.root, key, 0, blockNr)
}

func (t *Trie) tryGet(origNode node, key []byte, pos int, blockNr uint32) (value []byte, err error) {
	suffix := make([]byte, 4)
	binary.BigEndian.PutUint32(suffix, blockNr^0xffffffff - 1) // Invert the block number
	enc, err := t.db.Resolve(
		CompositeKey(key, t.prefix, suffix),
		CompositeKey(key, t.prefix, []byte{0xff, 0xff, 0xff, 0xff}))
	if err != nil {
		//fmt.Printf("tryGet error for key %s and block %d: %s\n", hex.EncodeToString(key), blockNr, err)
	}
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	val, _, err := rlp.SplitString(enc)
	return val, err
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *Trie) Update(key, value []byte, blockNr uint32) {
	if err := t.TryUpdate(key, value, blockNr, nil); err != nil {
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
func (t *Trie) TryUpdate(key, value []byte, blockNr uint32, respMap map[string]*PrefetchResponse) error {
	k := keybytesToHex(key)
	if respMap == nil && t.prefetchCh != nil {
		kk, pos := t.cachedPrefixFor(t.root, k, 0)
		if blockNr != 0 {
			//fmt.Printf("Result %s %d\n", hex.EncodeToString(kk), pos)
		}
		respMap = make(map[string]*PrefetchResponse)
		//fmt.Printf("TryUpdate with key %s pos %d blockNr %d\n", hex.EncodeToString(kk), pos, blockNr)
		t.RequestPrefetch(kk, pos, blockNr, respMap)
	}
	if len(value) != 0 {
		_, n, err := t.insert(t.root, k, 0, valueNode(value), blockNr, respMap)
		if err != nil {
			return err
		}
		t.root = n
	} else {
		_, n, err := t.delete(t.root, k, 0, blockNr, respMap)
		if err != nil {
			return err
		}
		t.root = n
	}
	return nil
}

func (t *Trie) insert(n node, key []byte, keyStart int, value node, blockNr uint32, respMap map[string]*PrefetchResponse) (bool, node, error) {
	if len(key) == keyStart {
		if v, ok := n.(valueNode); ok {
			return !bytes.Equal(v, value.(valueNode)), value, nil
		}
		return true, value, nil
	}
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key[keyStart:], n.Key)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(n.Key) {
			dirty, nn, err := t.insert(n.Val, key, keyStart+matchlen, value, blockNr, respMap)
			if !dirty || err != nil {
				return false, n, err
			}
			return true, &shortNode{n.Key, nn, t.newFlag()}, nil
		}
		// Otherwise branch out at the index where they differ.
		branch := &fullNode{flags: t.newFlag()}
		var err error
		_, branch.Children[n.Key[matchlen]], err = t.insert(nil, n.Key, matchlen+1, n.Val, blockNr, respMap)
		if err != nil {
			return false, nil, err
		}
		_, branch.Children[key[keyStart+matchlen]], err = t.insert(nil, key, keyStart+matchlen+1, value, blockNr, respMap)
		if err != nil {
			return false, nil, err
		}
		// Replace this shortNode with the branch if it occurs at index 0.
		if matchlen == 0 {
			return true, branch, nil
		}
		// Otherwise, replace it with a short node leading up to the branch.
		return true, &shortNode{key[keyStart:keyStart+matchlen], branch, t.newFlag()}, nil

	case *fullNode:
		dirty, nn, err := t.insert(n.Children[key[keyStart]], key, keyStart+1, value, blockNr, respMap)
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[keyStart]] = nn
		return true, n, nil

	case nil:
		return true, &shortNode{key[keyStart:], value, t.newFlag()}, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveHash(n, key[:keyStart], blockNr, respMap)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.insert(rn, key, keyStart, value, blockNr, respMap)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		fmt.Printf("Key: %s, Prefix: %s\n", hex.EncodeToString(key[keyStart:]), hex.EncodeToString(key[:keyStart]))
		t.PrintTrie()
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete removes any existing value for key from the trie.
func (t *Trie) Delete(key []byte, blockNr uint32) {
	if err := t.TryDelete(key, blockNr, nil); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryDelete removes any existing value for key from the trie.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryDelete(key []byte, blockNr uint32, respMap map[string]*PrefetchResponse) error {
	k := keybytesToHex(key)
	if respMap == nil && t.prefetchCh != nil {
		kk, pos := t.cachedPrefixFor(t.root, k, 0)
		respMap = make(map[string]*PrefetchResponse)
		//fmt.Printf("TryUpdate with key %s pos %d blockNr %d\n", hex.EncodeToString(kk), pos, blockNr)
		t.RequestPrefetch(kk, pos, blockNr, respMap)
	}
	_, n, err := t.delete(t.root, k, 0, blockNr, respMap)
	if err != nil {
		return err
	}
	t.root = n
	return nil
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(n node, key []byte, keyStart int, blockNr uint32, respMap map[string]*PrefetchResponse) (bool, node, error) {
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key[keyStart:], n.Key)
		if matchlen < len(n.Key) {
			return false, n, nil // don't replace n on mismatch
		}
		if matchlen == len(key) - keyStart {
			return true, nil, nil // remove n entirely for whole matches
		}
		// The key is longer than n.Key. Remove the remaining suffix
		// from the subtrie. Child can never be nil here since the
		// subtrie must contain at least two other values with keys
		// longer than n.Key.
		dirty, child, err := t.delete(n.Val, key, keyStart+len(n.Key), blockNr, respMap)
		if !dirty || err != nil {
			return false, n, err
		}
		switch child := child.(type) {
		case *shortNode:
			// Deleting from the subtrie reduced it to another
			// short node. Merge the nodes to avoid creating a
			// shortNode{..., shortNode{...}}. Use concat (which
			// always creates a new slice) instead of append to
			// avoid modifying n.Key since it might be shared with
			// other nodes.
			return true, &shortNode{concat(n.Key, child.Key...), child.Val, t.newFlag()}, nil
		default:
			return true, &shortNode{n.Key, child, t.newFlag()}, nil
		}

	case *fullNode:
		dirty, nn, err := t.delete(n.Children[key[keyStart]], key, keyStart+1, blockNr, respMap)
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[keyStart]] = nn

		// Check how many non-nil entries are left after deleting and
		// reduce the full node to a short node if only one entry is
		// left. Since n must've contained at least two children
		// before deletion (otherwise it would not be a full node) n
		// can never be reduced to nil.
		//
		// When the loop is done, pos contains the index of the single
		// value that is left in n or -2 if n contains at least two
		// values.
		pos := -1
		for i, cld := range n.Children {
			if cld != nil {
				if pos == -1 {
					pos = i
				} else {
					pos = -2
					break
				}
			}
		}
		if pos >= 0 {
			if pos != 16 {
				// If the remaining entry is a short node, it replaces
				// n and its key gets the missing nibble tacked to the
				// front. This avoids creating an invalid
				// shortNode{..., shortNode{...}}.  Since the entry
				// might not be loaded yet, resolve it just for this
				// check.
				cnode, err := t.resolve(n.Children[pos], concat(key[:keyStart], byte(pos)), blockNr)
				if err != nil {
					return false, nil, err
				}
				if cnode, ok := cnode.(*shortNode); ok {
					k := append([]byte{byte(pos)}, cnode.Key...)
					return true, &shortNode{k, cnode.Val, t.newFlag()}, nil
				}
			}
			// Otherwise, n is replaced by a one-nibble short node
			// containing the child.
			return true, &shortNode{[]byte{byte(pos)}, n.Children[pos], t.newFlag()}, nil
		}
		// n still contains at least two values and cannot be reduced.
		return true, n, nil

	case valueNode:
		return true, nil, nil

	case nil:
		return false, nil, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveHash(n, key[:keyStart], blockNr, respMap)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.delete(rn, key, keyStart, blockNr, respMap)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

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

func (t *Trie) resolve(n node, prefix []byte, blockNr uint32) (node, error) {
	if n, ok := n.(hashNode); ok {
		return t.resolveHash(n, prefix, blockNr, nil)
	}
	return n, nil
}

func (t *Trie) readResolve(prefix []byte, blockNr uint32) ([]byte, error) {
	suffix := make([]byte, 4)
	binary.BigEndian.PutUint32(suffix, blockNr^0xffffffff - 1) // Invert the block number
	startKey := CompositeKey(prefix, t.prefix, suffix)
	limitKey := CompositeKey(prefix, t.prefix, []byte{0xff, 0xff, 0xff, 0xff})
	//fmt.Printf("Resolving prefix %s, startkey %s limitkey %s block %d\n",
	//		hex.EncodeToString(prefix), hex.EncodeToString(startKey), hex.EncodeToString(limitKey), blockNr)
	enc, err := t.db.Resolve(startKey, limitKey)
	if err != nil {
		//fmt.Printf("Resolving wrong hash for prefix %s, startkey %s limitkey %s block %d: %v\n",
		//	hex.EncodeToString(prefix), hex.EncodeToString(startKey), hex.EncodeToString(limitKey), blockNr, err)
	}
	return enc, err
}

func (t *Trie) resolveHash(n hashNode, prefix []byte, blockNr uint32, respMap map[string]*PrefetchResponse) (node, error) {
	cacheMissCounter.Inc(1)
	var enc []byte
	var err error
	if respMap == nil {
		enc, err = t.readResolve(prefix, blockNr)
	} else {
		if response, ok := respMap[string(prefix)]; ok {
			//fmt.Printf("Resolving %s with prefetch %d\n", hex.EncodeToString(prefix), blockNr)
			response.mu.Lock()
			for !response.ready {
				response.c.Wait()
			}
			defer response.mu.Unlock()
			enc, err = response.value, response.err
		} else {
			enc, err = t.readResolve(prefix, blockNr)
			fmt.Printf("Had to resolve %s without prefetch\n", hex.EncodeToString(prefix))
		}
	}
	if err != nil || enc == nil {
		return nil, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: prefix}
	}
	// Check that the hash matches
	sha := sha3.NewKeccak256()
	sha.Write(enc)
	gotHash := sha.Sum(nil)
	if bytes.Compare(n, gotHash) != 0 {
		fmt.Printf("Resolving wrong hash for prefix %s, block %d: %v\n",
			hex.EncodeToString(prefix), blockNr, err)
		fmt.Printf("Expected hash %s\n", hex.EncodeToString(n))
		fmt.Printf("Got hash %s\n", hex.EncodeToString(gotHash))
		fmt.Printf("Got data %s\n", hex.EncodeToString(enc))
		fmt.Printf("Stack: %s\n", debug.Stack())
		return nil, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: prefix}
	}
	dec := mustDecodeNode(n, enc, t.cachegen)
	return dec, nil
}

// Finds the part of the key that can be explored without loading anything from the
// underlying database
func (t *Trie) CachedPrefixFor(key []byte) ([]byte, int) {
	k := keybytesToHex(key)
	return t.cachedPrefixFor(t.root, k, 0)
}

func (t *Trie) cachedPrefixFor(n node, key []byte, pos int) ([]byte, int) {
	if pos == len(key) {
		return key, pos
	}
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key[pos:], n.Key)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		if matchlen == len(n.Key) {
			return t.cachedPrefixFor(n.Val, key, pos + matchlen)
		}
		// No further resolutions of the trie required, apart from edge cases of deleting penultimate slot in a full node
		return key, len(key)
	case *fullNode:
		return t.cachedPrefixFor(n.Children[key[pos]], key, pos + 1)

	case nil:
		// No further resolutions of the trie required
		return key, len(key)

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet.
		return key, pos

	default:
		fmt.Printf("Key: %s\n", hex.EncodeToString(key))
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

func (t *Trie) RequestPrefetch(key []byte, prefixEnd int, blockNr uint32, respMap map[string]*PrefetchResponse) {
	for i := prefixEnd; i <= len(key); i++ {
		keyStr := string(key[:i])
		if _, ok := respMap[keyStr]; !ok {
			var response PrefetchResponse
			response.c = sync.NewCond(&response.mu)
			respMap[keyStr] = &response
			t.prefetchCh <- PrefetchRequest{key: key, prefixEnd: i, responsePtr: &response, blockNr: blockNr}
		}
	}
}

// Root returns the root hash of the trie.
// Deprecated: use Hash instead.
func (t *Trie) Root() []byte { return t.Hash().Bytes() }

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
func (t *Trie) Hash() common.Hash {
	hash, cached, _ := t.hashRoot(nil, 0)
	t.root = cached
	return common.BytesToHash(hash.(hashNode))
}

// Commit writes all nodes to the trie's database.
// Nodes are stored with their sha3 hash as the key.
//
// Committing flushes nodes from memory.
// Subsequent Get calls will load nodes from the database.
func (t *Trie) Commit(writeBlockNr uint32) (root common.Hash, err error) {
	if t.db == nil {
		panic("Commit called on trie with nil database")
	}
	return t.CommitTo(t.db, writeBlockNr)
}

// CommitTo writes all nodes to the given database.
// Nodes are stored with their sha3 hash as the key.
//
// Committing flushes nodes from memory. Subsequent Get calls will
// load nodes from the trie's database. Calling code must ensure that
// the changes made to db are written back to the trie's attached
// database before using the trie.
func (t *Trie) CommitTo(db DatabaseWriter, writeBlockNr uint32) (root common.Hash, err error) {
	hash, cached, err := t.hashRoot(db, writeBlockNr)
	if err != nil {
		return (common.Hash{}), err
	}
	t.root = cached
	t.cachegen++
	return common.BytesToHash(hash.(hashNode)), nil
}

func (t *Trie) hashRoot(db DatabaseWriter, writeBlockNr uint32) (node, node, error) {
	if t.root == nil {
		return hashNode(emptyRoot.Bytes()), nil, nil
	}
	h := newHasher(t.cachegen, t.cachelimit, db, t.prefix, writeBlockNr)
	return h.hash(t.root, true, []byte{})
}
