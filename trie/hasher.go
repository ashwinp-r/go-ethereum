// Copyright 2016 The go-ethereum Authors
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

package trie

import (
	"bytes"
	//"fmt"
	"hash"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/rlp"
)

type Sha3Hash interface {
	hash.Hash
	Read(out []byte) (n int, err error)
}

type hasher struct {
	tmp            *bytes.Buffer
	sha            Sha3Hash
	encodeToBytes  bool
	shortCollapsed [Levels]shortNode
	fullCollapsed  [Levels]fullNode
}

// hashers live in a global db.
var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{tmp: new(bytes.Buffer), sha: sha3.NewKeccak256().(Sha3Hash)}
	},
}

func newHasher(encodeToBytes bool) *hasher {
	h := hasherPool.Get().(*hasher)
	h.encodeToBytes = encodeToBytes
	return h
}

func returnHasherToPool(h *hasher) {
	hasherPool.Put(h)
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *hasher) hash(n node, force bool, storeTo []byte) (node, error) {
	return h.hashInternal(n, force, storeTo, 0)
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *hasher) hashInternal(n node, force bool, storeTo []byte, level int) (node, error) {
	// Trie not processed yet or needs storage, walk the children
	collapsed, err := h.hashChildren(n, level)
	if err != nil {
		return hashNode{}, err
	}
	hashed, err := h.store(collapsed, force, storeTo)
	if err != nil {
		return hashNode{}, err
	}
	return hashed, nil
}

// hashChildren replaces the children of a node with their hashes if the encoded
// size of the child is larger than a hash, returning the collapsed node as well
// as a replacement for the original node with the child hashes cached in.
func (h *hasher) hashChildren(original node, level int) (node, error) {
	var err error
	switch n := original.(type) {
	case *shortNode:
		// Hash the short node's child, caching the newly hashed subtree
		collapsed := &h.shortCollapsed[level]
		collapsed.Key = n.Key

		if child, ok := n.Val.(valueNode); !ok {
			if !n.hashTrue {
				if n.valHash == nil {
					n.valHash = make([]byte, common.HashLength)
				}
				collapsed.Val, err = h.hashInternal(n.Val, false, n.valHash, level+1)
				if err != nil {
					return original, err
				}
				if _, ok := collapsed.Val.(hashNode); ok {
					n.hashTrue = true
				} else {
					n.hashTrue = false
				}
			} else {
				collapsed.Val = n.valHash
			}
		} else if h.encodeToBytes {
			enc, _ := rlp.EncodeToBytes(child)
			collapsed.Val = valueNode(enc)
		} else {
			collapsed.Val = child
		}
		if collapsed.Val == nil {
			collapsed.Val = valueNode(nil) // Ensure that nil children are encoded as empty strings.
		}
		return collapsed, nil

	case *duoNode:
		i1, i2 := n.childrenIdx()
		collapsed := &h.fullCollapsed[level]
		for i := 0; i < 17; i++ {
			if i == int(i1) {
				if (n.hashTrueMask & (uint32(1)<<i1)) == 0 {
					if n.child1Hash == nil {
						n.child1Hash = make([]byte, common.HashLength)
					}
					collapsed.Children[i], err = h.hashInternal(n.child1, false, n.child1Hash, level+1)
					if err != nil {
						return original, err
					}
					if _, ok := collapsed.Children[i].(hashNode); ok {
						n.hashTrueMask |= (uint32(1)<<i1)
					} else {
						n.hashTrueMask &^= (uint32(1)<<i1)
					}
				} else {
					collapsed.Children[i] = n.child1Hash
				}
			} else if i == int(i2) {
				if (n.hashTrueMask & (uint32(1)<<i2)) == 0 {
					if n.child2Hash == nil {
						n.child2Hash = make([]byte, common.HashLength)
					}
					collapsed.Children[i], err = h.hashInternal(n.child2, false, n.child2Hash, level+1)
					if err != nil {
						return original, err
					}
					if _, ok := collapsed.Children[i].(hashNode); ok {
						n.hashTrueMask |= (uint32(1)<<i2)
					} else {
						n.hashTrueMask &^= (uint32(1)<<i2)
					}
				} else {
					collapsed.Children[i] = n.child2Hash
				}
			} else {
				collapsed.Children[i] = valueNode(nil)
			}
		}
		return collapsed, nil

	case *fullNode:
		// Hash the full node's children, caching the newly hashed subtrees
		collapsed := &h.fullCollapsed[level]

		for i := 0; i < 16; i++ {
			if n.Children[i] != nil {
				if (n.hashTrueMask & (uint32(1)<<uint(i))) == 0 {
					if n.childHashes[i] == nil {
						n.childHashes[i] = make([]byte, common.HashLength)
					}
					collapsed.Children[i], err = h.hashInternal(n.Children[i], false, n.childHashes[i], level+1)
					if err != nil {
						return original, err
					}
					if _, ok := collapsed.Children[i].(hashNode); ok {
						n.hashTrueMask |= (uint32(1)<<uint(i))
					} else {
						n.hashTrueMask &^= (uint32(1)<<uint(i))
					}
				} else {
					collapsed.Children[i] = n.childHashes[i]
				}
			} else {
				collapsed.Children[i] = valueNode(nil) // Ensure that nil children are encoded as empty strings.
			}
		}
		collapsed.Children[16] = n.Children[16]
		if collapsed.Children[16] == nil {
			collapsed.Children[16] = valueNode(nil)
		}
		return collapsed, nil

	case valueNode:
		//fmt.Printf("value\n")
		if h.encodeToBytes {
			enc, _ := rlp.EncodeToBytes(n)
			return valueNode(enc), nil
		} else {
			return n, nil
		}

	default:
		// Value and hash nodes don't have children so they're left as were
		return n, nil
	}
}

func EncodeAsValue(data []byte) ([]byte, error) {
	tmp := new(bytes.Buffer)
	err := rlp.Encode(tmp, valueNode(data))
	if err != nil {
		return nil, err
	}
	return tmp.Bytes(), nil
}

// store hashes the node n and if we have a storage layer specified, it writes
// the key/value pair to it and tracks any node->child references as well as any
// node->external trie references.
func (h *hasher) store(n node, force bool, storeTo []byte) (node, error) {
	// Don't store hashes or empty nodes.
	if hash, isHash := n.(hashNode); n == nil || isHash {
		copy(storeTo, hash)
		return hash, nil
	}
	// Generate the RLP encoding of the node
	h.tmp.Reset()
	if err := rlp.Encode(h.tmp, n); err != nil {
		panic("encode error: " + err.Error())
	}
	if h.tmp.Len() < 32 && !force {
		if short, ok := n.(*shortNode); ok {
			c := short.copy()
			if v, ok := c.Val.(valueNode); ok {
				c.Val = valueNode(common.CopyBytes(v))
			}
			return c, nil
		}
		return n, nil // Nodes smaller than 32 bytes are stored inside their parent
	}

	h.sha.Reset()
	h.sha.Write(h.tmp.Bytes())
	if storeTo != nil {
		h.sha.Read(storeTo)
		return hashNode(storeTo), nil
	}
	return hashNode(h.sha.Sum(nil)), nil
}
