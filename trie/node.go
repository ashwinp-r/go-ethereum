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

package trie

import (
	"fmt"
	"io"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

type node interface {
	fstring(string) string
	unlisted() bool
}

type nodep interface {
	next() nodep
	setnext(nodep)
	prev() nodep
	setprev(nodep)
}

type (
	fullNode struct {
		Children [17]node // Actual trie node data to encode/decode (needs custom encoder)
		flags    nodeFlag
		hashTrueMask uint32 // When bit is set, the hash with the corresponding index is valid
		childHashes [17]hashNode
	}
	duoNode struct {
		mask	uint32 // Bitmask. The set bits indicate the child is not nil
		child1  node
		child2  node
		flags   nodeFlag
		hashTrueMask uint32
		child1Hash hashNode
		child2Hash hashNode
	}
	shortNode struct {
		Key   []byte
		Val   node
		flags nodeFlag
		hashTrue bool
		valHash hashNode
	}
	hashNode  []byte
	valueNode []byte
)

// EncodeRLP encodes a full node into the consensus RLP format.
func (n *fullNode) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, n.Children)
}

func (n *duoNode) EncodeRLP(w io.Writer) error {
	var children [17]node
	i1, i2 := n.childrenIdx()
	children[i1] = n.child1
	children[i2] = n.child2
	for i := 0; i < 17; i++ {
		if i != int(i1) && i != int(i2) {
			children[i] = valueNode(nil)
		}
	}
	return rlp.Encode(w, children)
}

func (n *duoNode) childrenIdx() (i1 byte, i2 byte) {
	child := 1
	var m uint32 = 1
	for i := 0; i < 17; i++ {
		if (n.mask & m) > 0 {
			if child == 1 {
				i1 = byte(i)
				child = 2
			} else if child == 2 {
				i2 = byte(i)
				break
			}
		}
		m <<= 1
	}
	return i1, i2
}

func (n *fullNode) copy() *fullNode   {
	copy := *n
	copy.flags.next = nil
	copy.flags.prev = nil
	for i, childHash := range copy.childHashes {
		copy.childHashes[i] = common.CopyBytes(childHash)
	}
	for i, child := range copy.Children {
		if hash, ok := child.(hashNode); ok {
			if (copy.hashTrueMask & (uint32(1)<<uint(i))) != 0 {
				copy.Children[i] = copy.childHashes[i]
			} else {
				copy.Children[i] = hashNode(common.CopyBytes(hash))
			}
		}
	}
	return &copy
}

func (n *fullNode) duoCopy() *duoNode {
	copy := duoNode{}
	first := true
	for i, child := range n.Children {
		if child == nil {
			continue
		}
		if first {
			first = false
			copy.mask |= (uint32(1)<<uint(i))
			if (n.hashTrueMask & (uint32(1)<<uint(i))) != 0 {
				copy.child1Hash = common.CopyBytes(n.childHashes[i])
				copy.hashTrueMask |= (uint32(1)<<uint(i))
			}
			if hash, ok := child.(hashNode); ok {
				if (copy.hashTrueMask & (uint32(1)<<uint(i))) != 0 {
					copy.child1 = copy.child1Hash
				} else {
					copy.child1 = hashNode(common.CopyBytes(hash))
				}
			} else {
				copy.child1 = child
			}
		} else {
			copy.mask |= (uint32(1)<<uint(i))
			if (n.hashTrueMask & (uint32(1)<<uint(i))) != 0 {
				copy.child2Hash = common.CopyBytes(n.childHashes[i])
				copy.hashTrueMask |= (uint32(1)<<uint(i))
			}
			if hash, ok := child.(hashNode); ok {
				if (copy.hashTrueMask & (uint32(1)<<uint(i))) != 0 {
					copy.child2 = copy.child2Hash
				} else {
					copy.child2 = hashNode(common.CopyBytes(hash))
				}
			} else {
				copy.child2 = child
			}
			break
		}
	}
	return &copy
}

func (n *duoNode) copy() *duoNode {
	copy := *n
	copy.flags.next = nil
	copy.flags.prev = nil
	copy.child1Hash = common.CopyBytes(copy.child1Hash)
	copy.child2Hash = common.CopyBytes(copy.child2Hash)
	i1, i2 := copy.childrenIdx()
	if hash, ok := copy.child1.(hashNode); ok {
		if (copy.hashTrueMask & (uint32(1)<<uint(i1))) != 0 {
			copy.child1 = copy.child1Hash
		} else {
			copy.child1 = hashNode(common.CopyBytes(hash))
		}
	}
	if hash, ok := copy.child2.(hashNode); ok {
		if (copy.hashTrueMask & (uint32(1)<<uint(i2))) != 0 {
			copy.child2 = copy.child1Hash
		} else {
			copy.child2 = hashNode(common.CopyBytes(hash))
		}
	}
	return &copy
}
func (n *shortNode) copy() *shortNode {
	copy := *n
	copy.flags.next = nil
	copy.flags.prev = nil
	copy.valHash = common.CopyBytes(copy.valHash)
	if hash, ok := copy.Val.(hashNode); ok {
		if copy.hashTrue {
			copy.Val = copy.valHash
		} else {
			copy.Val = hashNode(common.CopyBytes(hash))
		}
	}
	return &copy
}

// nodeFlag contains caching-related metadata about a node.
type nodeFlag struct {
	next, prev  nodep      // list element for efficient disposing of nodes
}

func (n *fullNode) unlisted() bool  { return n.flags.next == nil && n.flags.prev == nil }
func (n *duoNode) unlisted() bool   { return n.flags.next == nil && n.flags.prev == nil }
func (n *shortNode) unlisted() bool { return n.flags.next == nil && n.flags.prev == nil }
func (n hashNode) unlisted() bool   { return false }
func (n valueNode) unlisted() bool  { return false }

func (n *fullNode) next() nodep  { return n.flags.next }
func (n *duoNode) next() nodep   { return n.flags.next }
func (n *shortNode) next() nodep { return n.flags.next }

func (n *fullNode) setnext(next nodep)  { n.flags.next = next }
func (n *duoNode) setnext(next nodep)   { n.flags.next = next }
func (n *shortNode) setnext(next nodep) { n.flags.next = next }

func (n *fullNode) prev() nodep  { return n.flags.prev }
func (n *duoNode) prev() nodep   { return n.flags.prev }
func (n *shortNode) prev() nodep { return n.flags.prev }

func (n *fullNode) setprev(prev nodep)  { n.flags.prev = prev }
func (n *duoNode) setprev(prev nodep)   { n.flags.prev = prev }
func (n *shortNode) setprev(prev nodep) { n.flags.prev = prev }

// Pretty printing.
func (n fullNode) String() string  { return n.fstring("") }
func (n duoNode) String() string   { return n.fstring("") }
func (n shortNode) String() string { return n.fstring("") }
func (n hashNode) String() string   { return n.fstring("") }
func (n valueNode) String() string  { return n.fstring("") }

func (n *fullNode) fstring(ind string) string {
	resp := fmt.Sprintf("full\n%s  ", ind)
	for i, node := range n.Children {
		if node == nil {
			resp += fmt.Sprintf("%s: <nil> ", indices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", indices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}

func (n *duoNode) fstring(ind string) string {
	resp := fmt.Sprintf("duo[\n%s  ", ind)
	i1, i2 := n.childrenIdx()
	resp += fmt.Sprintf("%s: %v", indices[i1], n.child1.fstring(ind+"  "))
	resp += fmt.Sprintf("%s: %v", indices[i2], n.child2.fstring(ind+"  "))
	return resp + fmt.Sprintf("\n%s] ", ind)
}

func (n *shortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: %v} ", compactToHex(n.Key), n.Val.fstring(ind+"  "))
}
func (n hashNode) fstring(ind string) string {
	return fmt.Sprintf("<%x> ", []byte(n))
}
func (n valueNode) fstring(ind string) string {
	return fmt.Sprintf("%x ", []byte(n))
}

// decodeNode parses the RLP encoding of a trie node.
func decodeNode(hash, buf []byte) (node, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		n, err := decodeShort(hash, buf, elems)
		return n, wrapError(err, "short")
	case 17:
		n, err := decodeFull(hash, buf, elems)
		return n, wrapError(err, "full")
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

func decodeShort(hash, buf, elems []byte) (node, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	key := compactToHex(kbuf)
	if hasTerm(key) {
		// value node
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid value node: %v", err)
		}
		return &shortNode{Key: key, Val: append(valueNode{}, val...)}, nil
	}
	r, _, err := decodeRef(rest)
	if err != nil {
		return nil, wrapError(err, "val")
	}
	return &shortNode{Key: key, Val: r}, nil
}

func decodeFull(hash, buf, elems []byte) (*fullNode, error) {
	n := &fullNode{}
	for i := 0; i < 16; i++ {
		cld, rest, err := decodeRef(elems)
		if err != nil {
			return n, wrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i], elems = cld, rest
	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = append(valueNode{}, val...)
	}
	return n, nil
}

const hashLen = len(common.Hash{})

func decodeRef(buf []byte) (node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		// 'embedded' node reference. The encoding must be smaller
		// than a hash in order to be valid.
		if size := len(buf) - len(rest); size > hashLen {
			err := fmt.Errorf("oversized embedded node (size is %d bytes, want size < %d)", size, hashLen)
			return nil, buf, err
		}
		n, err := decodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// empty node
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return append(hashNode{}, val...), rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

// wraps a decoding error with information about the path to the
// invalid child node (for debugging encoding issues).
type decodeError struct {
	what  error
	stack []string
}

func wrapError(err error, ctx string) error {
	if err == nil {
		return nil
	}
	if decErr, ok := err.(*decodeError); ok {
		decErr.stack = append(decErr.stack, ctx)
		return decErr
	}
	return &decodeError{err, []string{ctx}}
}

func (err *decodeError) Error() string {
	return fmt.Sprintf("%v (decode path: %s)", err.what, strings.Join(err.stack, "<-"))
}
