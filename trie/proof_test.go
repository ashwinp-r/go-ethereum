// Copyright 2015 The go-ethereum Authors
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
	crand "crypto/rand"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
)

func init() {
	mrand.Seed(time.Now().Unix())
}

func testProof(t *testing.T) {
	trie, vals := randomTrie(500)
	root := trie.Hash()
	for _, kv := range vals {
		proofs := ethdb.NewMemDatabase()
		if trie.Prove(nil, kv.k, 0, proofs, 0) != nil {
			t.Fatalf("missing key %x while constructing proof", kv.k)
		}
		val, err, _ := VerifyProof(root, kv.k, proofs)
		if err != nil {
			t.Fatalf("VerifyProof error for key %x: %v\nraw proof: %v", kv.k, err, proofs)
		}
		if !bytes.Equal(val, kv.v) {
			t.Fatalf("VerifyProof returned wrong value for key %x: got %x, want %x", kv.k, val, kv.v)
		}
	}
}

func testOneElementProof(t *testing.T) {
	trie := new(Trie)
	updateString(trie, nil, "k", "v")
	proofs := ethdb.NewMemDatabase()
	trie.Prove(nil, []byte("k"), 0, proofs, 0)
	if len(proofs.Keys()) != 1*2 {  // keys[0] is the bucket, keys[1] is the key
		t.Error("proof should have one element")
	}
	val, err, _ := VerifyProof(trie.Hash(), []byte("k"), proofs)
	if err != nil {
		t.Fatalf("VerifyProof error: %v\nproof hashes: %v", err, proofs.Keys())
	}
	if !bytes.Equal(val, []byte("v")) {
		t.Fatalf("VerifyProof returned wrong value: got %x, want 'k'", val)
	}
}

func testVerifyBadProof(t *testing.T) {
	trie, vals := randomTrie(800)
	root := trie.Hash()
	for _, kv := range vals {
		proofs := ethdb.NewMemDatabase()
		trie.Prove(nil, kv.k, 0, proofs, 0)
		if len(proofs.Keys()) == 0 {
			t.Fatal("zero length proof")
		}
		keys := proofs.Keys()
		key := keys[mrand.Intn(len(keys))]
		node, _ := proofs.Get(testbucket, key)
		proofs.Delete(testbucket, key)
		mutateByte(node)
		proofs.Put(testbucket, crypto.Keccak256(node), node)
		if _, err, _ := VerifyProof(root, kv.k, proofs); err == nil {
			t.Fatalf("expected proof to fail for key %x", kv.k)
		}
	}
}

// mutateByte changes one byte in b.
func mutateByte(b []byte) {
	for r := mrand.Intn(len(b)); ; {
		new := byte(mrand.Intn(255))
		if new != b[r] {
			b[r] = new
			break
		}
	}
}

func benchmarkProve(b *testing.B) {
	trie, vals := randomTrie(100)
	var keys []string
	for k := range vals {
		keys = append(keys, k)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := vals[keys[i%len(keys)]]
		proofs := ethdb.NewMemDatabase()
		if trie.Prove(nil, kv.k, 0, proofs, 0); len(proofs.Keys()) == 0 {
			b.Fatalf("zero length proof for %x", kv.k)
		}
	}
}

func benchmarkVerifyProof(b *testing.B) {
	trie, vals := randomTrie(100)
	root := trie.Hash()
	var keys []string
	var proofs []ethdb.Mutation
	for k := range vals {
		keys = append(keys, k)
		proof := ethdb.NewMemDatabase()
		trie.Prove(nil, []byte(k), 0, proof, 0)
		proofs = append(proofs, proof)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		im := i % len(keys)
		if _, err, _ := VerifyProof(root, []byte(keys[im]), proofs[im]); err != nil {
			b.Fatalf("key %x: %v", keys[im], err)
		}
	}
}

var testbucket = []byte("B")

func randomTrie(n int) (*Trie, map[string]*kv) {
	trie := new(Trie)
	trie.prefix = testbucket
	vals := make(map[string]*kv)
	for i := byte(0); i < 100; i++ {
		value := &kv{common.LeftPadBytes([]byte{i}, 32), []byte{i}, false}
		value2 := &kv{common.LeftPadBytes([]byte{i + 10}, 32), []byte{i}, false}
		trie.Update(nil, value.k, value.v, 0)
		trie.Update(nil, value2.k, value2.v, 0)
		vals[string(value.k)] = value
		vals[string(value2.k)] = value2
	}
	for i := 0; i < n; i++ {
		value := &kv{randBytes(32), randBytes(20), false}
		trie.Update(nil, value.k, value.v, 0)
		vals[string(value.k)] = value
	}
	return trie, vals
}

func randBytes(n int) []byte {
	r := make([]byte, n)
	crand.Read(r)
	return r
}
