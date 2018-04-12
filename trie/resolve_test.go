package trie

import (
	//"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1Embedded(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, false)
	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("a"), 0)
	tc := &TrieContinuation{
		t: tr,
		action: TrieActionDelete,
		key: keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		value: nil,
		resolveKey: keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos: 10, // 5 bytes is 10 nibbles
		resolveHash: nil,
		resolved: nil,
		n: nil,
		touched: []Touch{},
	}
	if err := tc.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
}

// Put 1 embedded entry into the database and try to resolve it
func TestResolve1(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, false)
	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	tc := &TrieContinuation{
		t: tr,
		action: TrieActionDelete,
		key: keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		value: nil,
		resolveKey: keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos: 10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("741326629cbf4ba5d5afebd56dd714ba4a531ddb6b07b829aa85dee4d97d34a4").Bytes()),
		resolved: nil,
		n: nil,
		touched: []Touch{},
	}
	if err := tc.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve1 resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve2(t *testing.T) {
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, false)
	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	tc := &TrieContinuation{
		t: tr,
		action: TrieActionDelete,
		key: keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		value: nil,
		resolveKey: keybytesToHex([]byte("aaaaabbbbbaaaaabbbbbaaaaabbbbbaa")),
		resolvePos: 10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
		resolved: nil,
		n: nil,
		touched: []Touch{},
	}
	if err := tc.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2 resolved:\n%s\n", tc.resolved.fstring(""))
}

func TestResolve2Keep(t *testing.T) {
	fmt.Printf("TestResolve2\n")
	db := ethdb.NewMemDatabase()
	tr := New(common.Hash{}, testbucket, false)
	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	tc := &TrieContinuation{
		t: tr,
		action: TrieActionDelete,
		key: keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		value: nil,
		resolveKey: keybytesToHex([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
		resolvePos: 10, // 5 bytes is 10 nibbles
		resolveHash: hashNode(common.HexToHash("c9f98a7d966d37c7231d11910c72f01a213057111b8171f5f137269bb73e45e4").Bytes()),
		resolved: nil,
		n: nil,
		touched: []Touch{},
	}
	if err := tc.ResolveWithDb(db, 0); err != nil {
		t.Errorf("Could not resolve: %v", err)
	}
	//t.Errorf("TestResolve2Keep resolved:\n%s\n", tc.resolved.fstring(""))
}