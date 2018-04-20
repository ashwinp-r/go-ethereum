// Copyright 2017 The go-ethereum Authors
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

package state

import (
	"bytes"
	"fmt"
	"runtime"
	//"runtime/debug"
	//"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	lru "github.com/hashicorp/golang-lru"
)

// Trie cache generation limit after which to evic trie nodes from memory.
var MaxTrieCacheGen = uint32(4*1024*1024)

var AccountsBucket = []byte("AT")
var CodeBucket = []byte("CODE")

const (
	// Number of past tries to keep. This value is chosen such that
	// reasonable chain reorg depths will hit an existing trie.
	maxPastTries = 12

	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 100000
)

type StateReader interface {
	ReadAccountData(address *common.Address) (*Account, error)
	ReadAccountStorage(address *common.Address, key *common.Hash) ([]byte, error)
	ReadAccountCode(addres *common.Address) ([]byte, error)
}

type StateWriter interface {
	UpdateAccountData(address *common.Address, account *Account) error
	UpdateAccountCode(codeHash common.Hash, code []byte) error
	DeleteAccount(address *common.Address) error
	WriteAccountStorage(address *common.Address, key, value *common.Hash) error
}

// Implements StateReader by wrapping database only, without trie
type DbState struct {
	db ethdb.Mutation
	blockNr uint64
}

func NewDbState(db ethdb.Database, blockNr uint64) *DbState {
	return &DbState{
		db: db.NewBatch(),
		blockNr: blockNr,
	}
}

func (dbs *DbState) SetBlockNr(blockNr uint64) {
	dbs.db.DeleteTimestamp(blockNr)
	dbs.blockNr = blockNr
}

func (dbs *DbState) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey, value common.Hash) bool) {
	addrHash := crypto.Keccak256Hash(addr[:])
	var s [32]byte
	copy(s[:], start)
	dbs.db.WalkAsOf(addrHash[:], s[:], 0, dbs.blockNr, func(ks, vs []byte) (bool, error) {
		if vs == nil || len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		key, err := dbs.db.Get(trie.SecureKeyPrefix, ks)
		if err != nil {
			return false, err
		}
		return cb(common.BytesToHash(key), common.BytesToHash(ks), common.BytesToHash(vs)), nil
	})
}

func (dbs *DbState) ReadAccountData(address *common.Address) (*Account, error) {
	seckey := crypto.Keccak256Hash(address[:])
	enc, err := dbs.db.GetAsOf(AccountsBucket, seckey[:], dbs.blockNr)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data Account
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func (dbs *DbState) ReadAccountStorage(address *common.Address, key *common.Hash) ([]byte, error) {
	addrHash := crypto.Keccak256Hash(address[:])
	seckey := crypto.Keccak256Hash(key[:])
	enc, err := dbs.db.GetAsOf(addrHash[:], seckey[:], dbs.blockNr)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (dbs *DbState) ReadAccountCode(address *common.Address) ([]byte, error) {
	account, err := dbs.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, nil
	}
	if bytes.Equal(account.CodeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return dbs.db.Get(CodeBucket, account.CodeHash[:])
}

func (dbs *DbState) UpdateAccountData(address *common.Address, account *Account) error {
	data, err := rlp.EncodeToBytes(account)
	if err != nil {
		return err
	}
	seckey := crypto.Keccak256Hash(address[:])
	return dbs.db.PutS(AccountsBucket, seckey[:], data, dbs.blockNr)
}

func (dbs *DbState) DeleteAccount(address *common.Address) error {
	seckey := crypto.Keccak256Hash(address[:])
	return dbs.db.PutS(AccountsBucket, seckey[:], []byte{}, dbs.blockNr)
}

func (dbs *DbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return dbs.db.Put(CodeBucket, codeHash[:], code)
}

func (dbs *DbState) WriteAccountStorage(address *common.Address, key, value *common.Hash) error {
	addrHash := crypto.Keccak256Hash(address[:])
	seckey := crypto.Keccak256Hash(key[:])
	v := bytes.TrimLeft(value[:], "\x00") // PutS below will make a copy of v
	return dbs.db.PutS(addrHash[:], seckey[:], v, dbs.blockNr)
}

// Implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                *trie.Trie
	hashKeyCache     *lru.Cache
	db               Database
	nodeList         *trie.List
	blockNr          uint64
	storageTries     map[string]*trie.Trie
	continuations    []*trie.TrieContinuation
	accountUpdates   map[common.Address]*Account
	accountDeletes   map[common.Address]struct{}
	updatedStorage   map[common.Address]*trie.Trie
}

func NewTrieDbState(root common.Hash, db Database, blockNr uint64) (*TrieDbState, error) {
	hashKeyCache, err := lru.New(1024*1024)
	if err != nil {
		return nil, err
	}
	t := trie.New(root, AccountsBucket, false)
	tds := TrieDbState{
		t: t,
		hashKeyCache: hashKeyCache,
		db: db,
		nodeList: trie.NewList(),
		blockNr: blockNr,
		storageTries: make(map[string]*trie.Trie),
		continuations: []*trie.TrieContinuation{},
		accountUpdates: make(map[common.Address]*Account),
		accountDeletes: make(map[common.Address]struct{}),
		updatedStorage: make(map[common.Address]*trie.Trie),
	}
	t.MakeListed(tds.nodeList)
	return &tds, nil
}

func (tds *TrieDbState) Copy() (*TrieDbState, error) {
	hashKeyCache, err := lru.New(1024*1024)
	if err != nil {
		return nil, err
	}
	tcopy := *tds.t
	cpy := TrieDbState{
		t: &tcopy,
		hashKeyCache: hashKeyCache,
		db: tds.db,
		nodeList: nil,
		blockNr: tds.blockNr,
		storageTries: make(map[string]*trie.Trie),
		accountUpdates: make(map[common.Address]*Account),
		accountDeletes: make(map[common.Address]struct{}),
		updatedStorage: make(map[common.Address]*trie.Trie),
	}
	return &cpy, nil
}

func (tds *TrieDbState) Database() Database {
	return tds.db
}

func (tds *TrieDbState) AccountTrie() *trie.Trie {
	return tds.t
}

type AddrList []common.Address

func (l AddrList) Len() int {
	return len(l)
}
func (l AddrList) Less(i, j int) bool {
	return bytes.Compare(l[i][:], l[j][:]) < 0
}
func (l AddrList) Swap(i, j int) {
	var a common.Address
	a.Set(l[i])
	l[i].Set(l[j])
	l[j].Set(a)
}

func (tds *TrieDbState) TrieRoot() (common.Hash, error) {
	if len(tds.continuations) == 0 && len(tds.updatedStorage) == 0 && len(tds.accountUpdates) == 0 && len(tds.accountDeletes) == 0 {
		return tds.t.Hash(), nil
	}
	//fmt.Printf("tds.blockNr %d\n", tds.blockNr)
	//fmt.Printf("TrieRoot. Continuatons: %d, updatedStorage: %d, accountUpdates: %d, accountDeletes: %d\n",
	//	len(tds.continuations),
	//	len(tds.updatedStorage),
	//	len(tds.accountUpdates),
	//	len(tds.accountDeletes),
	//)
	//for addr, _ := range tds.accountUpdates {
	//	fmt.Printf("Udate account %x\n", addr)
	//}
	// First, execute storage updates and account deletions
	oldContinuations := tds.continuations
	newContinuations := []*trie.TrieContinuation{}
	for len(oldContinuations) > 0 {
		resolvers := make(map[common.Address]*trie.TrieResolver)
		for _, c := range oldContinuations {
			if !c.RunWithDb(tds.db.TrieDB()) {
				newContinuations = append(newContinuations, c)
				resolver, ok := resolvers[*c.Address()]
				if !ok {
					resolver = c.Trie().NewResolver(tds.db.TrieDB())
					resolvers[*c.Address()] = resolver
				}
				resolver.AddContinuation(c)
			}
		}
		//al := make(AddrList, 0)
		//for a, _ := range resolvers {
		//	al = append(al, a)
		//}
		//sort.Sort(al)
		for _, resolver := range resolvers {
			//resolver := resolvers[a]
			if err := resolver.ResolveWithDb(tds.db.TrieDB(), tds.blockNr); err != nil {
				return common.Hash{}, err
			}
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
	}
	//for _, storageTrie := range tds.updatedStorage {
	//	storageTrie.Relist()
	//	if storageTrie.IsMalformed() {
	//		fmt.Printf("Storage trie is malformed\n")
	//	}
	//}
	tds.updatedStorage = make(map[common.Address]*trie.Trie)
	for address, account := range tds.accountUpdates {
		storageTrie, err := tds.getStorageTrie(&address)
		if err != nil {
			return common.Hash{}, err
		}
		account.Root = storageTrie.Hash()
		data, err := rlp.EncodeToBytes(account)
		if err != nil {
			return common.Hash{}, err
		}
		addrHash, err := tds.HashKey(address[:])
		if err != nil {
			return common.Hash{}, err
		}
		c := tds.t.UpdateAction(nil, addrHash[:], data)
		oldContinuations = append(oldContinuations, c)
	}
	dels := len(tds.accountDeletes)
	for address, _ := range tds.accountDeletes {
		addrHash, err := tds.HashKey(address[:])
		if err != nil {
			return common.Hash{}, err
		}
		c := tds.t.DeleteAction(nil, addrHash[:])
		oldContinuations = append(oldContinuations, c)
	}
	tds.accountUpdates = make(map[common.Address]*Account)
	tds.accountDeletes = make(map[common.Address]struct{})
	it := 0
	for len(oldContinuations) > 0 {
		resolver := tds.t.NewResolver(tds.db.TrieDB())
		for _, c := range oldContinuations {
			if !c.RunWithDb(tds.db.TrieDB()) {
				newContinuations = append(newContinuations, c)
				resolver.AddContinuation(c)
			}
		}
		if len(newContinuations) > 0 {
			if err := resolver.ResolveWithDb(tds.db.TrieDB(), tds.blockNr); err != nil {
				return common.Hash{}, err
			}
//		} else {
//			if tds.t.IsMalformed() {
//				fmt.Printf("Account trie is malformed\n")
//			}
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
		it++
	}
	if dels == 0 && it > 2 {
		fmt.Printf("Resolved in %d iterations\n", it)
	}
	tds.t.SaveHashes(tds.db.TrieDB())
	tds.t.Relist()
	tds.continuations = newContinuations
	hash := tds.t.Hash()
	//fmt.Printf("Root hash: %x\n", hash)
	return hash, nil
}

func (tds *TrieDbState) TrieRootOld() (common.Hash, error) {
	if len(tds.continuations) == 0 && len(tds.updatedStorage) == 0 && len(tds.accountUpdates) == 0 && len(tds.accountDeletes) == 0 {
		return tds.t.Hash(), nil
	}
	//fmt.Printf("TrieRoot. Continuatons: %d, updatedStorage: %d, accountUpdates: %d, accountDeletes: %d, Stack:\n%s\n",
	//	len(tds.continuations),
	//	len(tds.updatedStorage),
	//	len(tds.accountUpdates),
	//	len(tds.accountDeletes),
	//	debug.Stack(),
	//)
	//for addr, _ := range tds.accountUpdates {
	//	fmt.Printf("Udate account %x\n", addr)
	//}
	// First, execute storage updates and account deletions
	oldContinuations := tds.continuations
	newContinuations := []*trie.TrieContinuation{}
	for len(oldContinuations) > 0 {
		for _, c := range oldContinuations {
			for !c.RunWithDb(tds.db.TrieDB()) {
				if err := c.ResolveWithDb(tds.db.TrieDB(), tds.blockNr); err != nil {
					return common.Hash{}, err
				}
			}
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
	}
	for _, storageTrie := range tds.updatedStorage {
		storageTrie.Relist()
	}
	tds.updatedStorage = make(map[common.Address]*trie.Trie)
	for address, account := range tds.accountUpdates {
		storageTrie, err := tds.getStorageTrie(&address)
		if err != nil {
			return common.Hash{}, err
		}
		account.Root = storageTrie.Hash()
		data, err := rlp.EncodeToBytes(account)
		if err != nil {
			return common.Hash{}, err
		}
		addrHash, err := tds.HashKey(address[:])
		if err != nil {
			return common.Hash{}, err
		}
		c := tds.t.UpdateAction(nil, addrHash[:], data)
		oldContinuations = append(oldContinuations, c)
	}
	for address, _ := range tds.accountDeletes {
		addrHash, err := tds.HashKey(address[:])
		if err != nil {
			return common.Hash{}, err
		}
		c := tds.t.DeleteAction(nil, addrHash[:])
		oldContinuations = append(oldContinuations, c)
	}
	tds.accountUpdates = make(map[common.Address]*Account)
	tds.accountDeletes = make(map[common.Address]struct{})
	for len(oldContinuations) > 0 {
		for _, c := range oldContinuations {
			for !c.RunWithDb(tds.db.TrieDB()) {
				if err := c.ResolveWithDb(tds.db.TrieDB(), tds.blockNr); err != nil {
					return common.Hash{}, err
				}
			}
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
	}
	tds.t.SaveHashes(tds.db.TrieDB())
	tds.t.Relist()
	tds.continuations = newContinuations
	hash := tds.t.Hash()
	//fmt.Printf("Root hash: %x\n", hash)
	//ic("")
	return hash, nil
}

func (tds *TrieDbState) Rebuild() error {
	tr := tds.AccountTrie()
	tr.Rebuild(tds.db.TrieDB(), tds.blockNr)
	return nil
}

func (tds *TrieDbState) SetBlockNr(blockNr uint64) {
	tds.blockNr = blockNr
}

func (tds *TrieDbState) ReadAccountData(address *common.Address) (*Account, error) {
	addrHash, err := tds.HashKey(address[:])
	if err != nil {
		return nil, err
	}
	enc, err := tds.t.TryGet(tds.db.TrieDB(), addrHash, tds.blockNr)
	if err != nil {
		return nil, err
	}
	if enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data Account
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func (tds *TrieDbState) HashKey(key []byte) ([]byte, error) {
	keyStr := string(key)
	if cached, ok := tds.hashKeyCache.Get(keyStr); ok {
		return cached.([]byte), nil
	}
	hash := crypto.Keccak256Hash(key)
	tds.hashKeyCache.Add(string(common.CopyBytes(key)), hash[:])
	if err := tds.db.TrieDB().Put(trie.SecureKeyPrefix, hash[:], key); err != nil {
		return nil, err
	}
	return hash[:], nil
}

func (tds *TrieDbState) GetKey(shaKey []byte) []byte {
	key, _ := tds.db.TrieDB().Get(trie.SecureKeyPrefix, shaKey)
	return key
}

func (tds *TrieDbState) getStorageTrie(address *common.Address) (*trie.Trie, error) {
	t, ok := tds.storageTries[string(address[:])]
	if !ok {
		account, err := tds.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		addrHash, err := tds.HashKey(address[:])
		if err != nil {
			return nil, err
		}
		if account == nil {
			t = trie.New(common.Hash{}, addrHash[:], true)
		} else {
			t = trie.New(account.Root, addrHash[:], true)
		}
		t.MakeListed(tds.nodeList)
		tds.storageTries[string(common.CopyBytes(address[:]))] = t
	}
	return t, nil
}

func (tds *TrieDbState) ReadAccountStorage(address *common.Address, key *common.Hash) ([]byte, error) {
	t, err := tds.getStorageTrie(address)
	if err != nil {
		return nil, err
	}
	seckey, err := tds.HashKey(key[:])
	if err != nil {
		return nil, err
	}
	enc, err := t.TryGet(tds.db.TrieDB(), seckey, tds.blockNr)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

func (tds *TrieDbState) ReadAccountCode(address *common.Address) ([]byte, error) {
	account, err := tds.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, nil
	}
	if bytes.Equal(account.CodeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return tds.db.TrieDB().Get(CodeBucket, account.CodeHash[:])
}

var prevMemStats runtime.MemStats

func (tds *TrieDbState) PruneTries() {
	listLen := tds.nodeList.Len()
	if listLen > int(MaxTrieCacheGen) {
		tds.nodeList.ShrinkTo(int(MaxTrieCacheGen))
		nodeCount := 0
		for addr, storageTrie := range tds.storageTries {
			count, empty, _ := storageTrie.TryPrune()
			nodeCount += count
			if empty {
				delete(tds.storageTries, addr)
			}
		}
		count, _, _ := tds.t.TryPrune()
		nodeCount += count
		log.Info("Nodes", "trie", nodeCount, "list", tds.nodeList.Len(), "list before pruning", listLen)
	} else {
		log.Info("Nodes", "list", tds.nodeList.Len())
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	churn := (m.TotalAlloc - prevMemStats.TotalAlloc) - (prevMemStats.Alloc - m.Alloc)
	log.Info("Memory", "alloc", int(m.Alloc / 1024), "churn", int(churn / 1024), "sys", int(m.Sys / 1024), "numGC", int(m.NumGC))
	prevMemStats = m
}

type TrieStateWriter struct {
	tds *TrieDbState
}

type DbStateWriter struct {
	tds *TrieDbState
}

func (tds *TrieDbState) TrieStateWriter() *TrieStateWriter {
	return &TrieStateWriter{tds: tds}
}

func (tds *TrieDbState) DbStateWriter() *DbStateWriter {
	return &DbStateWriter{tds: tds}
}

var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

func (tsw *TrieStateWriter) UpdateAccountData(address *common.Address, account *Account) error {
	tsw.tds.accountUpdates[*address] = account
	return nil
}

func (dsw *DbStateWriter) UpdateAccountData(address *common.Address, account *Account) error {
	storageTrie, err := dsw.tds.getStorageTrie(address)
	if err != nil {
		return err
	}
	account.Root = storageTrie.Hash()
	data, err := rlp.EncodeToBytes(account)
	if err != nil {
		return err
	}
	seckey, err := dsw.tds.HashKey(address[:])
	if err != nil {
		return err
	}
	return dsw.tds.db.TrieDB().PutS(AccountsBucket, seckey, data, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) DeleteAccount(address *common.Address) error {
	storageTrie, err := tsw.tds.getStorageTrie(address)
	if err != nil {
		return err
	}
	storageTrie.Unlink()
	delete(tsw.tds.storageTries, string(address[:]))
	tsw.tds.accountDeletes[*address] = struct{}{}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(address *common.Address) error {
	seckey, err := dsw.tds.HashKey(address[:])
	if err != nil {
		return err
	}
	return dsw.tds.db.TrieDB().PutS(AccountsBucket, seckey, []byte{}, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return dsw.tds.db.TrieDB().Put(CodeBucket, codeHash[:], code)
}

func (tsw *TrieStateWriter) WriteAccountStorage(address *common.Address, key, value *common.Hash) error {
	storageTrie, err := tsw.tds.getStorageTrie(address)
	if err != nil {
		return err
	}
	seckey, err := tsw.tds.HashKey(key[:])
	if err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00")
	var c *trie.TrieContinuation
	if len(v) > 0 {
		c = storageTrie.UpdateAction(address, seckey, common.CopyBytes(v))
	} else {
		c = storageTrie.DeleteAction(address, seckey)
	}
	tsw.tds.updatedStorage[*address] = storageTrie
	tsw.tds.continuations = append(tsw.tds.continuations, c)
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(address *common.Address, key, value *common.Hash) error {
	addrHash, err := dsw.tds.HashKey(address[:])
	if err != nil {
		return err
	}
	seckey, err := dsw.tds.HashKey(key[:])
	if err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00") // PutS below will make a copy of v
	return dsw.tds.db.TrieDB().PutS(addrHash, seckey, v, dsw.tds.blockNr)
}

// Database wraps access to tries and contract code.
type Database interface {
	// OpenTrie opens the main account trie.
	OpenTrie(root common.Hash) (Trie, error)

	// OpenStorageTrie opens the storage trie of an account.
	OpenStorageTrie(addrHash, root common.Hash) (Trie, error)

	// CopyTrie returns an independent copy of the given trie.
	CopyTrie(Trie) Trie

	// ContractCode retrieves a particular contract's code.
	ContractCode(addrHash, codeHash common.Hash) ([]byte, error)

	// ContractCodeSize retrieves a particular contracts code's size.
	ContractCodeSize(addrHash, codeHash common.Hash) (int, error)

	// TrieDB retrieves the low level trie database used for data storage.
	TrieDB() ethdb.Database
}

// Trie is a Ethereum Merkle Trie.
type Trie interface {
	Prove(db ethdb.Database, key []byte, fromLevel uint, proofDb ethdb.Putter, blockNr uint64) error
	TryGet(db ethdb.Database, key []byte, blockNr uint64) ([]byte, error)
	TryUpdate(db ethdb.Database, key, value []byte, blockNr uint64) error
	TryDelete(db ethdb.Database, key []byte, blockNr uint64) error
	Hash() common.Hash
	NodeIterator(db ethdb.Database, startKey []byte, blockNr uint64) trie.NodeIterator
	HashKey([]byte) []byte
	GetKey(trie.DatabaseReader, []byte) []byte // TODO(fjl): remove this when SecureTrie is removed
	PrintTrie()
	TryPrune() (int, bool, error)
	MakeListed(*trie.List)
	Unlink()
}

// NewDatabase creates a backing store for state. The returned database is safe for
// concurrent use and retains cached trie nodes in memory. The pool is an optional
// intermediate trie-node memory pool between the low level storage layer and the
// high level trie abstraction.
func NewDatabase(db ethdb.Database) Database {
	csc, _ := lru.New(codeSizeCacheSize)
	return &cachingDB{
		db:            db,
		codeSizeCache: csc,
	}
}

type cachingDB struct {
	db            ethdb.Database
	codeSizeCache *lru.Cache
}

// OpenTrie opens the main account trie.
func (db *cachingDB) OpenTrie(root common.Hash) (Trie, error) {
	return trie.NewSecure(root, AccountsBucket, false)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *cachingDB) OpenStorageTrie(addrHash, root common.Hash) (Trie, error) {
	return trie.NewSecure(root, addrHash[:], true)
}

// CopyTrie returns an independent copy of the given trie.
func (db *cachingDB) CopyTrie(t Trie) Trie {
	switch t := t.(type) {
	case *trie.SecureTrie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

// ContractCode retrieves a particular contract's code.
func (db *cachingDB) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
	code, err := db.db.Get(CodeBucket, codeHash[:])
	if err == nil {
		db.codeSizeCache.Add(codeHash, len(code))
	}
	return code, err
}

// ContractCodeSize retrieves a particular contracts code's size.
func (db *cachingDB) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
	if cached, ok := db.codeSizeCache.Get(codeHash); ok {
		return cached.(int), nil
	}
	code, err := db.ContractCode(addrHash, codeHash)
	if err == nil {
		db.codeSizeCache.Add(codeHash, len(code))
	}
	return len(code), err
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *cachingDB) TrieDB() ethdb.Database {
	return db.db
}
