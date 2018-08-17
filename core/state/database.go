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
	"math/big"
	//"runtime/debug"
	//"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	lru "github.com/hashicorp/golang-lru"
	"github.com/petar/GoLLRB/llrb"
)

// Trie cache generation limit after which to evict trie nodes from memory.
var MaxTrieCacheGen = uint32(4*1024*1024)

var AccountsBucket = []byte("AT")
var AccountsHistoryBucket = []byte("hAT")
var StorageBucket = []byte("ST")
var StorageHistoryBucket = []byte("hST")
var CodeBucket = []byte("CODE")

const (
	// Number of past tries to keep. This value is chosen such that
	// reasonable chain reorg depths will hit an existing trie.
	maxPastTries = 12

	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 100000
)

type StateReader interface {
	ReadAccountData(addrHash common.Hash) (*Account, error)
	ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error)
	ReadAccountCode(codeHash common.Hash) ([]byte, error)
	ReadAccountCodeSize(codeHash common.Hash) (int, error)
}

type StateWriter interface {
	UpdateAccountData(address common.Address, original, account *Account) error
	UpdateAccountCode(codeHash common.Hash, code []byte) error
	DeleteAccount(address common.Address) error
	WriteAccountStorage(address common.Address, key, value *common.Hash) error
}

type storageItem struct {
	key, seckey, value common.Hash
}

func (a *storageItem) Less(b llrb.Item) bool {
	bi := b.(*storageItem)
	return bytes.Compare(a.seckey[:], bi.seckey[:]) < 0
}

// Implements StateReader by wrapping database only, without trie
type DbState struct {
	db ethdb.Getter
	blockNr uint64
	storage map[common.Address]*llrb.LLRB
}

func NewDbState(db ethdb.Getter, blockNr uint64) *DbState {
	return &DbState{
		db: db,
		blockNr: blockNr,
		storage: make(map[common.Address]*llrb.LLRB),
	}
}

func (dbs *DbState) SetBlockNr(blockNr uint64) {
	dbs.blockNr = blockNr
}

func (dbs *DbState) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey, value common.Hash) bool, maxResults int) {
	st := llrb.New()
	var s [20+32]byte
	copy(s[:], addr[:])
	copy(s[20:], start)
	var lastSecKey common.Hash
	dbs.db.WalkAsOf(StorageHistoryBucket, s[:], 0, dbs.blockNr, func(ks, vs []byte) (bool, error) {
		if !bytes.HasPrefix(ks, addr[:]) {
			return false, nil
		}
		if vs == nil || len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		seckey := ks[20:]
		key, err := dbs.db.Get(trie.SecureKeyPrefix, seckey)
		if err != nil {
			return false, err
		}
		si := storageItem{}
		copy(si.key[:], key)
		copy(si.seckey[:], seckey)
		si.value.SetBytes(vs)
		copy(lastSecKey[:], seckey)
		st.ReplaceOrInsert(&si)
		return st.Len() < maxResults, nil
	})
	// Override
	min := &storageItem{seckey: common.BytesToHash(start)}
	if t, ok := dbs.storage[addr]; ok {
		t.AscendGreaterOrEqual1(min, func(i llrb.Item) bool {
			item := i.(*storageItem)
			if bytes.Compare(item.seckey[:], lastSecKey[:]) > 0 {
				// Overriding further will cause the result set grow beyond maxResults
				return false
			}
			st.ReplaceOrInsert(item)
			return true
		})
	}
	results := 0
	st.AscendGreaterOrEqual1(min, func(i llrb.Item) bool {
		item := i.(*storageItem)
		cb(item.key, item.seckey, item.value)
		results++
		return results < maxResults
	})
}

func (dbs *DbState) ReadAccountData(addrHash common.Hash) (*Account, error) {
	enc, err := dbs.db.GetAsOf(AccountsHistoryBucket, addrHash[:], dbs.blockNr)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data Account
	// Kind of hacky
	if len(enc) == 1 {
		data.Balance = new(big.Int)
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else if len(enc) < 60 {
		var extData ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			return nil, err
		}
		data.Nonce = extData.Nonce
		data.Balance = extData.Balance
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else {
		if err := rlp.DecodeBytes(enc, &data); err != nil {
			return nil, err
		}
	}
	return &data, nil
}

func (dbs *DbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	seckey := crypto.Keccak256Hash(key[:])
	enc, err := dbs.db.GetAsOf(StorageHistoryBucket, append(address[:], seckey[:]...), dbs.blockNr)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (dbs *DbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return dbs.db.Get(CodeBucket, codeHash[:])
}

func (dbs *DbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := dbs.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (dbs *DbState) UpdateAccountData(address common.Address, original, account *Account) error {
	return nil
}

func (dbs *DbState) DeleteAccount(address common.Address) error {
	return nil
}

func (dbs *DbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (dbs *DbState) WriteAccountStorage(address common.Address, key, value *common.Hash) error {
	t, ok := dbs.storage[address]
	if !ok {
		t = llrb.New()
		dbs.storage[address] = t
	}
	t.ReplaceOrInsert(&storageItem{key: *key, seckey: crypto.Keccak256Hash(key[:]), value: *value})
	return nil
}

// Implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                *trie.Trie
	addrHashCache    *lru.Cache
	keyHashCache     *lru.Cache
	db               ethdb.Database
	nodeList         *trie.List
	blockNr          uint64
	storageTries     map[common.Hash]*trie.Trie
	storageUpdates   map[common.Address]map[common.Hash][]byte
	accountUpdates   map[common.Hash]*Account
	deleted          map[common.Hash]struct{}
	codeCache        *lru.Cache
	codeSizeCache    *lru.Cache
	historical       bool
}

func NewTrieDbState(root common.Hash, db ethdb.Database, blockNr uint64) (*TrieDbState, error) {
	addrHashCache, err := lru.New(128*1024)
	if err != nil {
		return nil, err
	}
	keyHashCache, err := lru.New(128*1024)
	if err != nil {
		return nil, err
	}
	csc, err := lru.New(100000)
	if err != nil {
		return nil, err
	}
	cc, err := lru.New(10000)
	if err != nil {
		return nil, err
	}
	t := trie.New(root, AccountsBucket, nil, false)
	tds := TrieDbState{
		t: t,
		addrHashCache: addrHashCache,
		keyHashCache: keyHashCache,
		db: db,
		nodeList: trie.NewList(),
		blockNr: blockNr,
		storageTries: make(map[common.Hash]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*Account),
		deleted: make(map[common.Hash]struct{}),
		codeCache: cc,
		codeSizeCache: csc,
	}
	t.MakeListed(tds.nodeList)
	return &tds, nil
}

func (tds *TrieDbState) SetHistorical(h bool) {
	tds.historical = h
	tds.t.SetHistorical(h)
}

func (tds *TrieDbState) Copy() *TrieDbState {
	addrHashCache, err := lru.New(128*1024)
	if err != nil {
		panic(err)
	}
	keyHashCache, err := lru.New(128*1024)
	if err != nil {
		panic(err)
	}
	tcopy := *tds.t
	cpy := TrieDbState{
		t: &tcopy,
		addrHashCache: addrHashCache,
		keyHashCache: keyHashCache,
		db: tds.db,
		nodeList: nil,
		blockNr: tds.blockNr,
		storageTries: make(map[common.Hash]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*Account),
		deleted: make(map[common.Hash]struct{}),
	}
	return &cpy
}

func (tds *TrieDbState) Database() ethdb.Database {
	return tds.db
}

func (tds *TrieDbState) AccountTrie() *trie.Trie {
	return tds.t
}

func (tds *TrieDbState) TrieRoot() (common.Hash, error) {
	return tds.trieRoot(true)
}

func (tds *TrieDbState) trieRoot(forward bool) (common.Hash, error) {
	if len(tds.storageUpdates) == 0 && len(tds.accountUpdates) == 0 {
		return tds.t.Hash(), nil
	}
	//for address, account := range tds.accountUpdates {
	//	fmt.Printf("%x %d %x %x\n", address[:], account.Balance, account.CodeHash, account.Root[:])
	//}
	//fmt.Printf("=================\n")
	oldContinuations := []*trie.TrieContinuation{}
	newContinuations := []*trie.TrieContinuation{}
	relist := []*trie.Trie{}
	for address, m := range tds.storageUpdates {
		addrHash, err := tds.HashAddress(address, false /*save*/)
		if err != nil {
			return common.Hash{}, nil
		}
		if _, ok := tds.deleted[addrHash]; ok {
			continue
		}
		storageTrie, err := tds.getStorageTrie(address, addrHash, true)
		if err != nil {
			return common.Hash{}, err
		}
		for keyHash, v := range m {
			var c *trie.TrieContinuation
			if len(v) > 0 {
				c = storageTrie.UpdateAction(keyHash[:], v)
			} else {
				c = storageTrie.DeleteAction(keyHash[:])
			}
			oldContinuations = append(oldContinuations, c)
		}
		relist = append(relist, storageTrie)
	}
	it := 0
	for len(oldContinuations) > 0 {
		var resolver *trie.TrieResolver
		for _, c := range oldContinuations {
			if !c.RunWithDb(tds.db) {
				newContinuations = append(newContinuations, c)
				if resolver == nil {
					resolver = trie.NewResolver(tds.db, false, false)
					resolver.SetHistorical(tds.historical)
				}
				resolver.AddContinuation(c)
			}
		}
		if len(newContinuations) > 0 {
			if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
				return common.Hash{}, err
			}
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
		it++
	}
	if it > 3 {
		fmt.Printf("Resolved storage in %d iterations\n", it)
	}
	for _, storageTrie := range relist {
		storageTrie.Relist()
	}
	oldContinuations = []*trie.TrieContinuation{}
	newContinuations = []*trie.TrieContinuation{}
	tds.storageUpdates = make(map[common.Address]map[common.Hash][]byte)
	for addrHash, account := range tds.accountUpdates {
		var c *trie.TrieContinuation
		// first argument to getStorageTrie is not used unless the last one == true
		storageTrie, err := tds.getStorageTrie(common.Address{}, addrHash, false)
		if err != nil {
			return common.Hash{}, err
		}
		deleteStorageTrie := false
		if account != nil {
			if _, ok := tds.deleted[addrHash]; ok {
				deleteStorageTrie = true
				account.Root = emptyRoot
			} else if storageTrie != nil {
				account.Root = storageTrie.Hash()
			}
			//fmt.Printf("Set root %x %x\n", address[:], account.Root[:])
			data, err := rlp.EncodeToBytes(account)
			if err != nil {
				return common.Hash{}, err
			}
			c = tds.t.UpdateAction(addrHash[:], data)
		} else {
			deleteStorageTrie = true
			c = tds.t.DeleteAction(addrHash[:])
		}
		if deleteStorageTrie && storageTrie != nil {
			storageTrie.Unlink()
			delete(tds.storageTries, addrHash)
		}
		oldContinuations = append(oldContinuations, c)
	}
	tds.accountUpdates = make(map[common.Hash]*Account)
	tds.deleted = make(map[common.Hash]struct{})
	it = 0
	for len(oldContinuations) > 0 {
		var resolver *trie.TrieResolver
		for _, c := range oldContinuations {
			if !c.RunWithDb(tds.db) {
				newContinuations = append(newContinuations, c)
				if resolver == nil {
					resolver = trie.NewResolver(tds.db, false, true)
					resolver.SetHistorical(tds.historical)
				}
				resolver.AddContinuation(c)
			}
		}
		if len(newContinuations) > 0 {
			if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
				return common.Hash{}, err
			}
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
		it++
	}
	if it > 3 {
		fmt.Printf("Resolved in %d iterations\n", it)
	}
	hash := tds.t.Hash()
	tds.t.SaveHashes(tds.db)
	tds.t.Relist()
	return hash, nil
}


func (tds *TrieDbState) Rebuild() error {
	tr := tds.AccountTrie()
	tr.Rebuild(tds.db, tds.blockNr)
	return nil
}

func (tds *TrieDbState) SetBlockNr(blockNr uint64) {
	tds.blockNr = blockNr
}

func (tds *TrieDbState) UnwindTo(blockNr uint64, commit bool) error {
	batch := tds.db.NewBatch()
	fmt.Printf("Rewinding from block %d to block %d\n", tds.blockNr, blockNr)
	if err := tds.db.RewindData(tds.blockNr, blockNr, func (bucket, key, value []byte) error {
		//fmt.Printf("Rewind with bucket %x key %x value %x\n", bucket, key, value)
		var err error
		if bytes.Equal(bucket, AccountsHistoryBucket) {
			var addrHash common.Hash
			copy(addrHash[:], key)
			if len(value) > 0 {
				tds.accountUpdates[addrHash], err = encodingToAccount(value)
				if err != nil {
					return err
				}
				err = batch.Put(AccountsBucket, key, value)
				if err != nil {
					return err
				}
			} else {
				tds.accountUpdates[addrHash] = nil
				tds.deleted[addrHash] = struct{}{}
				err = batch.Delete(AccountsBucket, key)
				if err != nil {
					return err
				}
			}
		} else if bytes.Equal(bucket, StorageHistoryBucket) {
			var address common.Address
			copy(address[:], key[:20])
			var keyHash common.Hash
			copy(keyHash[:], key[20:])
			m, ok := tds.storageUpdates[address]
			if !ok {
				m = make(map[common.Hash][]byte)
				tds.storageUpdates[address] = m
			}
			m[keyHash] = value
			if len(value) > 0 {
				batch.Put(StorageBucket, key, value)
			} else {
				batch.Delete(StorageBucket, key)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if _, err := tds.trieRoot(false); err != nil {
		return err
	}
	if commit {
		for i := tds.blockNr; i > blockNr; i-- {
			if err := batch.DeleteTimestamp(i); err != nil {
				return err
			}
		}
		if err := batch.Commit(); err != nil {
			return err
		}
	}
	tds.blockNr = blockNr
	return nil
}

func encodingToAccount(enc []byte) (*Account, error) {
	if enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data Account
	// Kind of hacky
	if len(enc) == 1 {
		data.Balance = new(big.Int)
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else if len(enc) < 60 {
		var extData ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			return nil, err
		}
		data.Nonce = extData.Nonce
		data.Balance = extData.Balance
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else {
		if err := rlp.DecodeBytes(enc, &data); err != nil {
			return nil, err
		}
	}
	return &data, nil
}

func (tds *TrieDbState) ReadAccountData(addrHash common.Hash) (*Account, error) {
	enc, _, err := tds.t.TryGet(tds.db, addrHash[:], tds.blockNr)
	if err != nil {
		return nil, err
	}
	return encodingToAccount(enc)
}

func (tds *TrieDbState) savePreimage(save bool, hash, preimage []byte) error {
	if !save {
		return nil
	}
	return tds.db.Put(trie.SecureKeyPrefix, hash, preimage)
}

func (tds *TrieDbState) HashAddress(address common.Address, save bool) (common.Hash, error) {
	if cached, ok := tds.addrHashCache.Get(address); ok {
		var hash common.Hash
		copy(hash[:], cached.([]byte))
		return hash, tds.savePreimage(save, hash[:], address[:])
	}
	hash := crypto.Keccak256Hash(address[:])
	tds.addrHashCache.Add(address, hash[:])
	return hash, tds.savePreimage(save, hash[:], address[:])
}

func (tds *TrieDbState) HashKey(key common.Hash, save bool) (common.Hash, error) {
	if cached, ok := tds.keyHashCache.Get(key); ok {
		var hash common.Hash
		copy(hash[:], cached.([]byte))
		return hash, tds.savePreimage(save, hash[:], key[:])
	}
	hash := crypto.Keccak256Hash(key[:])
	tds.keyHashCache.Add(key, hash[:])
	return hash, tds.savePreimage(save, hash[:], key[:])
}

func (tds *TrieDbState) GetKey(shaKey []byte) []byte {
	key, _ := tds.db.Get(trie.SecureKeyPrefix, shaKey)
	return key
}

func (tds *TrieDbState) getStorageTrie(address common.Address, addrHash common.Hash, create bool) (*trie.Trie, error) {
	t, ok := tds.storageTries[addrHash]
	if !ok && create {
		account, err := tds.ReadAccountData(addrHash)
		if err != nil {
			return nil, err
		}
		if account == nil {
			t = trie.New(common.Hash{}, StorageBucket, address[:], true)
		} else {
			t = trie.New(account.Root, StorageBucket, address[:], true)
		}
		t.SetHistorical(tds.historical)
		t.MakeListed(tds.nodeList)
		tds.storageTries[addrHash] = t
	}
	return t, nil
}

func (tds *TrieDbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	addrHash, err := tds.HashAddress(address, false /*save*/)
	if err != nil {
		return nil, err
	}
	t, err := tds.getStorageTrie(address, addrHash, true)
	if err != nil {
		return nil, err
	}
	seckey, err := tds.HashKey(*key, false /*save*/)
	if err != nil {
		return nil, err
	}
	enc, _, err := t.TryGet(tds.db, seckey[:], tds.blockNr)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

func (tds *TrieDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if cached, ok := tds.codeCache.Get(codeHash); ok {
		return cached.([]byte), nil
	}
	code, err := tds.db.Get(CodeBucket, codeHash[:])
	if err == nil {
		tds.codeSizeCache.Add(codeHash, len(code))
		tds.codeCache.Add(codeHash, code)
	}
	return code, err
}

func (tds *TrieDbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	if cached, ok := tds.codeSizeCache.Get(codeHash); ok {
		return cached.(int), nil
	}
	code, err := tds.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

var prevMemStats runtime.MemStats

func (tds *TrieDbState) PruneTries() {
	listLen := tds.nodeList.Len()
	if listLen > int(MaxTrieCacheGen) {
		tds.nodeList.ShrinkTo(int(MaxTrieCacheGen))
		nodeCount := 0
		for addrHash, storageTrie := range tds.storageTries {
			count, empty := storageTrie.TryPrune()
			nodeCount += count
			if empty {
				delete(tds.storageTries, addrHash)
			}
		}
		count, _ := tds.t.TryPrune()
		nodeCount += count
		log.Info("Nodes", "trie", nodeCount, "list", tds.nodeList.Len(), "list before pruning", listLen)
	} else {
		log.Info("Nodes", "list", tds.nodeList.Len())
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Memory", "alloc", int(m.Alloc / 1024), "sys", int(m.Sys / 1024), "numGC", int(m.NumGC))
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

func accountsEqual(a1, a2 *Account) bool {
	return a1.Nonce == a2.Nonce && a1.Balance.Cmp(a2.Balance) == 0 && a1.Root == a2.Root && bytes.Equal(a1.CodeHash, a2.CodeHash)
}

func (tsw *TrieStateWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	addrHash, err := tsw.tds.HashAddress(address, false /*save*/)
	if err != nil {
		return err
	}
	tsw.tds.accountUpdates[addrHash] = account
	return nil
}

func (dsw *DbStateWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	var data []byte
	var err error
	if bytes.Equal(account.CodeHash, emptyCodeHash) && (account.Root == emptyRoot || account.Root == common.Hash{}) {
		if account.Balance.Sign() == 0 && account.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount ExtAccount
			extAccount.Nonce = account.Nonce
			extAccount.Balance = account.Balance
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return err
			}
		}
	} else {
		data, err = rlp.EncodeToBytes(account)
		if err != nil {
			return err
		}
	}
	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	if err = dsw.tds.db.Put(AccountsBucket, addrHash[:], data); err != nil {
		return err
	}
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], data, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) DeleteAccount(address common.Address) error {
	addrHash, err := tsw.tds.HashAddress(address, false /*save*/)
	if err != err {
		return err
	}
	tsw.tds.accountUpdates[addrHash] = nil
	tsw.tds.deleted[addrHash] = struct{}{}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(address common.Address) error {
	addrHash, err := dsw.tds.HashAddress(address, false /*save*/)
	if err != nil {
		return err
	}
	if err := dsw.tds.db.Delete(AccountsBucket, addrHash[:]); err != nil {
		return err
	}
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], []byte{}, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return dsw.tds.db.Put(CodeBucket, codeHash[:], code)
}

func (tsw *TrieStateWriter) WriteAccountStorage(address common.Address, key, value *common.Hash) error {
	v := bytes.TrimLeft(value[:], "\x00")
	m, ok := tsw.tds.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.storageUpdates[address] = m
	}
	seckey, err :=tsw.tds.HashKey(*key, false /*save*/)
	if err != nil {
		return err
	}
	if len(v) > 0 {
		m[seckey] = common.CopyBytes(v)
	} else {
		m[seckey] = nil
	}
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(address common.Address, key, value *common.Hash) error {
	seckey, err := dsw.tds.HashKey(*key, true /*save*/)
	if err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	compositeKey := append(address[:], seckey[:]...)
	if len(v) == 0 {
		err = dsw.tds.db.Delete(StorageBucket, compositeKey)
	} else {
		err = dsw.tds.db.Put(StorageBucket, compositeKey, vv)
	}
	if err != nil {
		return err
	}
	return dsw.tds.db.PutS(StorageHistoryBucket, compositeKey, vv, dsw.tds.blockNr)
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
	GetKey(trie.DatabaseReader, []byte) []byte // TODO(fjl): remove this when SecureTrie is removed
	PrintTrie()
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
	return trie.NewSecure(root, AccountsBucket, nil, false)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *cachingDB) OpenStorageTrie(addrHash, root common.Hash) (Trie, error) {
	return trie.NewSecure(root, StorageBucket, addrHash[:], true)
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
	return len(code), err
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *cachingDB) TrieDB() ethdb.Database {
	return db.db
}
