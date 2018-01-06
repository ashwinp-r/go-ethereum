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

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"fmt"
	"math/big"
	"sort"
	"sync"
	"encoding/hex"
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/crypto/sha3"
)

type revision struct {
	id           int
	journalIndex int
}

// StateDBs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
// * Contracts
// * Accounts
type StateDB struct {
	db   Database
	trie Trie

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects      map[common.Address]*stateObject
	stateObjectsDirty map[common.Address]struct{}

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by StateDB.Commit.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund *big.Int

	thash, bhash common.Hash
	txIndex      int
	logs         map[common.Hash][]*types.Log
	logSize      uint

	preimages map[common.Hash][]byte

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        journal
	validRevisions []revision
	nextRevisionId int

	lock sync.Mutex

	prefetchCh chan PrefetchRequest
}

type PrefetchRequest struct {
	db trie.Database
	triePrefix []byte
	key []byte
	prefixEnd int
	blockNr uint32
	responsePtr *trie.PrefetchResponse
}

// Create a new state from a given trie
func New(root common.Hash, db Database, blockNr uint32) (*StateDB, error) {
	tr, err := db.OpenTrie(root, blockNr)
	if err != nil {
		return nil, err
	}
	prefetchCh := make(chan PrefetchRequest, 1024)
	prefetch := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Printf("internal error: %v\n", p)
			}
		}()
		for request := range prefetchCh {
			enc, err := trie.ReadResolve(request.db, request.triePrefix, request.key[:request.prefixEnd], request.blockNr)
			response := request.responsePtr
			response.Mu.Lock()
			response.Value, response.Err = enc, err
			response.Ready = true
			response.C.Signal()
			response.Mu.Unlock()
		}
	}
	for i := 0; i < 16; i++ {
		go prefetch()
	}
	return &StateDB{
		db:                db,
		trie:              tr,
		stateObjects:      make(map[common.Address]*stateObject),
		stateObjectsDirty: make(map[common.Address]struct{}),
		refund:            new(big.Int),
		logs:              make(map[common.Hash][]*types.Log),
		preimages:         make(map[common.Hash][]byte),
		prefetchCh:        prefetchCh,
	}, nil
}

const PrefetchDepth int = 5

func (self *StateDB) RequestPrefetch(triePrefix, key []byte, prefixEnd int, blockNr uint32, respMap map[string]*trie.PrefetchResponse) {
	for i := prefixEnd; i < prefixEnd + PrefetchDepth && i <= len(key); i++ {
		keyStr := string(key[:i])
		if _, ok := respMap[keyStr]; !ok {
			var response trie.PrefetchResponse
			response.C = sync.NewCond(&response.Mu)
			respMap[keyStr] = &response
			self.prefetchCh <- PrefetchRequest{db: self.db.TrieDb(), triePrefix: triePrefix, key: key, prefixEnd: i, responsePtr: &response, blockNr: blockNr}
		}
	}
}

// setError remembers the first non-nil error it is called with.
func (self *StateDB) setError(err error) {
	if self.dbErr == nil {
		self.dbErr = err
	}
}

func (self *StateDB) Error() error {
	return self.dbErr
}

// Reset clears out all emphemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (self *StateDB) Reset(root common.Hash, blockNr uint32) error {
	tr, err := self.db.OpenTrie(root, blockNr)
	if err != nil {
		return err
	}
	self.trie = tr
	self.stateObjects = make(map[common.Address]*stateObject)
	self.stateObjectsDirty = make(map[common.Address]struct{})
	self.thash = common.Hash{}
	self.bhash = common.Hash{}
	self.txIndex = 0
	self.logs = make(map[common.Hash][]*types.Log)
	self.logSize = 0
	self.preimages = make(map[common.Hash][]byte)
	self.clearJournalAndRefund()
	return nil
}

func (self *StateDB) AddLog(log *types.Log) {
	self.journal = append(self.journal, addLogChange{txhash: self.thash})

	log.TxHash = self.thash
	log.BlockHash = self.bhash
	log.TxIndex = uint(self.txIndex)
	log.Index = self.logSize
	self.logs[self.thash] = append(self.logs[self.thash], log)
	self.logSize++
}

func (self *StateDB) GetLogs(hash common.Hash) []*types.Log {
	return self.logs[hash]
}

func (self *StateDB) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range self.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (self *StateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := self.preimages[hash]; !ok {
		self.journal = append(self.journal, addPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		self.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (self *StateDB) Preimages() map[common.Hash][]byte {
	return self.preimages
}

func (self *StateDB) AddRefund(gas *big.Int) {
	self.journal = append(self.journal, refundChange{prev: new(big.Int).Set(self.refund)})
	self.refund.Add(self.refund, gas)
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (self *StateDB) Exist(addr common.Address, blockNr uint32) bool {
	//fmt.Printf("Checking existence of %s\n", hex.EncodeToString(addr[:]))
	return self.getStateObject(addr, blockNr) != nil
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (self *StateDB) Empty(addr common.Address, blockNr uint32) bool {
	so := self.getStateObject(addr, blockNr)
	return so == nil || so.empty()
}

// Retrieve the balance from the given address or 0 if object not found
func (self *StateDB) GetBalance(addr common.Address, blockNr uint32) *big.Int {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.Big0
}

func (self *StateDB) GetNonce(addr common.Address, blockNr uint32) uint64 {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject != nil {
		return stateObject.Nonce()
	}

	return 0
}

func (self *StateDB) GetCode(addr common.Address, blockNr uint32) []byte {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject != nil {
		return stateObject.Code(self.db)
	}
	return nil
}

func (self *StateDB) GetCodeSize(addr common.Address, blockNr uint32) int {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject == nil {
		return 0
	}
	if stateObject.code != nil {
		return len(stateObject.code)
	}
	size, err := self.db.ContractCodeSize(stateObject.addrHash, common.BytesToHash(stateObject.CodeHash()))
	if err != nil {
		self.setError(err)
	}
	return size
}

func (self *StateDB) GetCodeHash(addr common.Address, blockNr uint32) common.Hash {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject == nil {
		return common.Hash{}
	}
	return common.BytesToHash(stateObject.CodeHash())
}

func (self *StateDB) GetState(a common.Address, b common.Hash, blockNr uint32) common.Hash {
	stateObject := self.getStateObject(a, blockNr)
	if stateObject != nil {
		return stateObject.GetState(self.db, b, blockNr)
	}
	return common.Hash{}
}

// StorageTrie returns the storage trie of an account.
// The return value is a copy and is nil for non-existent accounts.
func (self *StateDB) StorageTrie(a common.Address, blockNr uint32) Trie {
	stateObject := self.getStateObject(a, blockNr)
	if stateObject == nil {
		return nil
	}
	cpy := stateObject.deepCopy(self, nil)
	return cpy.updateTrie(self.db, nil, blockNr, 0, nil)
}

func (self *StateDB) HasSuicided(addr common.Address, blockNr uint32) bool {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject != nil {
		return stateObject.suicided
	}
	return false
}

/*
 * SETTERS
 */

// AddBalance adds amount to the account associated with addr
func (self *StateDB) AddBalance(addr common.Address, amount *big.Int, blockNr uint32) {
	stateObject := self.GetOrNewStateObject(addr, blockNr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr
func (self *StateDB) SubBalance(addr common.Address, amount *big.Int, blockNr uint32) {
	stateObject := self.GetOrNewStateObject(addr, blockNr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
	}
}

func (self *StateDB) SetBalance(addr common.Address, amount *big.Int, blockNr uint32) {
	stateObject := self.GetOrNewStateObject(addr, blockNr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (self *StateDB) SetNonce(addr common.Address, nonce uint64, blockNr uint32) {
	stateObject := self.GetOrNewStateObject(addr, blockNr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (self *StateDB) SetCode(addr common.Address, code []byte, blockNr uint32) {
	stateObject := self.GetOrNewStateObject(addr, blockNr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (self *StateDB) SetState(addr common.Address, key common.Hash, value common.Hash, blockNr uint32) {
	stateObject := self.GetOrNewStateObject(addr, blockNr)
	if stateObject != nil {
		stateObject.SetState(self.db, key, value, blockNr)
	}
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (self *StateDB) Suicide(addr common.Address, blockNr uint32) bool {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject == nil {
		return false
	}
	self.journal = append(self.journal, suicideChange{
		account:     &addr,
		prev:        stateObject.suicided,
		prevbalance: new(big.Int).Set(stateObject.Balance()),
	})
	stateObject.markSuicided()
	stateObject.data.Balance = new(big.Int)

	return true
}

//
// Setting, updating & deleting state object methods
//

// updateStateObject writes the given object to the trie.
func (self *StateDB) updateStateObject(stateObject *stateObject, blockNr uint32, respMap map[string]*trie.PrefetchResponse) {
	addr := stateObject.Address()
	data, err := rlp.EncodeToBytes(stateObject)
	if err != nil {
		panic(fmt.Errorf("can't encode object at %x: %v", addr[:], err))
	}
	//fmt.Printf("TryUpdate %s\n", hex.EncodeToString(addr[:]))
	self.setError(self.trie.TryUpdate(addr[:], data, blockNr, respMap))
}

// deleteStateObject removes the given object from the state trie.
func (self *StateDB) deleteStateObject(stateObject *stateObject, blockNr uint32, respMap map[string]*trie.PrefetchResponse) {
	stateObject.deleted = true
	addr := stateObject.Address()
	self.setError(self.trie.TryDelete(addr[:], blockNr, respMap))
}

// Retrieve a state object given my the address. Returns nil if not found.
func (self *StateDB) getStateObject(addr common.Address, blockNr uint32) (stateObject *stateObject) {
	// Prefer 'live' objects.
	if obj := self.stateObjects[addr]; obj != nil {
		if obj.deleted {
			return nil
		}
		return obj
	}

	// Load the object from the database.
	enc, err := self.trie.TryGet(addr[:], blockNr)
	if len(enc) == 0 {
		self.setError(err)
		return nil
	}
	var data Account
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		log.Error("Failed to decode state object", "addr", addr.Hex(), "err", err, "enc", hex.EncodeToString(enc))
		return nil
	}
	// Insert into the live set.
	obj := newObject(self, addr, data, self.MarkStateObjectDirty)
	self.setStateObject(obj)
	return obj
}

func (self *StateDB) setStateObject(object *stateObject) {
	self.stateObjects[object.Address()] = object
}

// Retrieve a state object or create a new state object if nil
func (self *StateDB) GetOrNewStateObject(addr common.Address, blockNr uint32) *stateObject {
	stateObject := self.getStateObject(addr, blockNr)
	if stateObject == nil || stateObject.deleted {
		stateObject, _ = self.createObject(addr, blockNr)
	}
	return stateObject
}

// MarkStateObjectDirty adds the specified object to the dirty map to avoid costly
// state object cache iteration to find a handful of modified ones.
func (self *StateDB) MarkStateObjectDirty(addr common.Address) {
	self.stateObjectsDirty[addr] = struct{}{}
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.
func (self *StateDB) createObject(addr common.Address, blockNr uint32) (newobj, prev *stateObject) {
	prev = self.getStateObject(addr, blockNr)
	newobj = newObject(self, addr, Account{}, self.MarkStateObjectDirty)
	newobj.setNonce(0) // sets the object to dirty
	if prev == nil {
		self.journal = append(self.journal, createObjectChange{account: &addr})
	} else {
		self.journal = append(self.journal, resetObjectChange{prev: prev})
	}
	self.setStateObject(newobj)
	return newobj, prev
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//   1. sends funds to sha(account ++ (nonce + 1))
//   2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
func (self *StateDB) CreateAccount(addr common.Address, blockNr uint32) {
	new, prev := self.createObject(addr, blockNr)
	if prev != nil {
		new.setBalance(prev.data.Balance)
	}
}

func (db *StateDB) ForEachStorage(addr common.Address, cb func(key, value common.Hash) bool, blockNr uint32) {
	so := db.getStateObject(addr, blockNr)
	if so == nil {
		return
	}

	// When iterating over the storage check the cache first
	for h, value := range so.cachedStorage {
		cb(h, value)
	}

	it := trie.NewIterator(so.getTrie(db.db, blockNr).NodeIterator(nil, blockNr))
	for it.Next() {
		// ignore cached values
		key := common.BytesToHash(db.trie.GetKey(it.Key))
		if _, ok := so.cachedStorage[key]; !ok {
			cb(key, common.BytesToHash(it.Value))
		}
	}
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (self *StateDB) Copy() *StateDB {
	self.lock.Lock()
	defer self.lock.Unlock()

	// Copy all the basic fields, initialize the memory ones
	state := &StateDB{
		db:                self.db,
		trie:              self.db.CopyTrie(self.trie),
		stateObjects:      make(map[common.Address]*stateObject, len(self.stateObjectsDirty)),
		stateObjectsDirty: make(map[common.Address]struct{}, len(self.stateObjectsDirty)),
		refund:            new(big.Int).Set(self.refund),
		logs:              make(map[common.Hash][]*types.Log, len(self.logs)),
		logSize:           self.logSize,
		preimages:         make(map[common.Hash][]byte),
	}
	// Copy the dirty states, logs, and preimages
	for addr := range self.stateObjectsDirty {
		state.stateObjects[addr] = self.stateObjects[addr].deepCopy(state, state.MarkStateObjectDirty)
		state.stateObjectsDirty[addr] = struct{}{}
	}
	for hash, logs := range self.logs {
		state.logs[hash] = make([]*types.Log, len(logs))
		copy(state.logs[hash], logs)
	}
	for hash, preimage := range self.preimages {
		state.preimages[hash] = preimage
	}
	return state
}

// Snapshot returns an identifier for the current revision of the state.
func (self *StateDB) Snapshot() int {
	id := self.nextRevisionId
	self.nextRevisionId++
	self.validRevisions = append(self.validRevisions, revision{id, len(self.journal)})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (self *StateDB) RevertToSnapshot(revid int, blockNr uint32) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(self.validRevisions), func(i int) bool {
		return self.validRevisions[i].id >= revid
	})
	if idx == len(self.validRevisions) || self.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := self.validRevisions[idx].journalIndex

	// Replay the journal to undo changes.
	for i := len(self.journal) - 1; i >= snapshot; i-- {
		self.journal[i].undo(self, blockNr)
	}
	self.journal = self.journal[:snapshot]

	// Remove invalidated snapshots from the stack.
	self.validRevisions = self.validRevisions[:idx]
}

// GetRefund returns the current value of the refund counter.
// The return value must not be modified by the caller and will become
// invalid at the next call to AddRefund.
func (self *StateDB) GetRefund() *big.Int {
	return self.refund
}

func (s *StateDB) CachedPrefixes(deleteEmptyObjects bool, blockNr uint32) (map[string]*trie.PrefetchResponse, map[string]map[string]*trie.PrefetchResponse) {
	respMap := make(map[string]*trie.PrefetchResponse)
	storageRespMap := make(map[string]map[string]*trie.PrefetchResponse)
	for _, addr := range s.sortedDirtyAccounts() {
		k, pos := s.trie.CachedPrefixFor(addr[:])
		s.RequestPrefetch([]byte("AT"), k, pos, blockNr, respMap)
		stateObject, _ := s.stateObjects[addr]
		if !stateObject.suicided && (!deleteEmptyObjects || !stateObject.empty()) {
			storageRespMap[string(addr[:])] = stateObject.CachedPrefixes(blockNr)
		}
	}
	return respMap, storageRespMap
}

// Finalise finalises the state by removing the self destructed objects
// and clears the journal as well as the refunds.
func (s *StateDB) Finalise(deleteEmptyObjects bool, blockNr uint32) {
	//respMap, storageRespMap := s.CachedPrefixes(deleteEmptyObjects, blockNr)
	for _, addr := range s.sortedDirtyAccounts() {
		stateObject := s.stateObjects[addr]
		if stateObject.suicided || (deleteEmptyObjects && stateObject.empty()) {
			s.deleteStateObject(stateObject, blockNr, nil)
		} else {
			stateObject.updateRoot(s.db, blockNr, /*storageRespMap[string(addr[:])]*/nil)
			s.updateStateObject(stateObject, blockNr, nil)
		}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	s.clearJournalAndRefund()
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
func (s *StateDB) IntermediateRoot(deleteEmptyObjects bool, blockNr uint32) common.Hash {
	//fmt.Printf("\n\n\n")
	//s.trie.PrintTrie()
	s.Finalise(deleteEmptyObjects, blockNr)
	return s.trie.Hash()
}

// Prepare sets the current transaction hash and index and block hash which is
// used when the EVM emits new state logs.
func (self *StateDB) Prepare(thash, bhash common.Hash, ti int) {
	self.thash = thash
	self.bhash = bhash
	self.txIndex = ti
}

// DeleteSuicides flags the suicided objects for deletion so that it
// won't be referenced again when called / queried up on.
//
// DeleteSuicides should not be used for consensus related updates
// under any circumstances.
func (s *StateDB) DeleteSuicides() {
	// Reset refund so that any used-gas calculations can use this method.
	s.clearJournalAndRefund()

	for _, addr := range s.sortedDirtyAccounts() {
		stateObject := s.stateObjects[addr]

		// If the object has been removed by a suicide
		// flag the object as deleted.
		if stateObject.suicided {
			stateObject.deleted = true
		}
		delete(s.stateObjectsDirty, addr)
	}
}

func (s *StateDB) clearJournalAndRefund() {
	s.journal = nil
	s.validRevisions = s.validRevisions[:0]
	s.refund = new(big.Int)
}

func keybytesToHex(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

// CommitTo writes the state to the given database.
func (s *StateDB) CommitTo(dbw trie.DatabaseWriter, deleteEmptyObjects bool, blockNr uint32, writeBlockNr uint32) (root common.Hash, err error) {
	defer s.clearJournalAndRefund()
	//chanMap := s.CachedPrefixes(blockNr)
	suffix := make([]byte, 4)
	binary.BigEndian.PutUint32(suffix, writeBlockNr^0xffffffff - 1)	// Commit objects to the trie.
	for _, addr := range s.sortedAllAccounts() {
		stateObject := s.stateObjects[addr]
		_, isDirty := s.stateObjectsDirty[addr]
		switch {
		case stateObject.suicided || (isDirty && deleteEmptyObjects && stateObject.empty()):
			// If the object has been removed, don't bother syncing it
			// and just mark it for deletion in the trie.
			s.deleteStateObject(stateObject, blockNr, nil)
			sha := sha3.NewKeccak256()
			sha.Write(addr[:])
			seckey := sha.Sum(nil)
			compKey := trie.CompositeKey(keybytesToHex(seckey[:]), []byte("AT"), suffix)
			if err := dbw.Put(compKey, []byte{}); err != nil {
				return common.Hash{}, err
			}
		case isDirty:
			// Write any contract code associated with the state object
			if stateObject.code != nil && stateObject.dirtyCode {
				if err := dbw.Put(stateObject.CodeHash(), stateObject.code); err != nil {
					return common.Hash{}, err
				}
				stateObject.dirtyCode = false
			}
			// Write any storage changes in the state object to its storage trie.
			if err := stateObject.CommitTrie(s.db, dbw, blockNr, writeBlockNr); err != nil {
				return common.Hash{}, err
			}
			// Update the object in the main account trie.
			s.updateStateObject(stateObject, blockNr, nil)
		}
		delete(s.stateObjectsDirty, addr)
		if !isDirty {
			stateObject.idleAge++
			if stateObject.idleAge >= 128 {
				//fmt.Printf("Removing idle state object %s\n", addr.Hex())
				delete(s.stateObjects, addr)
			}
		}
	}
	// Write trie changes.
	root, err = s.trie.CommitTo(dbw, writeBlockNr)
	log.Debug("Trie cache stats after commit", "misses", trie.CacheMisses(), "unloads", trie.CacheUnloads())
	return root, err
}

func (s *StateDB) CleanForNextBlock() {
	s.logs = make(map[common.Hash][]*types.Log)
	s.logSize = 0
	s.clearJournalAndRefund()
	// Restore onDirty callbacks
	for _, stateObject := range s.stateObjects {
		stateObject.onDirty = s.MarkStateObjectDirty
	}
}
