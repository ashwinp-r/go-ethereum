package main

import (
	"bytes"
	"math/big"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/params"
)

// Implements ethdb.Database
type FakeDatabase struct {
	genesisHeaderRLP []byte
	genesisBodyRLP []byte
	genesisDiffRLP []byte
}

var (
	trieSyncKey   = []byte("TrieSync")
	headerPrefix        = []byte("h")
	headBlockKey  = []byte("LastBlock")
	blockHashPrefix     = []byte("H")
	bodyPrefix          = []byte("b")
	headHeaderKey = []byte("LastHeader")
	headFastKey   = []byte("LastFast")
	tdSuffix            = []byte("t")
)

func NewFakeDatabase() (*FakeDatabase, error) {
	fd := FakeDatabase{}
	genesisBlock, _, _, err := core.DefaultGenesisBlock().ToBlock(nil)
	if err != nil {
		panic(fmt.Sprintf("Error creating genesis %v", err))
	}
	fd.genesisHeaderRLP, err = rlp.EncodeToBytes(genesisBlock.Header())
	if err != nil {
		return nil, err
	}
	fd.genesisBodyRLP, err = rlp.EncodeToBytes(genesisBlock.Body())
	if err != nil {
		return nil, err
	}
	fd.genesisDiffRLP, err = rlp.EncodeToBytes(genesisBlock.Header().Difficulty)
	if err != nil {
		return nil, err
	}
	return &fd, nil
}

func (fd *FakeDatabase) Get(bucket, key []byte) ([]byte, error) {
	if bytes.Equal(bucket, trieSyncKey) && bytes.Equal(key, trieSyncKey) {
		return big.NewInt(0).Bytes(), nil
	}
	if bytes.Equal(bucket, headerPrefix) && bytes.Equal(key, []byte("\x00\x00\x00\x00\x00\x00\x00\x00n")){
		return params.MainnetGenesisHash[:], nil
	}
	if bytes.Equal(bucket, headerPrefix) &&
		len(key) == 40 &&
		bytes.Equal(key[:8], []byte("\x00\x00\x00\x00\x00\x00\x00\x00")) &&
		bytes.Equal(key[8:], params.MainnetGenesisHash[:]) {

		return fd.genesisHeaderRLP, nil
	}
	if bytes.Equal(bucket, headBlockKey) && bytes.Equal(key, headBlockKey) {
		return params.MainnetGenesisHash[:], nil
	}
	if bytes.Equal(bucket, blockHashPrefix) && bytes.Equal(key, params.MainnetGenesisHash[:]) {
		return []byte("\x00\x00\x00\x00\x00\x00\x00\x00"), nil
	}
	if bytes.Equal(bucket, bodyPrefix) &&
		len(key) == 40 &&
		bytes.Equal(key[:8], []byte("\x00\x00\x00\x00\x00\x00\x00\x00")) &&
		bytes.Equal(key[8:], params.MainnetGenesisHash[:]) {

		return fd.genesisBodyRLP, nil
	}
	if bytes.Equal(bucket, headHeaderKey) && bytes.Equal(key, headHeaderKey) {
		return params.MainnetGenesisHash[:], nil
	}
	if bytes.Equal(bucket, headFastKey) && bytes.Equal(key, headFastKey) {
		return params.MainnetGenesisHash[:], nil
	}
	if bytes.Equal(bucket, headerPrefix) &&
		len(key) == 41 &&
		bytes.Equal(key[:8], []byte("\x00\x00\x00\x00\x00\x00\x00\x00")) &&
		bytes.Equal(key[8:40], params.MainnetGenesisHash[:]) &&
		bytes.Equal(key[40:], tdSuffix) {

		return fd.genesisDiffRLP, nil
	}
	if bytes.Equal(bucket, blockHashPrefix) {
		if _, ok := core.BadHashes[common.BytesToHash(key)]; ok {
			return nil, nil
		}
	}
	panic(fmt.Sprintf("Get %x %x", bucket, key))
}

func (fd *FakeDatabase) GetAsOf(bucket, key []byte, timestamp uint64) ([]byte, error) {
	panic("")
}

func (fd *FakeDatabase) Has(bucket, key []byte) (bool, error) {
	panic("")
}

func (fd *FakeDatabase) Walk(bucket, startkey []byte, fixedbits uint, walker ethdb.WalkerFunc) error {
	panic("")
}

func (fd *FakeDatabase) WalkAsOf(bucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	panic("")
}

func (fd *FakeDatabase) MultiWalkAsOf(bucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	panic("")
}

func (fd *FakeDatabase) GetHash(index uint32) []byte {
	panic("")
}

func (fd *FakeDatabase) Put(bucket, key, value []byte) error {
	fmt.Printf("Wrote %s %x %x\n", string(bucket), key, value)
	return nil
}

func (fd *FakeDatabase) PutS(bucket, key, value []byte, timestamp uint64) error {
	panic("")
}

func (fd *FakeDatabase) DeleteTimestamp(timestamp uint64) error {
	panic("")
}

func (fd *FakeDatabase) PutHash(index uint32, hash []byte) {
	panic("")
}

func (fd *FakeDatabase) Delete(bucket, key []byte) error {
	panic("")
}

func (fd *FakeDatabase) MultiPut(tuples ...[]byte) error {
	panic("")
}

func (fd *FakeDatabase) RewindData(timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	panic("")
}

func (fd *FakeDatabase) Close() {
	panic("")
}

func (fd *FakeDatabase) NewBatch() ethdb.Mutation {
	return fd
}

func (fd *FakeDatabase) Size() int {
	panic("")
}

func (fd *FakeDatabase) Commit() error {
	panic("")
}

func (fd *FakeDatabase) Rollback() {
	panic("")
}

func (fd *FakeDatabase) Keys() [][]byte {
	panic("")
}

func (fd *FakeDatabase) BatchSize() int {
	panic("")
}
