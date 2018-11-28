package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"time"
	"syscall"

	"github.com/boltdb/bolt"
	lru "github.com/hashicorp/golang-lru"

	"github.com/ethereum/go-ethereum/avl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	cpuprofile = flag.String("cpu-profile", "", "write cpu profile `file`")
	blockchain = flag.String("blockchain", "data/blockchain", "file containing blocks to load")
	hashlen    = flag.Int("hashlen", 32, "size of the hashes for inter-page references")
	datadir   = flag.String("datadir", ".", "directory for data files")
	load       = flag.Bool("load", false, "load blocks into pages")
	spacescan  = flag.Bool("spacescan", false, "perform space scan")
)

var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
var emptyCodeHash = crypto.Keccak256(nil)

// ChainContext implements Ethereum's core.ChainContext and consensus.Engine
// interfaces. It is needed in order to apply and process Ethereum
// transactions. There should only be a single implementation in Ethermint. For
// the purposes of Ethermint, it should be support retrieving headers and
// consensus parameters from  the current blockchain to be used during
// transaction processing.
//
// NOTE: Ethermint will distribute the fees out to validators, so the structure
// and functionality of this is a WIP and subject to change.
type ChainContext struct {
	Coinbase        common.Address
	headersByNumber map[uint64]*types.Header
}

func NewChainContext() *ChainContext {
	return &ChainContext{
		headersByNumber: make(map[uint64]*types.Header),
	}
}

// Engine implements Ethereum's core.ChainContext interface. As a ChainContext
// implements the consensus.Engine interface, it is simply returned.
func (cc *ChainContext) Engine() consensus.Engine {
	return cc
}

// SetHeader implements Ethereum's core.ChainContext interface. It sets the
// header for the given block number.
func (cc *ChainContext) SetHeader(number uint64, header *types.Header) {
	cc.headersByNumber[number] = header
}

// GetHeader implements Ethereum's core.ChainContext interface.
//
// TODO: The Cosmos SDK supports retreiving such information in contexts and
// multi-store, so this will be need to be integrated.
func (cc *ChainContext) GetHeader(_ common.Hash, number uint64) *types.Header {
	if header, ok := cc.headersByNumber[number]; ok {
		return header
	}

	return nil
}

// Author implements Ethereum's consensus.Engine interface. It is responsible
// for returned the address of the validtor to receive any fees. This function
// is only invoked if the given author in the ApplyTransaction call is nil.
//
// NOTE: Ethermint will distribute the fees out to validators, so the structure
// and functionality of this is a WIP and subject to change.
func (cc *ChainContext) Author(_ *types.Header) (common.Address, error) {
	return cc.Coinbase, nil
}

// APIs implements Ethereum's consensus.Engine interface. It currently performs
// a no-op.
//
// TODO: Do we need to support such RPC APIs? This will tie into a bigger
// discussion on if we want to support web3.
func (cc *ChainContext) APIs(_ consensus.ChainReader) []rpc.API {
	return nil
}

// CalcDifficulty implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
func (cc *ChainContext) CalcDifficulty(_ consensus.ChainReader, _ uint64, _ *types.Header) *big.Int {
	return nil
}

// Finalize implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Finalize(
	_ consensus.ChainReader, _ *types.Header, _ *state.StateDB,
	_ []*types.Transaction, _ []*types.Header, _ []*types.Receipt,
) (*types.Block, error) {
	return nil, nil
}

// Prepare implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Prepare(_ consensus.ChainReader, _ *types.Header) error {
	return nil
}

// Seal implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Seal(_ consensus.ChainReader, _ *types.Block, _ chan<- *types.Block, _ <-chan struct{}) error {
	return nil
}

// SealHash implements Ethereum's consensus.Engine interface. It returns the
// hash of a block prior to it being sealed.
func (cc *ChainContext) SealHash(header *types.Header) common.Hash {
	return common.Hash{}
}

// VerifyHeader implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifyHeader(_ consensus.ChainReader, _ *types.Header, _ bool) error {
	return nil
}

// VerifyHeaders implements Ethereum's consensus.Engine interface. It
// currently performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifyHeaders(_ consensus.ChainReader, _ []*types.Header, _ []bool) (chan<- struct{}, <-chan error) {
	return nil, nil
}

// VerifySeal implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifySeal(_ consensus.ChainReader, _ *types.Header) error {
	return nil
}

// VerifyUncles implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
func (cc *ChainContext) VerifyUncles(_ consensus.ChainReader, _ *types.Block) error {
	return nil
}

// Close implements Ethereum's consensus.Engine interface. It terminates any
// background threads maintained by the consensus engine. It currently performs
// a no-op.
func (cc *ChainContext) Close() error {
	return nil
}

type MorusDb struct {
	db *avl.Avl1
	codeDb *bolt.DB
	preDb  *bolt.DB
	codeCache        *lru.Cache
	codeSizeCache    *lru.Cache
	currentStateDb   *state.StateDB
}

func NewMorusDb(datadir string, hashlen int) *MorusDb {
	db := avl.NewAvl1()
	db.SetHashLength(uint32(hashlen))
	pagefile := filepath.Join(datadir, "pages")
	valuefile := filepath.Join(datadir, "values")
	verfile := filepath.Join(datadir, "versions")
	codefile := filepath.Join(datadir, "codes")
	prefile := filepath.Join(datadir, "preimages")
	db.UseFiles(pagefile, valuefile, verfile, false)
	codeDb, err := bolt.Open(codefile, 0600, &bolt.Options{})
	if err != nil {
		panic(err)
	}
	preDb, err := bolt.Open(prefile, 0600, &bolt.Options{})
	if err != nil {
		panic(err)
	}
	csc, err := lru.New(100000)
	if err != nil {
		panic(err)
	}
	cc, err := lru.New(10000)
	if err != nil {
		panic(err)
	}
	return &MorusDb{db: db, codeDb: codeDb, preDb: preDb, codeCache: cc, codeSizeCache: csc}
}

var preimageBucket = []byte("P")

func (md *MorusDb) CommitPreimages(statedb *state.StateDB) {
	if err := md.preDb.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(preimageBucket)
		if err != nil {
			return err
		}
		pi := statedb.Preimages()
		for hash, preimage := range pi {
			if err := bucket.Put(hash[:], preimage); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func (md *MorusDb) LatestVersion() int64 {
	return int64(md.db.CurrentVersion())
}

func (md *MorusDb) Commit() uint64 {
	return md.db.Commit()
}

func (md *MorusDb) Close() {
	md.db.Close()
	md.codeDb.Close()
	md.preDb.Close()
}

func (md *MorusDb) PrintStats() {
	md.db.PrintStats()
}

func accountToEncoding(account *state.Account) ([]byte, error) {
	var data []byte
	var err error
	if (account.CodeHash == nil || bytes.Equal(account.CodeHash, emptyCodeHash)) && (account.Root == emptyRoot || account.Root == common.Hash{}) {
		if (account.Balance == nil || account.Balance.Sign() == 0) && account.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount state.ExtAccount
			extAccount.Nonce = account.Nonce
			extAccount.Balance = account.Balance
			if extAccount.Balance == nil {
				extAccount.Balance = new(big.Int)
			}
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return nil, err
			}
		}
	} else {
		a := *account
		if a.Balance == nil {
			a.Balance = new(big.Int)
		}
		if a.CodeHash == nil {
			a.CodeHash = emptyCodeHash
		}
		if a.Root == (common.Hash{}) {
			a.Root = emptyRoot
		}
		data, err = rlp.EncodeToBytes(a)
		if err != nil {
			return nil, err
		}
	}
	return data, err
}

func encodingToAccount(enc []byte) (*state.Account, error) {
	if enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data state.Account
	// Kind of hacky
	if len(enc) == 1 {
		data.Balance = new(big.Int)
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else if len(enc) < 60 {
		var extData state.ExtAccount
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

var codeBucket = []byte("C")
const (
	PREFIX_ACCOUNT = byte(0)
	PREFIX_STORAGE = byte(1)
	PREFIX_ONE_WORD = byte(2)
	PREFIX_TWO_WORDS = byte(3)
)

func (md *MorusDb) createAccountKey(address *common.Address) []byte {
	k := make([]byte, 21)
	k[0] = PREFIX_ACCOUNT
	copy(k[1:], (*address)[:])
	return k
}

func (md *MorusDb) createStorageKey(address *common.Address, key *common.Hash) []byte {
	/*
	var preimage []byte
	if err := md.preDb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(preimageBucket)
		if bucket == nil {
			return nil
		}
		v := bucket.Get((*key)[:])
		if len(v) > 0 {
			preimage = common.CopyBytes(v)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	if preimage != nil {
		if len(preimage) == 32 {
			v := bytes.TrimLeft(preimage, "\x00")
			k := make([]byte, 21 + len(v))
			k[0] = PREFIX_ONE_WORD
			copy(k[1:], (*address)[:])
			copy(k[21:], v)
			return k
		} else if len(preimage) == 64 {
			v1 := bytes.TrimLeft(preimage[:32], "\x00")
			v2 := bytes.TrimLeft(preimage[32:], "\x00")
			k := make([]byte, 22 + len(v1) + len(v2))
			k[0] = PREFIX_TWO_WORDS
			copy(k[1:], (*address)[:])
			k[21] = byte(len(v1))
			copy(k[22:], v1)
			copy(k[22+len(v1):], v2)
			return k
		}
	}
	*/
	k := make([]byte, 53)
	k[0] = PREFIX_STORAGE
	copy(k[1:], (*address)[:])
	copy(k[21:], (*key)[:])
	return k
}

func (md *MorusDb) ReadAccountData(address common.Address) (*state.Account, error) {
	enc, found := md.db.Get(md.createAccountKey(&address))
	if !found || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return encodingToAccount(enc)
}

func (md *MorusDb) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	enc, found := md.db.Get(md.createStorageKey(&address, key))
	if !found || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (md *MorusDb) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if cached, ok := md.codeCache.Get(codeHash); ok {
		return cached.([]byte), nil
	}
	var code []byte
	if err := md.codeDb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(codeBucket)
		if bucket == nil {
			return nil
		}
		v := bucket.Get(codeHash[:])
		if len(v) > 0 {
			code = common.CopyBytes(v)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if code != nil {
		md.codeSizeCache.Add(codeHash, len(code))
		md.codeCache.Add(codeHash, code)
	}
	return code, nil
}

func (md *MorusDb) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	if cached, ok := md.codeSizeCache.Get(codeHash); ok {
		return cached.(int), nil
	}
	code, err := md.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (md *MorusDb) UpdateAccountData(address common.Address, original, account *state.Account) error {
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	md.db.Insert(md.createAccountKey(&address), data)
	return nil
}

func (md *MorusDb) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	if err := md.codeDb.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(codeBucket)
		if err != nil {
			return err
		}
		return bucket.Put(codeHash[:], code)
	}); err != nil {
		return err
	}
	return nil
}

func (md *MorusDb) DeleteAccount(address common.Address, original *state.Account) error {
	md.db.Delete(md.createAccountKey(&address))
	return nil
}

func (md *MorusDb) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	if len(vv) > 0 {
		md.db.Insert(md.createStorageKey(&address, key), vv)
	} else {
		md.db.Delete(md.createStorageKey(&address, key))
	}
	return nil
}

// Some weird constants to avoid constant memory allocs for them.
var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

// accumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func accumulateRewards(config *params.ChainConfig, state *state.StateDB, header *types.Header, uncles []*types.Header) {
	// select the correct block reward based on chain progression
	blockReward := ethash.FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ethash.ByzantiumBlockReward
	}

	// accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)

	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		state.AddBalance(uncle.Coinbase, r)
		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}

	state.AddBalance(header.Coinbase, reward)
}

func main() {
	flag.Parse()
	noopWriter := state.NewNoopWriter()
	morus := NewMorusDb(*datadir, *hashlen)
	if morus.LatestVersion() == 0 {
		statedb := state.New(morus)
		genBlock := core.DefaultGenesisBlock()
		for addr, account := range genBlock.Alloc {
			statedb.AddBalance(addr, account.Balance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)

			for key, value := range account.Storage {
				statedb.SetState(addr, key, value)
			}
		}
		if err := statedb.Commit(false, morus); err != nil {
			panic(err)
		}
		morus.CommitPreimages(statedb)
		cp := morus.Commit()

		fmt.Printf("Committed pages for genesis state: %d\n", cp)
	}
	// file with blockchain data exported from geth by using "geth exportdb"
	// command.
	input, err := os.Open(*blockchain)
	if err != nil {
		panic(err)
	}
	defer input.Close()

	// ethereum mainnet config
	chainConfig := params.MainnetChainConfig

	// create RLP stream for exported blocks
	stream := rlp.NewStream(input, 0)

	var block types.Block

	var prevRoot common.Hash
	binary.BigEndian.PutUint64(prevRoot[:8], uint64(morus.LatestVersion()))

	chainContext := NewChainContext()
	vmConfig := vm.Config{EnablePreimageRecording: true}

	startTime := time.Now()
	interrupt := false

	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	var lastSkipped uint64
	var cpRun uint64
	for !interrupt {
		if err = stream.Decode(&block); err == io.EOF {
			err = nil
			break
		} else if err != nil {
			panic(fmt.Errorf("failed to decode at block %d: %s", block.NumberU64(), err))
		}

		// don't import blocks already imported
		if block.NumberU64() < uint64(morus.LatestVersion()) {
			lastSkipped = block.NumberU64()
			continue
		}

		if lastSkipped > 0 {
			fmt.Printf("skipped blocks up to %d\n", lastSkipped)
			lastSkipped = 0
		}

		header := block.Header()
		chainContext.Coinbase = header.Coinbase
		chainContext.SetHeader(block.NumberU64(), header)

		statedb := state.New(morus)

		var (
			receipts types.Receipts
			usedGas  = new(uint64)
			allLogs  []*types.Log
			gp       = new(core.GasPool).AddGas(block.GasLimit())
		)

		if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
			misc.ApplyDAOHardFork(statedb)
		}

		for i, tx := range block.Transactions() {
			statedb.Prepare(tx.Hash(), block.Hash(), i)

			receipt, _, err := core.ApplyTransaction(chainConfig, chainContext, nil, gp, statedb, noopWriter, header, tx, usedGas, vmConfig)

			if err != nil {
				panic(fmt.Errorf("at block %d, tx %x: %v", block.NumberU64(), tx.Hash(), err))
			}

			receipts = append(receipts, receipt)
			allLogs = append(allLogs, receipt.Logs...)
		}

		// apply mining rewards to the geth stateDB

		accumulateRewards(chainConfig, statedb, header, block.Uncles())

		// commit block in geth
		err = statedb.Commit(chainConfig.IsEIP158(block.Number()), morus)
		if err != nil {
			panic(fmt.Errorf("at block %d: %v", block.NumberU64(), err))
		}

		// commit block in Ethermint
		morus.CommitPreimages(statedb)
		cp := morus.Commit()
		cpRun += cp

		if (block.NumberU64() % 10000) == 0 {
			fmt.Printf("processed %d blocks, time so far: %v\n", block.NumberU64(), time.Since(startTime))
			fmt.Printf("committed pages: %d, Mb %.3f\n", cpRun, float64(cpRun)*float64(avl.PageSize)/1024.0/1024.0)
			morus.PrintStats()
			cpRun = 0
		}

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	morus.Close()

	fmt.Printf("processed %d blocks\n", block.NumberU64())
}