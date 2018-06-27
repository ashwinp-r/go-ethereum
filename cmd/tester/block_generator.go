package main

import (
    "crypto/ecdsa"
    "fmt"
    "math/big"
    "os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

type BlockGenerator struct {
	genesisBlock *types.Block
	coinbaseKey *ecdsa.PrivateKey
	blockOffsetByHash map[common.Hash]uint64
	blockOffsetByNumber map[uint64]uint64
	headersByHash map[common.Hash]*types.Header
	headersByNumber map[uint64]*types.Header
	lastBlock *types.Block
	totalDifficulty *big.Int
}

func NewBlockGenerator(outputFile string, initialHeight int) (*BlockGenerator, error) {
	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	db := ethdb.NewMemDatabase()
	genesisBlock, _, tds, err := core.DefaultGenesisBlock().ToBlock(db)
	if err != nil {
		return nil, err
	}
	parent := genesisBlock
	extra := []byte("BlockGenerator")
	coinbaseKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	coinbase := crypto.PubkeyToAddress(coinbaseKey.PublicKey)
	config := params.MainnetChainConfig
	var pos uint64
	td := new(big.Int)
	bg := &BlockGenerator{
		genesisBlock: genesisBlock,
		coinbaseKey: coinbaseKey,
		blockOffsetByHash: make(map[common.Hash]uint64),
		blockOffsetByNumber: make(map[uint64]uint64),
		headersByHash: make(map[common.Hash]*types.Header),
		headersByNumber: make(map[uint64]*types.Header),
	}
	for height := 1; height <= initialHeight; height++ {
		num := parent.Number()
		tstamp := parent.Time().Int64() + 15
		header := &types.Header{
			ParentHash: parent.Hash(),
			Number:     num.Add(num, common.Big1),
			GasLimit:   core.CalcGasLimit(parent),
			Extra:      extra,
			Time:       big.NewInt(tstamp),
			Coinbase: coinbase,
			Difficulty: ethash.CalcDifficulty(config, uint64(tstamp), parent.Header()),
		}
		tds.SetBlockNr(parent.NumberU64())
		statedb := state.New(tds)
		accumulateRewards(config, statedb, header, []*types.Header{})
		header.Root, err = tds.IntermediateRoot(statedb, config.IsEIP158(header.Number))
		if err != nil {
			return nil, err
		}
		err = statedb.Commit(config.IsEIP158(header.Number), tds.DbStateWriter())
		if err != nil {
			return nil, err
		}
		// Generate an empty block
		block := types.NewBlock(header, []*types.Transaction{}, []*types.Header{}, []*types.Receipt{})
		fmt.Printf("block hash for %d: %x\n", block.Number(), block.Hash())
		if buffer, err := rlp.EncodeToBytes(block); err != nil {
			return nil, err
		} else {
			output.Write(buffer)
			pos += uint64(len(buffer))
		}
		hash := header.Hash()
		bg.headersByHash[hash] = header
		bg.headersByNumber[block.NumberU64()] = header
		bg.blockOffsetByHash[hash] = pos
		bg.blockOffsetByNumber[block.NumberU64()] = pos
		td = new(big.Int).Add(td, block.Difficulty())
		parent = block
	}
	bg.lastBlock = parent
	bg.totalDifficulty = td
	return bg, nil
}

// Some weird constants to avoid constant memory allocs for them.
var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

// AccumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func accumulateRewards(config *params.ChainConfig, state *state.StateDB, header *types.Header, uncles []*types.Header) {
	// Select the correct block reward based on chain progression
	blockReward := ethash.FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ethash.ByzantiumBlockReward
	}
	// Accumulate the rewards for the miner and any included uncles
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
