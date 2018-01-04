package main

import (
	//"bytes"
	"fmt"
	//"encoding/json"
	//"io/ioutil"
	"flag"
	"runtime/pprof"
	"os"
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/params"
	//"github.com/ethereum/go-ethereum/rlp"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")

// computeTxEnv returns the execution environment of a certain transaction.
func computeTxEnv(bc *core.BlockChain, blockHash common.Hash, txIndex int) (core.Message, vm.Context, *state.StateDB, uint32, error) {
	// Create the parent state.
	block := bc.GetBlockByHash(blockHash)
	if block == nil {
		return nil, vm.Context{}, nil, 0, fmt.Errorf("block %x not found", blockHash)
	}
	parent := bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, vm.Context{}, nil, 0, fmt.Errorf("block parent %x not found", block.ParentHash())
	}
	statedb, err := bc.StateAt(parent.Root(), uint32(parent.NumberU64()))
	if err != nil {
		return nil, vm.Context{}, nil, uint32(parent.NumberU64()), err
	}
	txs := block.Transactions()

	// Recompute transactions up to the target index.
	signer := types.MakeSigner(params.MainnetChainConfig, block.Number())
	for idx, tx := range txs {
		// Assemble the transaction call message
		msg, _ := tx.AsMessage(signer)
		context := core.NewEVMContext(msg, block.Header(), bc, nil)
		if idx == txIndex {
			return msg, context, statedb, uint32(parent.NumberU64()), nil
		}

		vmenv := vm.NewEVM(context, statedb, params.MainnetChainConfig, vm.Config{})
		gp := new(core.GasPool).AddGas(tx.Gas())
		_, _, _, err := core.ApplyMessage(vmenv, msg, gp)
		if err != nil {
			return nil, vm.Context{}, nil, 0, fmt.Errorf("tx %x failed: %v", tx.Hash(), err)
		}
		statedb.DeleteSuicides()
	}
	return nil, vm.Context{}, nil, 0 , fmt.Errorf("tx index %d out of range for block %x", txIndex, blockHash)
}

func traceTx(bc *core.BlockChain, chainDb ethdb.Database, txHash common.Hash) (interface{}, error) {
	tracer := vm.NewStructLogger(nil)

	// Retrieve the tx from the chain and the containing block
	tx, blockHash, _, txIndex := core.GetTransaction(chainDb, txHash)
	if tx == nil {
		return nil, fmt.Errorf("transaction %x not found", txHash)
	}
	msg, context, statedb, _, err := computeTxEnv(bc, blockHash, int(txIndex))
	if err != nil {
		return nil, err
	}

	// Run the transaction with tracing enabled.
	vmenv := vm.NewEVM(context, statedb, params.MainnetChainConfig, vm.Config{Debug: true, Tracer: tracer})
	ret, gas, failed, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas()))
	if err != nil {
		return nil, fmt.Errorf("tracing failed: %v", err)
	}
	return &ethapi.ExecutionResult{
		Gas:         gas,
		Failed:      failed,
		ReturnValue: fmt.Sprintf("%x", ret),
		StructLogs:  ethapi.FormatLogs(tracer.StructLogs()),
	}, nil
}

func main() {
	flag.Parse()
    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal("could not create CPU profile: ", err)
        }
        if err := pprof.StartCPUProfile(f); err != nil {
            log.Fatal("could not start CPU profile: ", err)
        }
        defer pprof.StopCPUProfile()
    }
    ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 1024, 512)
	defer ethDb.Close()
	if err != nil {
		panic(fmt.Sprintf("Could not open database %s\n", err))
	}
	bc, err := core.NewBlockChain(ethDb, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{})
	currentBlockNr := bc.CurrentBlock().NumberU64()
	fmt.Printf("Current block number: %d\n", currentBlockNr)
	var recordingStateDb *state.RecordingStateDatabase
	var recordingState *state.StateDB
	for blockNr := uint64(2284500 + 5000); blockNr < uint64(2284500 + 10000 + 1); blockNr++ {
		block := bc.GetBlockByNumber(blockNr)
		if block == nil {
			fmt.Printf("block #%d not found\n", blockNr)
			continue
		}
		var (
			validator  = bc.Validator()
			processor  = bc.Processor()
		)
		config := vm.Config{
			Debug:  false,
			Tracer: nil,
		}
		// State root hash before block's execution (state root hash of the previous block)
		prevBlock := bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
		preBlockStateRoot := prevBlock.Root()

		//readBlockNr := uint32(prevBlock.NumberU64())
		if recordingState == nil /*|| recordingState.IntermediateRoot(params.MainnetChainConfig.IsEIP158(prevBlock.Number()), readBlockNr) != prevBlock.Root()*/ {
			recordingStateDb = state.NewRecordingStateDatabase(ethDb)
			recordingState, err = state.RecordingState(preBlockStateRoot, recordingStateDb, uint32(blockNr-1))
			if err != nil {
				fmt.Printf("block #%d could not create recordingState: %s\n", blockNr, err)
				continue
			}
		} else {
			recordingStateDb.CleanForNextBlock()
			recordingState.CleanForNextBlock()
			//fmt.Printf("Reused stateDB from block %d\n", readBlockNr)
		}
		receipts, _, usedGas, err := processor.Process(block, recordingState, config)

		if err != nil {
			fmt.Printf("block #%d could not process: %s\n", blockNr, err)
			continue
		}
		if err := validator.ValidateState(block, prevBlock, recordingState, receipts, usedGas); err != nil {
			fmt.Printf("block #%d could not validate: %s\n", blockNr, err)
			continue
		}
		if blockNr % 1000 == 0 {
			fmt.Printf("Blocks up to %d processed\n", blockNr)
		}
		/*
		var witness_size int
		playbackDatabaseNew := state.EmptyPlaybackDatabase()
		{
			playbackDatabaseOld := state.NewPlaybackDatabase(recordingStateDb)

			// Create a witness blob
			witness, err := rlp.EncodeToBytes(playbackDatabaseOld)
			if err != nil {
				fmt.Printf("block #%d could not encode witness: %s\n", blockNr, err)
				continue
			}
			witness_size = len(witness)

			// Recreate playback database from the witness blob
			stream := rlp.NewStream(bytes.NewReader(witness), 0)
			if err = stream.Decode(&playbackDatabaseNew); err != nil {
				fmt.Printf("block #%d could not decode witness: %s\n", blockNr, err)
				continue
			}
		}

		playbackState, err := state.PlaybackState(preBlockStateRoot, playbackDatabaseNew, uint32(blockNr-1))
		if err != nil {
			fmt.Printf("block #%d could not create playbackState: %s\n", blockNr, err)
			continue
		}
		receipts, _, usedGas, err = processor.Process(block, playbackState, config)

		if err != nil {
			fmt.Printf("block #%d could re-process: %s\n", blockNr, err)
			continue
		}
		if err = validator.ValidateState(block, prevBlock, playbackState, receipts, usedGas); err != nil {
			fmt.Printf("block #%d could not validate re-processed: %s\n", blockNr, err)
			continue
		}
		fmt.Printf("%d %d\n", blockNr, witness_size)
		*/
	}
	/*
	execResult, err := traceTx(bc, ethDb, common.HexToHash("0xaf7a40833cff97e8dacacb2d41dd97a8decaa4f751b413e78e40f0c8bf7d242d"))
	if err != nil {
		fmt.Printf("Tracing failed failed: %v\n", err)
	}
	rJson, err := json.MarshalIndent(execResult, "", " ")
	if err != nil {
		fmt.Printf("Mashaling failed: %v\n", err)
	}
	ioutil.WriteFile("/Users/alexeyakhunov/my/go-ethereum/tx_trace.json", rJson, 0)
	*/
}

