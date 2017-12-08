package eth

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

type StateAccessTraceResult struct {
	BlockNumber     int     `json:"block_number"`   // Block number
    WitnessSize 	int 	`json:"witness_size"`   // Witness size in bytes
	Error 		    string  `json:"error"`
}

func (api *PrivateDebugAPI) BlockWitnessSizeByNumber(blockNr rpc.BlockNumber) StateAccessTraceResult {
	// Fetch the block that we aim to reprocess
	var block *types.Block
	switch blockNr {
	case rpc.PendingBlockNumber:
		// Pending block is only known by the miner
		block = api.eth.miner.PendingBlock()
	case rpc.LatestBlockNumber:
		block = api.eth.blockchain.CurrentBlock()
	default:
		block = api.eth.blockchain.GetBlockByNumber(uint64(blockNr))
	}
	if block == nil {
		return StateAccessTraceResult{Error: fmt.Sprintf("block #%d not found", blockNr)}
	}
	witness_size, err := api.computeWitnessSize(block)
	if err != nil {
		return StateAccessTraceResult{BlockNumber: int(blockNr), Error: formatError(err)}
	}
	return StateAccessTraceResult{BlockNumber: int(blockNr), WitnessSize: witness_size}
}

func (api *PrivateDebugAPI) computeWitnessSize(block *types.Block) (int, error) {
	// Validate and reprocess the block
	var (
		blockchain = api.eth.BlockChain()
		validator  = blockchain.Validator()
		processor  = blockchain.Processor()
	)
	config := vm.Config{
		Debug:  false,
		Tracer: nil,
	}
	if err := api.eth.engine.VerifyHeader(blockchain, block.Header(), true); err != nil {
		return 0, err
	}
	// State root hash before block's execution (state root hash of the previous block)
	preBlockStateRoot := blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1).Root()

	recordingStateDb := state.NewRecordingStateDatabase(api.eth.ChainDb())
	recordingState, err := state.RecordingState(preBlockStateRoot, recordingStateDb)
	if err != nil {
		return 0, err
	}

	receipts, _, usedGas, err := processor.Process(block, recordingState, config)

	if err != nil {
		return 0, err
	}
	if err := validator.ValidateState(block, blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1), recordingState, receipts, usedGas); err != nil {
		return 0, err
	}

	var witness_size int
	playbackDatabaseNew := state.EmptyPlaybackDatabase()
	{
		playbackDatabaseOld := state.NewPlaybackDatabase(recordingStateDb)

		// Create a witness blob
		witness, err := rlp.EncodeToBytes(playbackDatabaseOld)
		if err != nil {
			return 0, err
		}
		witness_size = len(witness)

		// Recreate playback database from the witness blob
		stream := rlp.NewStream(bytes.NewReader(witness), 0)
		if err = stream.Decode(&playbackDatabaseNew); err != nil {
			return 0, err
		}
	}

	playbackState, err := state.PlaybackState(preBlockStateRoot, playbackDatabaseNew)
	if err != nil {
		return 0, err
	}
	receipts, _, usedGas, err = processor.Process(block, playbackState, config)

	if err != nil {
		return 0, err
	}
	if err = validator.ValidateState(block, blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1), playbackState, receipts, usedGas); err != nil {
		return 0, err
	}

	return witness_size, nil
}
