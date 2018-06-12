package main

import (
	"os"
	"fmt"
	"io"
	"compress/gzip"
	"strings"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

func rewriteChain(inputFile/*, outputFile*/ string) (lastBlockHash common.Hash, lastBlockDifficulty *big.Int,
		headersByHash map[common.Hash]*types.Header, headersByNumber map[uint64]*types.Header, err error) {
	input, err := os.Open(inputFile)
	if err != nil {
		return
	}
	defer input.Close()
	/*
	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer output.Close()
	*/
	var reader io.Reader = input
	if strings.HasSuffix(inputFile, ".gz") {
		if reader, err = gzip.NewReader(reader); err != nil {
			return
		}
	}
	stream := rlp.NewStream(reader, 0)
	//blocks := make(types.Blocks, importBatchSize)
	var b types.Block
	headersByHash = make(map[common.Hash]*types.Header)
	headersByNumber = make(map[uint64]*types.Header)
	n := 0
	for {
		if err = stream.Decode(&b); err == io.EOF {
			err = nil // Clear it
			break
		} else if err != nil {
			err = fmt.Errorf("at block %d: %v", n, err)
			return
		}
		// don't import first block
		if b.NumberU64() == 0 {
			continue
		}
		headersByHash[b.Hash()] = b.Header()
		headersByNumber[b.NumberU64()] = b.Header()
		n++
	}
	fmt.Printf("%d blocks read\n", n)
	lastBlockHash = b.Hash()
	lastBlockDifficulty = b.Difficulty()
	return
}