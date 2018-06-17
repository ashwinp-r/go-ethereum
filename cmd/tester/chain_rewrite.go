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

type BlockAccessor struct {
	input *os.File
	blockOffsetByHash map[common.Hash]uint64
	blockOffsetByNumber map[uint64]uint64
	headersByHash map[common.Hash]*types.Header
	headersByNumber map[uint64]*types.Header
}

func (ba *BlockAccessor) Close() {
	ba.input.Close()
}

func (ba *BlockAccessor) GetHeaderByHash(hash common.Hash) *types.Header {
	return ba.headersByHash[hash]
}

func (ba *BlockAccessor) GetHeaderByNumber(number uint64) *types.Header {
	return ba.headersByNumber[number]
}

func (ba *BlockAccessor) readBlockFromOffset(offset uint64) (*types.Block, error) {
	ba.input.Seek(int64(offset), 0)
	stream := rlp.NewStream(ba.input, 0)
	var b types.Block
	if err := stream.Decode(&b); err != nil {
		return nil, err
	}
	return &b, nil
}

func (ba *BlockAccessor) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	if blockOffset, ok := ba.blockOffsetByHash[hash]; ok {
		return ba.readBlockFromOffset(blockOffset)
	}
	return nil, nil
}

func rewriteChain(inputFile/*, outputFile*/ string) (lastBlock *types.Block, td *big.Int, ba *BlockAccessor, err error) {
	input, err := os.Open(inputFile)
	if err != nil {
		return
	}
	ba = &BlockAccessor{
		input: input,
		blockOffsetByHash: make(map[common.Hash]uint64),
		blockOffsetByNumber: make(map[uint64]uint64),
		headersByHash: make(map[common.Hash]*types.Header),
		headersByNumber: make(map[uint64]*types.Header),
	}
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
	n := 0
	td = new(big.Int)
	var pos uint64
	for {
		blockOffset := pos
		var size uint64
		_, size, err = stream.Kind()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return
		}
		pos += 1 + size
		if size >= 56 {
			bytecount := 0
			for size > 0 {
				size >>= 8
				bytecount++
			}
			pos += uint64(bytecount)
		}
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
		h := b.Header()
		hash := h.Hash()
		ba.headersByHash[hash] = h
		ba.headersByNumber[b.NumberU64()] = h
		ba.blockOffsetByHash[hash] = blockOffset
		ba.blockOffsetByNumber[b.NumberU64()] = blockOffset
		td = new(big.Int).Add(td, b.Difficulty())
		n++
	}
	fmt.Printf("%d blocks read, bytes read %d\n", n, pos)
	lastBlock = &b
	return
}