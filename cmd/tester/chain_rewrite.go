package main

import (
	"os"
	"fmt"
	"io"
	"compress/gzip"
	"strings"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

func rewriteChain(inputFile, outputFile string) error {
	input, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer output.Close()
	var reader io.Reader = input
	if strings.HasSuffix(inputFile, ".gz") {
		if reader, err = gzip.NewReader(reader); err != nil {
			return err
		}
	}
	stream := rlp.NewStream(reader, 0)
	//blocks := make(types.Blocks, importBatchSize)
	n := 0
	for {
		var b types.Block
		if err := stream.Decode(&b); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("at block %d: %v", n, err)
		}
		// don't import first block
		if b.NumberU64() == 0 {
			continue
		}
		n++
	}
	fmt.Printf("%d blocks read\n", n)
	return nil
}