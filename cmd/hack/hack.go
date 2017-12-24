package main

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

func main() {
	ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 1024, 1024)
	defer ethDb.Close()
	if err != nil {
		panic(fmt.Sprintf("Could not open database %s\n", err))
	}
	bc, err := core.NewBlockChain(ethDb, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{})
	fmt.Printf("Current block number: %d\n", bc.CurrentBlock().NumberU64())
}

