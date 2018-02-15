package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/console"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/internal/debug"
	"github.com/ethereum/go-ethereum/params"
	"gopkg.in/urfave/cli.v1"
)

var (
	// Git SHA1 commit hash of the release (set via linker flags)
	gitCommit = ""
	// The app that holds all commands and flags.
	app = utils.NewApp(gitCommit, "Ethereum Tester")
	// flags that configure the node
	flags = []cli.Flag{
	}
)

func init() {
	// Initialize the CLI app and start Geth
	app.Action = tester
	app.HideVersion = true // we have a command to print the version
	app.Copyright = "Copyright 2018 The go-ethereum Authors"
	app.Commands = []cli.Command{
	}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = append(app.Flags, flags...)

	app.Before = func(ctx *cli.Context) error {
		runtime.GOMAXPROCS(runtime.NumCPU())
		if err := debug.Setup(ctx); err != nil {
			return err
		}
		return nil
	}

	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		console.Stdin.Close() // Resets terminal mode.
		return nil
	}
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func tester(ctx *cli.Context) error {
	// Create a protocol manager
	fd, err := NewFakeDatabase()
	if err != nil {
		return err
	}
	bc, err := core.NewBlockChain(
		fd,
		nil, /* *CacheConfig */
		params.MainnetChainConfig,
		nil, /* consensus.Engine */
		vm.Config{},
	)
	if err != nil {
		return err
	}
	txPool := core.NewTxPool(
		core.DefaultTxPoolConfig,
		params.MainnetChainConfig,
		bc,
	)
	pm, err := eth.NewProtocolManager(
		params.MainnetChainConfig,
		downloader.FullSync,
		1,
		new(event.TypeMux),
		txPool, /* txPool */
		nil, /* consensus.Engine */
		bc, /* *core.BlockChain */
		fd, /* ethdb.Database */
	)
	if err != nil {
		return err
	}
	pm.Start(1)
	fmt.Printf("%s %s\n", ctx.Args()[0], ctx.Args()[1])
	rewriteChain(ctx.Args()[0], ctx.Args()[1])
	return nil
}