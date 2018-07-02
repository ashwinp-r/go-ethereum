package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/console"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/internal/debug"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/log"
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
	var err error
	_, err = NewBlockGenerator("emptyblocks", 100)
	if err != nil {
		return err
	}
	//fmt.Printf("%s %s\n", ctx.Args()[0], ctx.Args()[1])
	tp := &TesterProtocol{}
	//tp.blockFeeder, err = NewBlockAccessor(ctx.Args()[0]/*, ctx.Args()[1]*/)
	tp.blockFeeder, err = NewBlockGenerator("emptyblocks", 1000)
	defer tp.blockFeeder.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to read blockchain file: %v", err))
	}
	tp.protocolVersion = uint32(eth.ProtocolVersions[0])
	tp.networkId = 1 // Mainnet
	tp.genesisBlockHash = params.MainnetGenesisHash
	serverKey, err := crypto.GenerateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate server key: %v", err))
	}
	p2pConfig := p2p.Config{}
	p2pConfig.PrivateKey = serverKey
	p2pConfig.Name = "geth tester"
	p2pConfig.Logger = log.New()
	p2pConfig.Protocols = []p2p.Protocol{p2p.Protocol{
		Name: eth.ProtocolName,
		Version: eth.ProtocolVersions[0],
		Length: eth.ProtocolLengths[0],
		Run: tp.protocolRun,
	}}
	server := &p2p.Server{Config: p2pConfig}
	// Add protocol
	if err := server.Start(); err != nil {
		panic(fmt.Sprintf("Could not start server: %v", err))
	}
	nodeToConnect, err := discover.ParseNode(ctx.Args()[1])
	if err != nil {
		panic(fmt.Sprintf("Could not parse the node info: %v", err))
	}
	fmt.Printf("Parsed node: %s, IP: %s\n", nodeToConnect, nodeToConnect.IP)
	server.AddPeer(nodeToConnect)
	time.Sleep(1*time.Minute)
	return nil
}