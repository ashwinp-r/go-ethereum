package main

import (
	//"bytes"
	//"encoding/binary"
	//"math/big"
	"fmt"
	//"strings"
	//"strconv"
	"flag"
	"runtime/pprof"
	"os"
	"log"
	//"io/ioutil"
	//"bufio"
	"time"

	"github.com/boltdb/bolt"
	//"github.com/wcharczuk/go-chart"
	//util "github.com/wcharczuk/go-chart/util"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/common"
	//"github.com/ethereum/go-ethereum/consensus/ethash"
	//"github.com/ethereum/go-ethereum/core"
	//"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	//"github.com/ethereum/go-ethereum/core/types"
	//"github.com/ethereum/go-ethereum/core/vm"
	//"github.com/ethereum/go-ethereum/ethdb"
	//"github.com/ethereum/go-ethereum/params"
	//"github.com/ethereum/go-ethereum/trie"
	//"github.com/ethereum/go-ethereum/rlp"
)

var emptyCodeHash = crypto.Keccak256(nil)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421").Bytes()

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var reset = flag.Int("reset", -1, "reset to given block number")
var rewind = flag.Int("rewind", 1, "rewind to given number of blocks")
var block = flag.Int("block", 1, "specifies a block number for operation")
var account = flag.String("account", "0x", "specifies account to investigate")

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func stateGrowth1() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
 	check(err)
 	defer db.Close()
 	var count int
 	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.AccountsHistoryBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		return nil
 	})
 	check(err)
 	fmt.Printf("Processing took %s\n", time.Since(startTime))
 	fmt.Printf("Account history records: %d\n", count)
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
    stateGrowth1()
}

