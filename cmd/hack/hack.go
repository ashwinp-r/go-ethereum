package main

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"fmt"
	"strings"
	"strconv"
	"flag"
	"runtime/pprof"
	"os"
	"log"
	"io/ioutil"
	"bufio"
	"time"

	"github.com/boltdb/bolt"
	"github.com/wcharczuk/go-chart"
	util "github.com/wcharczuk/go-chart/util"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/rlp"
)

var emptyCodeHash = crypto.Keccak256(nil)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421").Bytes()

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var reset = flag.Int("reset", -1, "reset to given block number")

func bucketList(db *bolt.DB) [][]byte {
	bucketList := [][]byte{}
	err := db.View(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if len(name) == 20 || bytes.Equal(name, []byte("AT")) {
				n := make([]byte, len(name))
				copy(n, name)
				bucketList = append(bucketList, n)
			}
			return nil
		})
		return err
	})
	if err != nil {
		panic(fmt.Sprintf("Could view db: %s", err))
	}
	return bucketList
}

// prefixLen returns the length of the common prefix of a and b.
func prefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

	
func check(e error) {
    if e != nil {
        panic(e)
    }
}

func parseFloat64(str string) float64 {
	v, _ := strconv.ParseFloat(str, 64)
	return v
}

func readData(filename string) (blocks []float64, hours []float64, dbsize []float64, trienodes []float64, heap []float64) {
	err := util.File.ReadByLines(filename, func(line string) error {
		parts := strings.Split(line, ",")
		blocks = append(blocks, parseFloat64(strings.Trim(parts[0], " ")))
		hours = append(hours, parseFloat64(strings.Trim(parts[1], " ")))
		dbsize = append(dbsize, parseFloat64(strings.Trim(parts[2], " ")))
		trienodes = append(trienodes, parseFloat64(strings.Trim(parts[3], " ")))
		heap = append(heap, parseFloat64(strings.Trim(parts[4], " ")))
		return nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	return
}

func notables() []chart.GridLine {
	return []chart.GridLine{
		{Value: 1.0},
		{Value: 2.0},
		{Value: 3.0},
		{Value: 4.0},
		{Value: 5.0},
	}
}

func days() []chart.GridLine {
	return []chart.GridLine{
		{Value: 24.0},
		{Value: 48.0},
		{Value: 72.0},
		{Value: 96.0},
		{Value: 120.0},
		{Value: 144.0},
		{Value: 168.0},
		{Value: 192.0},
		{Value: 216.0},
		{Value: 240.0},
		{Value: 264.0},
		{Value: 288.0},
	}
}

func mychart() {
	blocks, hours, dbsize, trienodes, heap := readData("geth1.csv")
	blocks0, hours0, _, _, _ := readData("geth.csv")
	mainSeries := &chart.ContinuousSeries{
		Name: "Cumulative sync time (SSD)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: blocks,
		YValues: hours,
	}
	hddSeries := &chart.ContinuousSeries{
		Name: "Cumulative sync time (HDD)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorRed,
			FillColor:   chart.ColorRed.WithAlpha(100),
		},
		XValues: blocks0,
		YValues: hours0,
	}
	dbsizeSeries := &chart.ContinuousSeries{
		Name: "Database size",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: blocks,
		YValues: dbsize,
	}

	graph1 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "Elapsed time",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d h", int(v.(float64)))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: days(),
		},
		YAxisSecondary: chart.YAxis{
			NameStyle: chart.StyleShow(),
			Style: chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d G", int(v.(float64)))
			},
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			GridLines: notables(),
		},
		Series: []chart.Series{
			mainSeries,
			hddSeries,
			dbsizeSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err := graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart1.png", buffer.Bytes(), 0644)
    check(err)

	heapSeries := &chart.ContinuousSeries{
		Name: "Allocated heap",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorYellow,
			FillColor:   chart.ColorYellow.WithAlpha(100),
		},
		XValues: blocks,
		YValues: heap,
	}
	trienodesSeries := &chart.ContinuousSeries{
		Name: "Trie nodes",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorGreen,
		},
		YAxis:   chart.YAxisSecondary,
		XValues: blocks,
		YValues: trienodes,		
	}
	graph2 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "Allocated heap",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f G", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorYellow,
				StrokeWidth: 1.0,
			},
			GridLines: days(),
		},
		YAxisSecondary: chart.YAxis{
			NameStyle: chart.StyleShow(),
			Style: chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f m", v.(float64))
			},
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			GridLines: notables(),
		},
		Series: []chart.Series{
			heapSeries,
			trienodesSeries,
		},
	}

	graph2.Elements = []chart.Renderable{chart.LegendThin(&graph2)}
	buffer.Reset()
	err = graph2.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart2.png", buffer.Bytes(), 0644)
    check(err)
}

func accountSavings(db *bolt.DB) (int,int) {
	emptyRoots := 0
	emptyCodes := 0
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("AT"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if bytes.Index(v, emptyRoot) != -1 {
				emptyRoots++
			}
			if bytes.Index(v, emptyCodeHash) != -1 {
				emptyCodes++
			}
		}
		return nil
	})
	return emptyRoots, emptyCodes
}

func allBuckets(db *bolt.DB) [][]byte {
	bucketList := [][]byte{}
	err := db.View(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			n := make([]byte, len(name))
			copy(n, name)
			bucketList = append(bucketList, n)
			return nil
		})
		return err
	})
	if err != nil {
		panic(fmt.Sprintf("Could view db: %s", err))
	}
	return bucketList
}

func printBuckets(db *bolt.DB) {
	bucketList := allBuckets(db)
	for _, bucket := range bucketList {
		fmt.Printf("%s\n", bucket)
	}
}

func bucketStats(db *bolt.DB) {
	bucketList := allBuckets(db)
	storageStats := new(bolt.BucketStats)
	hStorageStats := new(bolt.BucketStats)
	fmt.Printf(",BranchPageN,BranchOverflowN,LeafPageN,LeafOverflowN,KeyN,Depth,BranchAlloc,BranchInuse,LeafAlloc,LeafInuse,BucketN,InlineBucketN,InlineBucketInuse\n")
	db.View(func (tx *bolt.Tx) error {
		for _, bucket := range bucketList {
			b := tx.Bucket(bucket)
			bs := b.Stats()
			if len(bucket) == 20 {
				storageStats.Add(bs)
			} else if len(bucket) == 21 {
				hStorageStats.Add(bs)
			} else {
				fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", string(bucket),
					bs.BranchPageN,bs.BranchOverflowN,bs.LeafPageN,bs.LeafOverflowN,bs.KeyN,bs.Depth,bs.BranchAlloc,bs.BranchInuse,
					bs.LeafAlloc,bs.LeafInuse,bs.BucketN,bs.InlineBucketN,bs.InlineBucketInuse)
			}
		}
		return nil
	})
	bs := *storageStats
	fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", "Contract Storage",
		bs.BranchPageN,bs.BranchOverflowN,bs.LeafPageN,bs.LeafOverflowN,bs.KeyN,bs.Depth,bs.BranchAlloc,bs.BranchInuse,
		bs.LeafAlloc,bs.LeafInuse,bs.BucketN,bs.InlineBucketN,bs.InlineBucketInuse)
	bs = *hStorageStats
	fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", "Contract hStorage",
		bs.BranchPageN,bs.BranchOverflowN,bs.LeafPageN,bs.LeafOverflowN,bs.KeyN,bs.Depth,bs.BranchAlloc,bs.BranchInuse,
		bs.LeafAlloc,bs.LeafInuse,bs.BucketN,bs.InlineBucketN,bs.InlineBucketInuse)
}

func printOccupancies(t *trie.Trie, db ethdb.Database, blockNr uint64) {
	o := make(map[int]map[int]int)
	t.CountOccupancies(db, blockNr, o)
	for level, lo := range o {
		for i, count := range lo {
			fmt.Printf("[%d %d]:%d ", level, i, count)
		}
	}
	fmt.Printf("\n")
}

func trieStats() {
	//db, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata")
	db, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	lastHash := rawdb.ReadHeadHeaderHash(db)
	lastNumber := rawdb.ReadHeaderNumber(db, lastHash)
	lastHeader := rawdb.ReadHeader(db, lastHash, *lastNumber)
	tds, err := state.NewTrieDbState(lastHeader.Root, db, *lastNumber)
	if err != nil {
		panic(err)
	}
	t := tds.AccountTrie()
	printOccupancies(t, db, *lastNumber)
	/*
	statedb := state.New(triedbst)
	t := statedb.GetTrie()
	for i := 0; i < 1; i++ {
		//h1 := t.ResolveRoot(db, lastHeader.Root, lastNumber, i)
		//fmt.Printf("Resolved hash for %d: %s\n", i, h1)
		startTime := time.Now()
		h2 := t.Rebuild(db, lastNumber, i)
		fmt.Printf("Rebuding took %s\n", time.Since(startTime))
		fmt.Printf("Rebult to %s, actual root %x\n", h2, lastHeader.Root)
		fmt.Printf("\n\n")
		//if !bytes.Equal(h1, h2) || !matched {
		//	fmt.Printf("DIFFERENT ROOTS: %d %s %s\n", i, h1, h2)
		//	break
		//}
	}
	statedb.PrintOccupancies()
	
	fmt.Printf("%x %x\n", lastHeader.Root, statedb.IntermediateRoot(true))
	triedb, tree, err := statedb.EnumerateAccounts()
	if err != nil {
		panic(err)
	}
	sectrie, _ := triedb.(*trie.SecureTrie)
	t := sectrie.GetTrie()
	printOccupancies(t, db, lastNumber)
	nextThreshold := big.NewInt(0)
	step := big.NewInt(1)
	tree.AscendGreaterOrEqual(&state.AccountItem{SecKey: nil, Balance: big.NewInt(0)}, func(i llrb.Item) bool {
		item := i.(*state.AccountItem)
		if item.Balance.Cmp(nextThreshold) != -1 {
			fmt.Printf("Threshold: %s | ", nextThreshold.String())
			printOccupancies(t, db, lastNumber)
			for ; item.Balance.Cmp(nextThreshold) != -1; {
				nextThreshold = nextThreshold.Add(nextThreshold, step)
			}
		}
		t.TryDelete(db, item.SecKey, lastNumber)
		return true
	})
	fmt.Printf("Final check | ")
	printOccupancies(t, db, lastNumber)
	*/
}

func readTrieLog() ([]float64, map[int][]float64, []float64) {
	data, err := ioutil.ReadFile("dust/hack.log")
	check(err)
	thresholds := []float64{}
	counts := map[int][]float64{}
	for i := 2; i <= 16; i++ {
		counts[i] = []float64{}
	}
	shorts := []float64{}
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		if bytes.HasPrefix(line, []byte("Threshold:")) {
			tokens := bytes.Split(line, []byte(" "))
			if len(tokens) == 23 {
				wei := parseFloat64(string(tokens[1]))
				thresholds = append(thresholds, wei)
				for i := 2; i <= 16; i++ {
					pair := bytes.Split(tokens[i+3], []byte(":"))
					counts[i] = append(counts[i], parseFloat64(string(pair[1])))
				}
				pair := bytes.Split(tokens[21], []byte(":"))
				shorts = append(shorts, parseFloat64(string(pair[1])))
			}
		}
	}
	return thresholds, counts, shorts
}

func ts() []chart.GridLine {
	return []chart.GridLine{
		{Value: 420.0},
	}
}

func trieChart() {
	thresholds, counts, shorts := readTrieLog()
	fmt.Printf("%d %d %d\n", len(thresholds), len(counts), len(shorts))
	shortsSeries := &chart.ContinuousSeries{
		Name: "Short nodes",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: thresholds,
		YValues: shorts,
	}
	countSeries := make(map[int]*chart.ContinuousSeries)
	for i := 2; i <= 16; i++ {
		countSeries[i] = &chart.ContinuousSeries{
			Name: fmt.Sprintf("%d-nodes", i),
			Style: chart.Style{
				Show:        true,
				StrokeColor: chart.GetAlternateColor(i),
			},
			XValues: thresholds,
			YValues: counts[i],
		}
	}
	xaxis := &chart.XAxis{
		Name: "Dust theshold",
		Style: chart.Style{
			Show: true,
		},
		ValueFormatter: func(v interface{}) string {
			return fmt.Sprintf("%d wei", int(v.(float64)))
		},
		GridMajorStyle: chart.Style{
			Show:        true,
			StrokeColor: chart.DefaultStrokeColor,
			StrokeWidth: 1.0,
		},
		Range: &chart.LogRange{
			Min: thresholds[0],
			Max: thresholds[len(thresholds)-1],
		},
		Ticks: []chart.Tick{
			{Value: 0.0, Label: "0"},
			{Value: 1.0, Label: "wei"},
			{Value: 10.0, Label: "10"},
			{Value: 100.0, Label: "100"},
			{Value: 1e3, Label: "1e3"},
			{Value: 1e4, Label: "1e4"},
			{Value: 1e5, Label: "1e5"},
			{Value: 1e6, Label: "1e6"},
			{Value: 1e7, Label: "1e7"},
			{Value: 1e8, Label: "1e8"},
			{Value: 1e9, Label: "1e9"},
			{Value: 1e10, Label: "1e10"},
			//{1e15, "finney"},
			//{1e18, "ether"},
		},
	}

	graph3 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		XAxis: *xaxis,
		YAxis: chart.YAxis{
			Name:      "Node count",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%dm", int(v.(float64)/1e6))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.DefaultStrokeColor,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			shortsSeries,
		},
	}
	graph3.Elements = []chart.Renderable{chart.LegendThin(&graph3)}
	buffer := bytes.NewBuffer([]byte{})
	err := graph3.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart3.png", buffer.Bytes(), 0644)
    check(err)
	graph4 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		XAxis: *xaxis,
		YAxis: chart.YAxis{
			Name:      "Node count",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fm", v.(float64)/1e6)
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.DefaultStrokeColor,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			countSeries[2],
			countSeries[3],
		},
	}
	graph4.Elements = []chart.Renderable{chart.LegendThin(&graph4)}
	buffer = bytes.NewBuffer([]byte{})
	err = graph4.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart4.png", buffer.Bytes(), 0644)
    check(err)
	graph5 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		XAxis: *xaxis,
		YAxis: chart.YAxis{
			Name:      "Node count",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.2fk", v.(float64)/1e3)
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.DefaultStrokeColor,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			countSeries[4],
			countSeries[5],
			countSeries[6],
			countSeries[7],
			countSeries[8],
			countSeries[9],
			countSeries[10],
			countSeries[11],
			countSeries[12],
			countSeries[13],
			countSeries[14],
			countSeries[15],
			countSeries[16],
		},
	}
	graph5.Elements = []chart.Renderable{chart.LegendThin(&graph5)}
	buffer = bytes.NewBuffer([]byte{})
	err = graph5.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("chart5.png", buffer.Bytes(), 0644)
    check(err)
}

func testRewind() {
	//ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	db := ethDb.NewBatch()
	defer db.Rollback()
	bc, err := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{})
	check(err)
	currentBlock := bc.CurrentBlock()
	currentBlockNr := currentBlock.NumberU64()
	//currentBlockNr -= 22
	//currentBlock = bc.GetBlockByNumber(currentBlockNr)
	fmt.Printf("Current block number: %d\n", currentBlockNr)
	fmt.Printf("Current block hash: %x\n", currentBlock.Hash())
	fmt.Printf("Current block root hash: %x\n", currentBlock.Root())
	fmt.Printf("All headers at the same height\n")
	{
		var hashes []common.Hash
		numberEnc := make([]byte, 8)
		binary.BigEndian.PutUint64(numberEnc, currentBlockNr)
		if err := ethDb.Walk([]byte("h"), numberEnc, 8*8, func(k, v []byte) ([]byte, ethdb.WalkAction, error) {
			if len(k) == 8 + 32 {
				hashes = append(hashes, common.BytesToHash(k[8:]))
			}
			return nil, ethdb.WalkActionNext, nil
		}); err != nil {
			panic(err)
		}
		for _, hash := range hashes {
			h := rawdb.ReadHeader(ethDb, hash, currentBlockNr)
			fmt.Printf("block hash: %x, root hash: %x\n", h.Hash(), h.Root)
		}
	}
	fmt.Printf("All headers at the previous height\n")
	{
		var hashes []common.Hash
		numberEnc := make([]byte, 8)
		binary.BigEndian.PutUint64(numberEnc, currentBlockNr-1)
		if err := ethDb.Walk([]byte("h"), numberEnc, 8*8, func(k, v []byte) ([]byte, ethdb.WalkAction, error) {
			if len(k) == 8 + 32 {
				hashes = append(hashes, common.BytesToHash(k[8:]))
			}
			return nil, ethdb.WalkActionNext, nil
		}); err != nil {
			panic(err)
		}
		for _, hash := range hashes {
			h := rawdb.ReadHeader(ethDb, hash, currentBlockNr)
			fmt.Printf("block hash: %x, root hash: %x\n", h.Hash(), h.Root)
		}
	}
	tds, err := state.NewTrieDbState(currentBlock.Root(), db, currentBlockNr)
	tds.SetHistorical(false)
	check(err)
	startTime := time.Now()
	err = tds.Rebuild()
	fmt.Printf("Rebuld done in %v\n", time.Since(startTime))
	check(err)
	rebuiltRoot, err := tds.TrieRoot()
	check(err)
	fmt.Printf("Rebuit root hash: %x\n", rebuiltRoot)
	startTime = time.Now()
	rewindLen := uint64(14)
	err = tds.UnwindTo(currentBlockNr - rewindLen, false)
	fmt.Printf("Unwind done in %v\n", time.Since(startTime))
	check(err)
	rewoundBlock := bc.GetBlockByNumber(currentBlockNr - rewindLen)
	fmt.Printf("Rewound block number: %d\n", rewoundBlock.NumberU64())
	fmt.Printf("Rewound block root hash: %x\n", rewoundBlock.Root())
	rewoundRoot, err := tds.TrieRoot()
	check(err)
	fmt.Printf("Calculated rewound root hash: %x\n", rewoundRoot)
}

func testStartup() {
	startTime := time.Now()
	//ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{})
	check(err)
	currentBlock := bc.CurrentBlock()
	currentBlockNr := currentBlock.NumberU64()
	fmt.Printf("Current block number: %d\n", currentBlockNr)
	fmt.Printf("Current block root hash: %x\n", currentBlock.Root())
	t := trie.New(common.Hash{}, state.AccountsBucket, nil, false)
	r := trie.NewResolver(ethDb, false, true)
	key := []byte{}
	rootHash := currentBlock.Root()
	tc := t.NewContinuation(key, 0, rootHash[:])
	r.AddContinuation(tc)
	err = r.ResolveWithDb(ethDb, currentBlockNr)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	fmt.Printf("Took %v\n", time.Since(startTime))
}

func testResolve() {
	startTime := time.Now()
	//ethDb, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	treePrefix := common.FromHex("1194e966965418c7d73a42cceeb254d875860356")
	t := trie.New(common.Hash{}, state.StorageBucket, treePrefix, true)
	r := trie.NewResolver(ethDb, false, false)
	key := common.FromHex("0d0c02060f0d09030a0a0000070808000c04080606090b070c060a0e020803030b030f040f0e080c090f0f020a0c06060b0402090e0b050007090d06010c0a0310")
	resolveHash := common.FromHex("931f3dbc6b2e9c23cf9cd399504139efe208e33d3e3ce51a2f040aac0159b79e")
	tc := t.NewContinuation(key, 0, resolveHash)
	r.AddContinuation(tc)
	err = r.ResolveWithDb(ethDb, 244574)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	fmt.Printf("Took %v\n", time.Since(startTime))
}

func hashFile() {
	f, err := os.Open("/Users/alexeyakhunov/mygit/go-ethereum/geth.log")
	check(err)
	defer f.Close()
	w, err := os.Create("/Users/alexeyakhunov/mygit/go-ethereum/geth_read.log")
	check(err)
	defer w.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "ResolveWithDb") || strings.HasPrefix(line, "Error") ||
			strings.HasPrefix(line, "0000000000000000000000000000000000000000000000000000000000000000") || 
			strings.HasPrefix(line, "ERROR") || strings.HasPrefix(line, "tc{") {
			fmt.Printf("%d %s\n", count, line)
			count++
		} else if count == 66 {
			w.WriteString(line)
			w.WriteString("\n")
		}
	}
	fmt.Printf("%d lines scanned\n", count)
}

func buildHashFromFile() {
	treePrefix := common.FromHex("2a0c0dbecc7e4d658f48e01e3fa353f44050c208")
	t := trie.New(common.Hash{}, state.StorageBucket, treePrefix, false)
	r := trie.NewResolver(nil, false, false)
	key := common.FromHex("010d04080f0e060e08000d09040800040f0f0f020c04060109090d0c020c090a010d0609070a0905020e0904000808030c020f060c0709060107080f0209020210")
	resolveHash := common.FromHex("9edde1605e84902c450fdf275a6c9ef00fc67691d2ed62b4de35ef8c8bf1b20")
	tc := t.NewContinuation(key, 0, resolveHash)
	r.AddContinuation(tc)
	f, err := os.Open("/Users/alexeyakhunov/mygit/go-ethereum/geth_read.log")
	check(err)
	defer f.Close()
	r.PrepareResolveParams()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		terms := strings.Split(scanner.Text(), " ")
		idx, _ := strconv.Atoi(terms[0])
		key := common.FromHex(terms[1])
		if len(key) == 0 {
			key = nil
		}
		value := common.FromHex(terms[2])
		if len(value) == 0 {
			value = nil
		}
		_, err := r.Walker(idx, key, value)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		count++
	}
	fmt.Printf("%d lines scanned\n", count)
}

func rlpIndices() {
	keybuf := new(bytes.Buffer)
	for i := 0; i < 512; i ++ {
		keybuf.Reset()
		rlp.Encode(keybuf, uint(i))
		fmt.Printf("Encoding of %d is %x\n", i, keybuf.Bytes())
	}
}

func printFullNodeRLPs() {
	trie.FullNode1()
	trie.FullNode2()
	trie.FullNode3()
	trie.FullNode4()
	trie.ShortNode1()
	trie.ShortNode2()
	trie.Hash1()
	trie.Hash2()
	trie.Hash3()
	trie.Hash4()
	trie.Hash5()
	trie.Hash6()
	trie.Hash7()
}

func testDifficulty() {
	genesisBlock, _, _, err := core.DefaultGenesisBlock().ToBlock(nil)
	check(err)
	d1 := ethash.CalcDifficulty(params.MainnetChainConfig, 100000, genesisBlock.Header())
	fmt.Printf("Block 1 difficulty: %d\n", d1)
}

func testRewindTests() {
	fmt.Printf("1 bucket\n")
	ethdb.TestRewindData1Bucket()
	fmt.Printf("2 buckets\n")
	ethdb.TestRewindData2Bucket()
}

func testBlockHashes() {
	ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	hash := rawdb.ReadCanonicalHash(ethDb, 823144)
	fmt.Printf("Canonical hash: %x\n", hash)
	header := rawdb.ReadHeader(ethDb, hash, 823144)
	fmt.Printf("Header.TxHash: %x\n", header.TxHash)
	fmt.Printf("Header.UncleHash: %x\n", header.UncleHash)
}

func printTxHashes() {
	ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	for b := uint64(0); b < uint64(100000); b++ {
		hash := rawdb.ReadCanonicalHash(ethDb, b)
		block := rawdb.ReadBlock(ethDb, hash, b)
		if block == nil {
			break
		}
		for _, tx := range block.Transactions() {
			fmt.Printf("%x\n", tx.Hash())
		}
	}
}

func relayoutKeys() {
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
 	check(err)
 	defer db.Close()
 	var count int
 	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("SUFFIX"))
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
 	fmt.Printf("Records: %d\n", count)
}

func upgradeBlocks() {
	//ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	start := []byte{}
	var keys [][]byte
	if err := ethDb.Walk([]byte("b"), start, 0, func (k, v []byte) ([]byte, ethdb.WalkAction, error) {
		if len(keys) % 1000 == 0 {
			fmt.Printf("Collected keys: %d\n", len(keys))
		}
		keys = append(keys, common.CopyBytes(k))
		return nil, ethdb.WalkActionNext, nil
	}); err != nil {
		panic(err)
	}
	for i, key := range keys {
		v, err := ethDb.Get([]byte("b"), key)
		if err != nil {
			panic(err)
		}
		smallBody := new(types.SmallBody) // To be changed to SmallBody
		if err := rlp.Decode(bytes.NewReader(v), smallBody); err != nil {
			panic(err)
		}
		body := new(types.Body)
		blockNum := binary.BigEndian.Uint64(key[:8])
		signer := types.MakeSigner(params.MainnetChainConfig, big.NewInt(int64(blockNum)))
		body.Senders = make([]common.Address, len(smallBody.Transactions))
		for j, tx := range smallBody.Transactions {
			addr, err := signer.Sender(tx)
			if err != nil {
				panic(err)
			}
			body.Senders[j] = addr
		}
		body.Transactions = smallBody.Transactions
		body.Uncles = smallBody.Uncles
		newV, err := rlp.EncodeToBytes(body)
		if err != nil {
			panic(err)
		}
		ethDb.Put([]byte("b"), key, newV)
		if i % 1000 == 0 {
			fmt.Printf("Upgraded keys: %d\n", i)
		}
	}
	check(ethDb.DeleteBucket([]byte("r")))
}

func removeReceipts() {

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
	//db, err := bolt.Open("/home/akhounov/dbcopy/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
 	//check(err)
 	//defer db.Close()
 	//bucketStats(db)
 	//mychart()
 	//testRebuild()
 	testRewind()
 	//hashFile()
 	//buildHashFromFile()
 	//testResolve()
 	//rlpIndices()
 	//printFullNodeRLPs()
 	//testStartup()
 	//testDifficulty()
 	//testRewindTests()
 	//if *reset != -1 {
 	//	testReset(uint64(*reset))
 	//}
 	//testBlockHashes()
 	//printBuckets(db)
 	//printTxHashes()
 	//relayoutKeys()
 	//testRedis()
 	//upgradeBlocks()
}

