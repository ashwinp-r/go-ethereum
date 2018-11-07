package main

import (
	"bytes"
	//"encoding/binary"
	"math/big"
	"fmt"
	"strings"
	"strconv"
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"io/ioutil"
	"bufio"
	"sort"
	"time"
	"syscall"
	"os/signal"
	"io"

	"github.com/boltdb/bolt"
	"github.com/wcharczuk/go-chart"
	util "github.com/wcharczuk/go-chart/util"
	"github.com/wcharczuk/go-chart/drawing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	//"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
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

func decodeTimestamp(suffix []byte) (uint64, []byte) {
	bytecount := int(suffix[0] >> 5)
	timestamp := uint64(suffix[0] & 0x1f)
	for i := 1; i < bytecount; i++ {
		timestamp = (timestamp << 8) | uint64(suffix[i])
	}
	return timestamp, suffix[bytecount:]
}

// Implements sort.Interface
type TimeSorterInt struct {
	length     int
	timestamps []uint64
	values     []int
}

func NewTimeSorterInt(length int) TimeSorterInt {
	return TimeSorterInt{
		length:     length,
		timestamps: make([]uint64, length),
		values:     make([]int, length),
	}
}

func (tsi TimeSorterInt) Len() int {
	return tsi.length
}

func (tsi TimeSorterInt) Less(i, j int) bool {
	return tsi.timestamps[i] < tsi.timestamps[j]
}

func (tsi TimeSorterInt) Swap(i, j int) {
	tsi.timestamps[i], tsi.timestamps[j] = tsi.timestamps[j], tsi.timestamps[i]
	tsi.values[i], tsi.values[j] = tsi.values[j], tsi.values[i]
}

type IntSorterAddr struct {
	length int
	ints   []int
	values []common.Address
}

func NewIntSorterAddr(length int) IntSorterAddr {
	return IntSorterAddr{
		length: length,
		ints: make([]int, length),
		values: make([]common.Address, length),
	}
}

func (isa IntSorterAddr) Len() int {
	return isa.length
}

func (isa IntSorterAddr) Less(i, j int) bool {
	return isa.ints[i] > isa.ints[j]
}

func (isa IntSorterAddr) Swap(i, j int) {
	isa.ints[i], isa.ints[j] = isa.ints[j], isa.ints[i]
	isa.values[i], isa.values[j] = isa.values[j], isa.values[i]
}

func stateGrowth1() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	var count int
	var maxTimestamp uint64
	// For each address hash, when was it last accounted
	lastTimestamps := make(map[common.Hash]uint64)
	// For each timestamp, how many accounts were created in the state
	creationsByBlock := make(map[uint64]int)
	var addrHash common.Hash
	// Go through the history of account first
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.AccountsHistoryBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// First 32 bytes is the hash of the address, then timestamp encoding
			copy(addrHash[:], k[:32])
			timestamp, _ := decodeTimestamp(k[32:])
			if timestamp+1 > maxTimestamp {
				maxTimestamp = timestamp + 1
			}
			if len(v) == 0 {
				creationsByBlock[timestamp]++
				if lt, ok := lastTimestamps[addrHash]; ok {
					creationsByBlock[lt]--
				}
			}
			lastTimestamps[addrHash] = timestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d account records\n", count)
			}
		}
		return nil
	})
	check(err)
	// Go through the current state
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.AccountsBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			// First 32 bytes is the hash of the address
			copy(addrHash[:], k[:32])
			lastTimestamps[addrHash] = maxTimestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d account records\n", count)
			}
		}
		return nil
	})
	check(err)
	for _, lt := range lastTimestamps {
		if lt < maxTimestamp {
			creationsByBlock[lt]--
		}
	}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Account history records: %d\n", count)
	fmt.Printf("Creating dataset...\n")
	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(creationsByBlock))
	idx := 0
	for timestamp, count := range creationsByBlock {
		tsi.timestamps[idx] = timestamp
		tsi.values[idx] = count
		idx++
	}
	sort.Sort(tsi)
	fmt.Printf("Writing dataset...")
	f, err := os.Create("accounts_growth.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < tsi.length; i++ {
		cumulative += tsi.values[i]
		fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
	}
}

func stateGrowth2() {
	startTime := time.Now()
	//db, err := bolt.Open("/home/akhounov/.ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	//db, err := bolt.Open("/Volumes/tb4/turbo-geth/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	var count int
	var maxTimestamp uint64
	// For each address hash, when was it last accounted
	lastTimestamps := make(map[common.Address]map[common.Hash]uint64)
	// For each timestamp, how many accounts were created in the state
	creationsByBlock := make(map[common.Address]map[uint64]int)
	var address common.Address
	var hash common.Hash
	// Go through the history of account first
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.StorageHistoryBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// First 20 bytes is the address
			copy(address[:], k[:20])
			copy(hash[:], k[20:52])
			timestamp, _ := decodeTimestamp(k[52:])
			if timestamp+1 > maxTimestamp {
				maxTimestamp = timestamp + 1
			}
			if len(v) == 0 {
				c, ok := creationsByBlock[address]
				if !ok {
					c = make(map[uint64]int)
					creationsByBlock[address] = c
				}
				c[timestamp]++
				l, ok := lastTimestamps[address]
				if !ok {
					l = make(map[common.Hash]uint64)
					lastTimestamps[address] = l
				}
				if lt, ok := l[hash]; ok {
					c[lt]--
				}
			}
			l, ok := lastTimestamps[address]
			if !ok {
				l = make(map[common.Hash]uint64)
				lastTimestamps[address] = l
			}
			l[hash] = timestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records\n", count)
			}
		}
		return nil
	})
	check(err)
	// Go through the current state
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.StorageBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(address[:], k[:20])
			copy(hash[:], k[20:52])
			l, ok := lastTimestamps[address]
			if !ok {
				l = make(map[common.Hash]uint64)
				lastTimestamps[address] = l
			}
			l[hash] = maxTimestamp
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records\n", count)
			}
		}
		return nil
	})
	check(err)
	for address, l := range lastTimestamps {
		for _, lt := range l {
			if lt < maxTimestamp {
				creationsByBlock[address][lt]--
			}
		}
	}

	fmt.Printf("Processing took %s\n", time.Since(startTime))
	fmt.Printf("Storage history records: %d\n", count)
	fmt.Printf("Creating dataset...\n")
	totalCreationsByBlock := make(map[uint64]int)
	isa := NewIntSorterAddr(len(creationsByBlock))
	idx := 0
	for addr, c := range creationsByBlock {
		cumulative := 0
		for timestamp, count := range c {
			totalCreationsByBlock[timestamp] += count
			cumulative += count
		}
		isa.ints[idx] = cumulative
		isa.values[idx] = addr
		idx++
	}
	sort.Sort(isa)
	// Sort accounts by timestamp
	tsi := NewTimeSorterInt(len(totalCreationsByBlock))
	idx = 0
	for timestamp, count := range totalCreationsByBlock {
		tsi.timestamps[idx] = timestamp
		tsi.values[idx] = count
		idx++
	}
	sort.Sort(tsi)
	fmt.Printf("Writing dataset...\n")
	f, err := os.Create("storage_growth.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	cumulative := 0
	for i := 0; i < tsi.length; i++ {
		cumulative += tsi.values[i]
		fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
	}
	// Top 16 contracts
	for i := 0; i < 16 && i < isa.length; i++ {
		addr := isa.values[i]
		tsi := NewTimeSorterInt(len(creationsByBlock[addr]))
		idx := 0
		for timestamp, count := range creationsByBlock[addr] {
			tsi.timestamps[idx] = timestamp
			tsi.values[idx] = count
			idx++
		}
		sort.Sort(tsi)
		fmt.Printf("Writing dataset for contract %x...\n", addr[:])
		f, err := os.Create(fmt.Sprintf("growth_%x.csv", addr[:]))
		check(err)
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()
		cumulative := 0
		for i := 0; i < tsi.length; i++ {
			cumulative += tsi.values[i]
			fmt.Fprintf(w, "%d, %d\n", tsi.timestamps[i], cumulative)
		}
	}
}

func parseFloat64(str string) float64 {
	v, _ := strconv.ParseFloat(str, 64)
	return v
}

func readData(filename string) (blocks []float64, items []float64, err error) {
	err = util.File.ReadByLines(filename, func(line string) error {
		parts := strings.Split(line, ",")
		blocks = append(blocks, parseFloat64(strings.Trim(parts[0], " "))/1000000.0)
		items = append(items, parseFloat64(strings.Trim(parts[1], " "))/1000000.0)
		return nil
	})
	return
}

func blockMillions() []chart.GridLine {
	return []chart.GridLine{
		{Value: 1.0},
		{Value: 2.0},
		{Value: 3.0},
		{Value: 4.0},
		{Value: 5.0},
		{Value: 6.0},
	}
}

func accountMillions() []chart.GridLine {
	return []chart.GridLine{
		{Value: 5.0},
		{Value: 10.0},
		{Value: 15.0},
		{Value: 20.0},
		{Value: 25.0},
		{Value: 30.0},
		{Value: 35.0},
		{Value: 40.0},
		{Value: 45.0},
	}
}

func stateGrowthChart1() {
	blocks, accounts, err := readData("accounts_growth.csv")
	check(err)
	mainSeries := &chart.ContinuousSeries{
		Name: "Number of accounts (EOA and contracts)",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: blocks,
		YValues: accounts,
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
			Name:      "Accounts",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: accountMillions(),
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
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			mainSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("accounts_growth.png", buffer.Bytes(), 0644)
    check(err)
}

func storageMillions() []chart.GridLine {
	return []chart.GridLine{
		{Value: 20.0},
		{Value: 40.0},
		{Value: 60.0},
		{Value: 80.0},
		{Value: 100.0},
		{Value: 120.0},
		{Value: 140.0},
	}
}

func stateGrowthChart2() {
	blocks, accounts, err := readData("storage_growth.csv")
	check(err)
	mainSeries := &chart.ContinuousSeries{
		Name: "Number of contract storage items",
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorGreen,
			FillColor:   chart.ColorGreen.WithAlpha(100),
		},
		XValues: blocks,
		YValues: accounts,
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
			Name:      "Storage items",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: storageMillions(),
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
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			mainSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("storage_growth.png", buffer.Bytes(), 0644)
    check(err)
}

func stateGrowthChart3() {
	files, err := ioutil.ReadDir("./")
	if err != nil {
		panic(err)
	}
	colors := []drawing.Color{
		chart.ColorRed,
		chart.ColorOrange,
		chart.ColorYellow,
		chart.ColorGreen,
		chart.ColorCyan,
		chart.ColorBlue,
		drawing.Color{R: 255, G: 0, B: 255, A: 255},
		chart.ColorBlack,
		drawing.Color{R: 165, G: 42, B: 42, A: 255},
	}
	seriesList := []chart.Series{}
	colorIdx := 0
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "growth_") && strings.HasSuffix(f.Name(), ".csv") {
			blocks, items, err := readData(f.Name())
			check(err)
			seriesList = append(seriesList, &chart.ContinuousSeries{
				Name: f.Name()[len("growth_"):len(f.Name())-len(".csv")],
				Style: chart.Style{
					StrokeWidth: float64(1+colorIdx/len(colors)),
					StrokeColor: colors[colorIdx%len(colors)],
					Show:        true,
				},
				XValues: blocks,
				YValues: items,
			})
			colorIdx++
		}
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
			Name:      "Storage items",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlue,
				StrokeWidth: 1.0,
			},
			GridLines: storageMillions(),
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
			GridLines: blockMillions(),
		},
		Series: seriesList,
	}

	graph1.Elements = []chart.Renderable{chart.LegendLeft(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("top_16_contracts.png", buffer.Bytes(), 0644)
    check(err)
}

type CreationTracer struct {
	w io.Writer
}

func NewCreationTracer(w io.Writer) CreationTracer {
	return CreationTracer{w: w}
}

func (ct CreationTracer) CaptureStart(from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (ct CreationTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct CreationTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct CreationTracer) CaptureEnd(output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (ct CreationTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	_, err := fmt.Fprintf(ct.w, "%x,%x\n", creation, creator)
	return err
}

func creators() {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	//ethDb, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/Volumes/tb4/turbo-geth/geth/chaindata")
	//ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	f, err := os.OpenFile("creators.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	ct := NewCreationTracer(w)
	chainConfig := params.MainnetChainConfig
	vmConfig := vm.Config{Tracer: ct, Debug: true}
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil)
	check(err)
	blockNum := uint64(*block)
	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewDbState(ethDb, block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		blockNum++
		if blockNum % 1000 == 0 {
			fmt.Printf("Processed %d blocks\n", blockNum)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Next time specify -block %d\n", blockNum)
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
	//stateGrowth1()
	//stateGrowthChart1()
	//stateGrowth2()
	//stateGrowthChart2()
	//stateGrowthChart3()
	creators()
}
