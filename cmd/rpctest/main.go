package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type EthError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type CommonResponse struct {
	Version   string    `json:"jsonrpc"`
	RequestId int       `json:"id"`
	Error     *EthError `json:"error"`
}

type EthBlockNumber struct {
	CommonResponse
	Number hexutil.Big `json:"result"`
}

type EthBalance struct {
	CommonResponse
	Balance hexutil.Big `json:"result"`
}

type EthTransaction struct {
	From common.Address  `json:"from"`
	To   *common.Address `json:"to"` // Pointer because it might be missing
	Hash string          `json:"hash"`
}

type EthBlockByNumberResult struct {
	Difficulty   hexutil.Big      `json:"difficulty"`
	Miner        common.Address   `json:"miner"`
	Transactions []EthTransaction `json:"transactions"`
	TxRoot       common.Hash      `json:"transactionsRoot"`
}

type EthBlockByNumber struct {
	CommonResponse
	Result EthBlockByNumberResult `json:"result"`
}

type StructLog struct {
	Op      string            `json:"op"`
	Pc      uint64            `json:"pc"`
	Depth   uint64            `json:"depth"`
	Error   *EthError         `json:"error"`
	Gas     uint64            `json:"gas"`
	GasCost uint64            `json:"gasCost"`
	Memory  []string          `json:"memory"`
	Stack   []string          `json:"stack"`
	Storage map[string]string `json:"storage"`
}

type EthTxTraceResult struct {
	Gas         uint64      `json:"gas"`
	Failed      bool        `json:"failed"`
	ReturnValue string      `json:"returnValue"`
	StructLogs  []StructLog `json:"structLogs"`
}

type EthTxTrace struct {
	CommonResponse
	Result EthTxTraceResult `json:"result"`
}

type DebugModifiedAccounts struct {
	CommonResponse
	Result []common.Address `json:"result"`
}

func post(client *http.Client, url, request string, response interface{}) error {
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		return err
	}
	if r.StatusCode != 200 {
		return fmt.Errorf("Status %s", r.Status)
	}
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	return decoder.Decode(response)
}

func print(client *http.Client, url, request string) {
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		fmt.Printf("Could not print: %v\n", err)
		return
	}
	if r.StatusCode != 200 {
		fmt.Printf("Status %s", r.Status)
		return
	}
	fmt.Printf("ContentLength: %d\n", r.ContentLength)
	buf := make([]byte, 20000)
	l, err := r.Body.Read(buf)
	if err != nil && err != io.EOF {
		fmt.Printf("Could not read response: %v\n", err)
		return
	}
	if l < len(buf) {
		fmt.Printf("Could not read response: %d out of %d\n", l, len(buf))
		//return
	}
	fmt.Printf("%s\n", buf[:l])
}

func compareBlocks(b, bg *EthBlockByNumber) bool {
	r := b.Result
	rg := bg.Result
	if r.Difficulty.ToInt().Cmp(rg.Difficulty.ToInt()) != 0 {
		fmt.Printf("Difficulty difference %d %d\n", r.Difficulty.ToInt(), rg.Difficulty.ToInt())
		return false
	}
	if r.Miner != rg.Miner {
		fmt.Printf("Miner different %x %x\n", r.Miner, rg.Miner)
		return false
	}
	if len(r.Transactions) != len(rg.Transactions) {
		fmt.Printf("Num of txs different: %d %d\n", len(r.Transactions), len(rg.Transactions))
		return false
	}
	for i, tx := range r.Transactions {
		txg := rg.Transactions[i]
		if tx.From != txg.From {
			fmt.Printf("Tx %d different From: %x %x\n", i, tx.From, txg.From)
			return false
		}
		if (tx.To == nil && txg.To != nil) || (tx.To != nil && txg.To == nil) {
			fmt.Printf("Tx %d different To nilness: %t %t\n", i, (tx.To==nil), (txg.To==nil))
			return false
		}
		if tx.To != nil && txg.To != nil && *tx.To != *txg.To {
			fmt.Printf("Tx %d different To: %x %x\n", i, *tx.To, *txg.To)
			return false
		}
		if tx.Hash != txg.Hash {
			fmt.Printf("Tx %x different Hash: %s %s\n", i, tx.Hash, txg.Hash)
			return false
		}
	}
	return true
}

func compareTraces(trace, traceg *EthTxTrace) bool {
	r := trace.Result
	rg := traceg.Result
	if r.Gas != rg.Gas {
		fmt.Printf("Trace different Gas: %d %d\n", r.Gas, rg.Gas)
		return false
	}
	if r.Failed != rg.Failed {
		fmt.Printf("Trace different Failed: %t %t\n", r.Failed, rg.Failed)
		return false
	}
	if r.ReturnValue != rg.ReturnValue {
		fmt.Printf("Trace different ReturnValue: %s %s\n", r.ReturnValue, rg.ReturnValue)
		return false
	}
	if len(r.StructLogs) != len(rg.StructLogs) {
		fmt.Printf("Trace different length: %d %d\n", len(r.StructLogs), len(rg.StructLogs))
		return false
	}
	for i, l := range r.StructLogs {
		lg := rg.StructLogs[i]
		if l.Op != lg.Op {
			fmt.Printf("Trace different Op: %d %s %s\n", i, l.Op, lg.Op)
			return false
		}
		if l.Pc != lg.Pc {
			fmt.Printf("Trace different Pc: %d %d %d\n", i, l.Pc, lg.Pc)
			return false
		}
	}
	return true
}

func compareBalances(balance, balanceg *EthBalance) bool {
	if balance.Balance.ToInt().Cmp(balanceg.Balance.ToInt()) != 0 {
		fmt.Printf("Different balance: %d %d\n", balance.Balance.ToInt(), balanceg.Balance.ToInt())
		return false
	}
	return true
}

func compareModifiedAccounts(ma, mag *DebugModifiedAccounts) bool {
	r := ma.Result
	rg := mag.Result
	rset := make(map[common.Address]struct{})
	rsetg := make(map[common.Address]struct{})
	for _, a := range r {
		rset[a] = struct{}{}
	}
	for _, a := range rg {
		rsetg[a] = struct{}{}
	}
	for _, a := range r {
		if _, ok := rsetg[a]; !ok {
			fmt.Printf("%x not present in rg\n", a)
		}
	}
	for _, a := range rg {
		if _, ok := rset[a]; !ok {
			fmt.Printf("%x not present in r\n", a)
		}
	}
	return true
}

func bench1() {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	geth_url := "http://192.168.1.96:8545"
	//geth_url := "http://localhost:8545"
	turbogeth_url := "http://localhost:8545"
	request := `
{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":83}
`
	var blockNumber EthBlockNumber
	if err := post(client, turbogeth_url, request, &blockNumber); err != nil {
		fmt.Printf("Could not get block number: %v\n", err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	lastBlock := blockNumber.Number.ToInt().Int64()
	fmt.Printf("Last block: %d\n", lastBlock)
	accounts := make(map[common.Address]struct{})
	firstBn := 1000000-2
	prevBn := firstBn
	for bn := firstBn; bn <= int(lastBlock); bn++ {
		template := `
{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x", true],"id":83}
`
		var b EthBlockByNumber
		if err := post(client, turbogeth_url, fmt.Sprintf(template, bn), &b); err != nil {
			fmt.Printf("Could not retrieve block %d: %v\n", bn, err)
			return
		}
		if b.Error != nil {
			fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
		}
		var bg EthBlockByNumber
		if err := post(client, geth_url, fmt.Sprintf(template, bn), &bg); err != nil {
			fmt.Printf("Could not retrieve block g %d: %v\n", bn, err)
			return
		}
		if bg.Error != nil {
			fmt.Printf("Error retrieving block g: %d %s\n", bg.Error.Code, bg.Error.Message)
		}
		if !compareBlocks(&b, &bg) {
			fmt.Printf("Block difference for %d\n", bn)
			return
		}
		accounts[b.Result.Miner] = struct{}{}
		for _, tx := range b.Result.Transactions {
			accounts[tx.From] = struct{}{}
			if tx.To != nil {
				accounts[*tx.To] = struct{}{}
			}
			/*
			template =`
{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["%s"],"id":83}
`
			var trace EthTxTrace
			if err := post(client, turbogeth_url, fmt.Sprintf(template, tx.Hash), &trace); err != nil {
				fmt.Printf("Could not trace transaction %s: %v\n", tx.Hash, err)
				print(client, turbogeth_url, fmt.Sprintf(template, tx.Hash))
				return
			}
			if trace.Error != nil {
				fmt.Printf("Error tracing transaction: %d %s\n", trace.Error.Code, trace.Error.Message)
			}
			var traceg EthTxTrace
			if err := post(client, geth_url, fmt.Sprintf(template, tx.Hash), &traceg); err != nil {
				fmt.Printf("Could not trace transaction g %s: %v\n", tx.Hash, err)
				print(client, geth_url, fmt.Sprintf(template, tx.Hash))
				return
			}
			if traceg.Error != nil {
				fmt.Printf("Error tracing transaction g: %d %s\n", traceg.Error.Code, traceg.Error.Message)
				return
			}
			if !compareTraces(&trace, &traceg) {
				fmt.Printf("Different traces block %d, tx %s\n", bn, tx.Hash)
				return
			}
			*/
		}
		template = `
{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x%x", "0x%x"],"id":83}
`
		var balance EthBalance
		if err := post(client, turbogeth_url, fmt.Sprintf(template, b.Result.Miner, bn), &balance); err != nil {
			fmt.Printf("Could not get account balance: %v\n", err)
			return
		}
		if balance.Error != nil {
			fmt.Printf("Error getting account balance: %d %s", balance.Error.Code, balance.Error.Message)
			return
		}
		var balanceg EthBalance
		if err := post(client, geth_url, fmt.Sprintf(template, b.Result.Miner, bn), &balanceg); err != nil {
			fmt.Printf("Could not get account balance g: %v\n", err)
			return
		}
		if balanceg.Error != nil {
			fmt.Printf("Error getting account balance g: %d %s\n", balanceg.Error.Code, balanceg.Error.Message)
			return
		}
		if !compareBalances(&balance, &balanceg) {
			fmt.Printf("Miner %x balance difference for block %d\n", b.Result.Miner, bn)
			return
		}
		if prevBn < bn && bn % 1000 == 0 {
			// Checking modified accounts
			template = `
{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":[%d, %d],"id":83}
`
			var ma DebugModifiedAccounts
			if err := post(client, turbogeth_url, fmt.Sprintf(template, prevBn, bn), &ma); err != nil {
				fmt.Printf("Could not get modified accounts: %v\n", err)
				return
			}
			if ma.Error != nil {
				fmt.Printf("Error getting modified accounts: %d %d\n", ma.Error.Code, ma.Error.Message)
				return
			}
			var mag DebugModifiedAccounts
			if err := post(client, geth_url, fmt.Sprintf(template, prevBn, bn), &mag); err != nil {
				fmt.Printf("Could not get modified accounts g: %v\n", err)
				return
			}
			if mag.Error != nil {
				fmt.Printf("Error getting modified accounts g: %d %d\n", mag.Error.Code, mag.Error.Message)
				return
			}
			if !compareModifiedAccounts(&ma, &mag) {
				fmt.Printf("Modified accouts different for blocks %d-%d\n", prevBn, bn)
				return
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d (%d)\n", prevBn, bn, len(ma.Result), len(mag.Result))
			prevBn = bn
		}
	}
}

func main() {
	bench1()
}
