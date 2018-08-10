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

type CommonResponse struct {
	Version   string `json:"jsonrpc"`
	RequestId int    `json:"id"`
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
	From common.Address `json:"from"`
	To   *common.Address `json:"to"` // Pointer because it might be missing
	Hash string          `json:"hash"`
}

type EthBlockByNumberResult struct {
	Difficulty   string           `json:"difficulty"`
	Miner        common.Address  `json:"miner"`
	Transactions []EthTransaction `json:"transactions"`
}

type EthBlockByNumber struct {
	CommonResponse
	Result EthBlockByNumberResult `json:"result"`
}

type StructLog struct {
	Depth int `json:"depth"`
}

type EthTxTraceResult struct {
	Gas         uint64      `json:"gas"`
	ReturnValue string      `json:"returnValue"`
	StructLogs  []StructLog `json:"structLogs"`
}

type EthTxTrace struct {
	CommonResponse
	Result EthTxTraceResult `json:"result"`
}

func post(client *http.Client, request string, response interface{}) error {
	r, err := client.Post("http://localhost:8545", "application/json", strings.NewReader(request))
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

func print(r *http.Response) {
	buf := make([]byte, r.ContentLength)
	l, err := r.Body.Read(buf)
	if err != nil && err != io.EOF {
		fmt.Printf("Could not read response: %v\n", err)
	}
	if l < len(buf) {
		fmt.Printf("Could not read response: %d out of %d\n", l, len(buf))
	}
	fmt.Printf("%s\n", buf)
}

func main() {
	fmt.Printf("Hello, world!\n")
	var client = &http.Client{
		Timeout: time.Second * 120,
	}
	request := `
{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":83}
`
	var blockNumber EthBlockNumber
	if err := post(client, request, &blockNumber); err != nil {
		fmt.Printf("Could not get block number: %v\n", err)
		return
	}
	lastBlock := blockNumber.Number.ToInt().Int64()
	fmt.Printf("Last block: %d\n", lastBlock)
	accounts := make(map[common.Address]struct{})
	for bn := 444351; bn <= int(lastBlock); bn++ {
		template := `
{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x", true],"id":83}
`
		var b EthBlockByNumber
		if err := post(client, fmt.Sprintf(template, bn), &b); err != nil {
			fmt.Printf("Could not retrieve block %d: %v\n", bn, err)
			return
		}
		accounts[b.Result.Miner] = struct{}{}
		for _, tx := range b.Result.Transactions {
			accounts[tx.From] = struct{}{}
			if tx.To != nil {
				accounts[*tx.To] = struct{}{}
			}
			template =`
{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["%s"],"id":83}
`
			var trace EthTxTrace
			if err := post(client, fmt.Sprintf(template, tx.Hash), &trace); err != nil {
				fmt.Printf("Could not trace transaction %s: %v\n", tx.Hash, err)
				return
			}
			if len(trace.Result.StructLogs) > 0 {
				fmt.Printf("Trace length for %s: %d\n", tx.Hash, len(trace.Result.StructLogs))
			}
		}
		template = `
{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x%x", "0x%x"],"id":83}
`
		var balance EthBalance
		if err := post(client, fmt.Sprintf(template, b.Result.Miner, bn), &balance); err != nil {
			fmt.Printf("Could not get account balance: %v\n", err)
			return
		}
		//fmt.Printf("Miner's balance: %s\n", balance.Balance.ToInt().Text(10))
		fmt.Printf("Done block %d, unique accounts: %d\n", bn, len(accounts))
	}
}
