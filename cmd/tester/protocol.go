package main

import (
	"fmt"
	"io"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/core/types"
)

type statusData struct {
	ProtocolVersion uint32
	NetworkId       uint64
	TD              *big.Int
	CurrentBlock    common.Hash
	GenesisBlock    common.Hash
}

type TesterProtocol struct {
	protocolVersion     uint32
	networkId           uint64
	genesisBlockHash    common.Hash
	blockAccessor       *BlockAccessor
}

func (tp *TesterProtocol) protocolRun (peer *p2p.Peer, rw p2p.MsgReadWriter) error {
	fmt.Printf("Ethereum peer connected: %s\n", peer.Name())
	// Synchronous "eth" handshake
	err := p2p.Send(rw, eth.StatusMsg, &statusData{
			ProtocolVersion: tp.protocolVersion,
			NetworkId:       tp.networkId,
			TD:              tp.blockAccessor.totalDifficulty,
			CurrentBlock:    tp.blockAccessor.lastBlock.Hash(),
			GenesisBlock:    tp.genesisBlockHash,
	})
	if err != nil {
		fmt.Printf("Failed to send status message to peer: %v\n", err)
		return err
	}
	msg, err := rw.ReadMsg()
	if err != nil {
		fmt.Printf("Failed to recevied state message from peer: %v\n", err)
		return err
	}
	if msg.Code != eth.StatusMsg {
		fmt.Printf("first msg has code %x (!= %x)\n", msg.Code, eth.StatusMsg)
		return fmt.Errorf("first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		fmt.Printf("message too large %v > %v", msg.Size, eth.ProtocolMaxMsgSize)
		return fmt.Errorf("message too large %v > %v", msg.Size, eth.ProtocolMaxMsgSize)
	}
	var statusResp statusData
	if err := msg.Decode(&statusResp); err != nil {
		fmt.Printf("Failed to decode msg %v: %v\n", msg, err)
		return fmt.Errorf("Failed to decode msg %v: %v\n", msg, err)
	}
	if statusResp.GenesisBlock != tp.genesisBlockHash {
		fmt.Printf("Mismatched genesis block hash %x (!= %x)", statusResp.GenesisBlock[:8], tp.genesisBlockHash[:8])
		return fmt.Errorf("Mismatched genesis block hash %x (!= %x)", statusResp.GenesisBlock[:8], tp.genesisBlockHash[:8])
	}
	if statusResp.NetworkId != tp.networkId {
		fmt.Printf("Mismatched network id %d (!= %d)", statusResp.NetworkId, tp.networkId)
		return fmt.Errorf("Mismatched network id %d (!= %d)", statusResp.NetworkId, tp.networkId)
	}
	if statusResp.ProtocolVersion != tp.protocolVersion {
		fmt.Printf("Mismached protocol version %d (!= %d)", statusResp.ProtocolVersion, tp.protocolVersion)
		return fmt.Errorf("Mismatched protocol version %d (!= %d)", statusResp.ProtocolVersion, tp.protocolVersion)
	}
	fmt.Printf("eth handshake complete, block hash: %x, block difficulty: %s\n", statusResp.CurrentBlock, statusResp.TD)

	for i := 0; i < 10000; i++ {
		fmt.Printf("Message loop i %d\n", i)
		// Read the next message
		msg, err = rw.ReadMsg()
		if err != nil {
			fmt.Printf("Failed to recevied state message from peer: %v\n", err)
			return err
		}
		switch {
		case msg.Code == eth.GetBlockHeadersMsg:
			if err = tp.handleGetBlockHeaderMsg(msg, rw); err != nil {
				return err
			}
		case msg.Code == eth.GetBlockBodiesMsg:
			if err = tp.handleGetBlockBodiesMsg(msg, rw); err != nil {
				return err
			}
		default:
			fmt.Printf("Next message: %v\n", msg)
		}
	}
	return nil
}

// hashOrNumber is a combined field for specifying an origin block.
type hashOrNumber struct {
	Hash   common.Hash // Block hash from which to retrieve headers (excludes Number)
	Number uint64      // Block hash from which to retrieve headers (excludes Hash)
}

// getBlockHeadersData represents a block header query.
type getBlockHeadersData struct {
	Origin  hashOrNumber // Block from which to retrieve headers
	Amount  uint64       // Maximum number of headers to retrieve
	Skip    uint64       // Blocks to skip between consecutive headers
	Reverse bool         // Query direction (false = rising towards latest, true = falling towards genesis)
}

// EncodeRLP is a specialized encoder for hashOrNumber to encode only one of the
// two contained union fields.
func (hn *hashOrNumber) EncodeRLP(w io.Writer) error {
	if hn.Hash == (common.Hash{}) {
		return rlp.Encode(w, hn.Number)
	}
	if hn.Number != 0 {
		return fmt.Errorf("both origin hash (%x) and number (%d) provided", hn.Hash, hn.Number)
	}
	return rlp.Encode(w, hn.Hash)
}

// DecodeRLP is a specialized decoder for hashOrNumber to decode the contents
// into either a block hash or a block number.
func (hn *hashOrNumber) DecodeRLP(s *rlp.Stream) error {
	_, size, _ := s.Kind()
	origin, err := s.Raw()
	if err == nil {
		switch {
		case size == 32:
			err = rlp.DecodeBytes(origin, &hn.Hash)
		case size <= 8:
			err = rlp.DecodeBytes(origin, &hn.Number)
		default:
			err = fmt.Errorf("invalid input size %d for origin", size)
		}
	}
	return err
}

func (tp *TesterProtocol) handleGetBlockHeaderMsg(msg p2p.Msg, rw p2p.MsgReadWriter) error {
	var query getBlockHeadersData
	if err := msg.Decode(&query); err != nil {
		fmt.Printf("Failed to decode msg %v: %v\n", msg, err)
		return fmt.Errorf("Failed to decode msg %v: %v\n", msg, err)
	}
	fmt.Printf("GetBlockHeadersMsg: %v\n", query)
	headers := []*types.Header{}
	if query.Origin.Hash == (common.Hash{}) && !query.Reverse {
		number := query.Origin.Number
		for i := 0; i < int(query.Amount); i++ {
			if header := tp.blockAccessor.GetHeaderByNumber(number); header != nil {
				//fmt.Printf("Going to send block %d\n", header.Number.Uint64())
				headers = append(headers, header)
			}
			number += query.Skip+1
		}
	}
	if query.Origin.Hash != (common.Hash{}) && query.Amount == 1 && query.Skip == 0 && !query.Reverse {
		if header:= tp.blockAccessor.GetHeaderByHash(query.Origin.Hash); header != nil {
			fmt.Printf("Going to send block %d\n", header.Number.Uint64())
			headers = append(headers, header)
		}
	}
	if len(headers) > 0 {
		if err := p2p.Send(rw, eth.BlockHeadersMsg, headers); err != nil {
			fmt.Printf("Failed to send headers: %v\n", err)
			return err
		}
		fmt.Printf("Sent %d headers\n", len(headers))
	}
	return nil
}

func (tp *TesterProtocol) handleGetBlockBodiesMsg(msg p2p.Msg, rw p2p.MsgReadWriter) error {
	msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
	fmt.Printf("GetBlockBodiesMsg with size %d\n", msg.Size)
	if _, err := msgStream.List(); err != nil {
		return err
	}
	// Gather blocks until the fetch or network limits is reached
	var (
		hash   common.Hash
		bodies []rlp.RawValue
	)
	for {
		// Retrieve the hash of the next block
		if err := msgStream.Decode(&hash); err == rlp.EOL {
			break
		} else if err != nil {
			fmt.Printf("Failed to decode msg %v: %v", msg, err)
			return fmt.Errorf("Failed to decode msg %v: %v", msg, err)
		}
		// Retrieve the requested block body, stopping if enough was found
		if block, err := tp.blockAccessor.GetBlockByHash(hash); err != nil {
			fmt.Printf("Failed to read block %v", err)
			return fmt.Errorf("Failed to read block %v", err)
		} else if block != nil {
			data, err := rlp.EncodeToBytes(block.Body())
			if err != nil {
				fmt.Printf("Failed to encode body: %v", err)
				return fmt.Errorf("Failed to encode body: %v", err)
			}
			bodies = append(bodies, data)
		}
	}
	p2p.Send(rw, eth.BlockBodiesMsg, bodies)
	fmt.Printf("Sent %d bodies\n", len(bodies))
	return nil
}

func (tp *TesterProtocol) sendLastBlock(rw p2p.MsgReadWriter) error {
	return p2p.Send(rw, eth.NewBlockMsg, []interface{}{tp.blockAccessor.lastBlock, tp.blockAccessor.totalDifficulty})
}
