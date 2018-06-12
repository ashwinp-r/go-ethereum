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
	lastBlockDifficulty *big.Int
	lastBlockHash       common.Hash
	genesisBlockHash    common.Hash
	headersByHash       map[common.Hash]*types.Header
	headersByNumber     map[uint64]*types.Header
}

func (tp *TesterProtocol) protocolRun (peer *p2p.Peer, rw p2p.MsgReadWriter) error {
	fmt.Printf("Ethereum peer connected: %s\n", peer.Name())
	// Synchronous "eth" handshake
	err := p2p.Send(rw, eth.StatusMsg, &statusData{
			ProtocolVersion: tp.protocolVersion,
			NetworkId:       tp.networkId,
			TD:              tp.lastBlockDifficulty,
			CurrentBlock:    tp.lastBlockHash,
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
		fmt.Printf("Mismatched protocol version %d (!= %d)", statusResp.ProtocolVersion, tp.protocolVersion)
		return fmt.Errorf("Mismatched protocol version %d (!= %d)", statusResp.ProtocolVersion, tp.protocolVersion)
	}
	fmt.Printf("eth handshake complete, block hash: %x, block difficulty: %s\n", statusResp.CurrentBlock, statusResp.TD)

	for i := 0; i < 2; i++ {
		fmt.Printf("Message loop i %d\n", i)
		// Read the next message
		msg, err = rw.ReadMsg()
		if err != nil {
			fmt.Printf("Failed to recevied state message from peer: %v\n", err)
			return err
		}
		switch {
		case msg.Code == eth.GetBlockHeadersMsg:
			return tp.handleGetBlockHeaderMsg(msg, rw)
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
	if query.Origin.Hash == (common.Hash{}) && query.Amount == 1 && query.Skip == 0 && !query.Reverse {
		if header, ok := tp.headersByNumber[query.Origin.Number]; ok {
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
