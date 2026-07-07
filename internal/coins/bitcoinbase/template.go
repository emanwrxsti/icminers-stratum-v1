// Package bitcoinbase contains coin-agnostic building blocks shared by every
// Bitcoin-family adapter: the getblocktemplate model, coinbase transaction
// construction, merkle branch/root math, nBits/target conversion, and
// mining.notify parameter assembly. Coin-specific adapters (internal/coins/btc,
// later rxd/scash/...) compose these pieces with their own hashing and address
// rules.
package bitcoinbase

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// TemplateTx is one transaction inside a getblocktemplate response.
type TemplateTx struct {
	Data   string `json:"data"`
	TxID   string `json:"txid"`
	Hash   string `json:"hash"`
	Fee    int64  `json:"fee"`
	Weight int64  `json:"weight"`
}

// Template models the fields of a Bitcoin Core getblocktemplate response that
// pool code needs. Unknown fields are ignored by json decoding, and the raw
// response is preserved by callers that need more.
type Template struct {
	Version           int32        `json:"version"`
	Rules             []string     `json:"rules"`
	PreviousBlockHash string       `json:"previousblockhash"`
	Transactions      []TemplateTx `json:"transactions"`
	CoinbaseValue     int64        `json:"coinbasevalue"`
	Target            string       `json:"target"`
	MinTime           int64        `json:"mintime"`
	CurTime           int64        `json:"curtime"`
	Bits              string       `json:"bits"`
	Height            int64        `json:"height"`
	// DefaultWitnessCommitment is the OP_RETURN script (hex) that must be added
	// as a coinbase output when the block contains segwit transactions.
	DefaultWitnessCommitment string `json:"default_witness_commitment"`
}

// ParseTemplate decodes a getblocktemplate JSON result and checks the fields
// the pool depends on.
func ParseTemplate(raw json.RawMessage) (*Template, error) {
	var t Template
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("parse getblocktemplate: %w", err)
	}
	if t.Height <= 0 {
		return nil, fmt.Errorf("getblocktemplate: missing/invalid height %d", t.Height)
	}
	if len(t.PreviousBlockHash) != 64 {
		return nil, fmt.Errorf("getblocktemplate: invalid previousblockhash %q", t.PreviousBlockHash)
	}
	if _, err := hex.DecodeString(t.PreviousBlockHash); err != nil {
		return nil, fmt.Errorf("getblocktemplate: previousblockhash not hex: %w", err)
	}
	if len(t.Bits) != 8 {
		return nil, fmt.Errorf("getblocktemplate: invalid bits %q", t.Bits)
	}
	if _, err := hex.DecodeString(t.Bits); err != nil {
		return nil, fmt.Errorf("getblocktemplate: bits not hex: %w", err)
	}
	if t.CurTime <= 0 {
		return nil, fmt.Errorf("getblocktemplate: invalid curtime %d", t.CurTime)
	}
	if t.CoinbaseValue <= 0 {
		return nil, fmt.Errorf("getblocktemplate: invalid coinbasevalue %d", t.CoinbaseValue)
	}
	for i, tx := range t.Transactions {
		if tx.TxID == "" && tx.Hash == "" {
			return nil, fmt.Errorf("getblocktemplate: transaction %d has no txid/hash", i)
		}
	}
	return &t, nil
}

// TxIDs returns the display-hex txids of the template transactions (txid,
// falling back to hash for pre-segwit daemons).
func (t *Template) TxIDs() []string {
	ids := make([]string, 0, len(t.Transactions))
	for _, tx := range t.Transactions {
		id := tx.TxID
		if id == "" {
			id = tx.Hash
		}
		ids = append(ids, id)
	}
	return ids
}
