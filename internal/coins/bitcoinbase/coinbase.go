package bitcoinbase

import (
	"encoding/binary"
	"fmt"
)

// CoinbaseParts is a coinbase transaction split at the extranonce placeholder,
// exactly as stratum requires: the full serialized coinbase is
//
//	coinb1 || extranonce1 || extranonce2 || coinb2
//
// The pool sends Coinb1/Coinb2 hex in mining.notify; miners insert their
// session extranonce1 and rolled extranonce2.
type CoinbaseParts struct {
	Coinb1 []byte
	Coinb2 []byte
	// ExtraNonceSize is the total placeholder size (en1 + en2 bytes).
	ExtraNonceSize int
}

// CoinbaseSpec describes how to build a coinbase for a template.
type CoinbaseSpec struct {
	Height        int64
	CoinbaseValue int64
	// PayoutScript is the pool wallet scriptPubKey receiving the reward.
	PayoutScript []byte
	// WitnessCommitmentScript is the OP_RETURN script from
	// default_witness_commitment; empty when the block has no segwit txs.
	WitnessCommitmentScript []byte
	// Tag is an arbitrary ASCII marker included in the coinbase scriptSig
	// (e.g. "/ICMINERS/"). May be empty.
	Tag string
	// ExtraNonce1Size and ExtraNonce2Size are the placeholder widths in bytes.
	ExtraNonce1Size int
	ExtraNonce2Size int
	// TxVersion is the coinbase transaction version (Bitcoin uses 1 or 2).
	TxVersion int32
}

// maxScriptSigLen is the consensus limit on coinbase scriptSig length.
const maxScriptSigLen = 100

// BuildCoinbase constructs a legacy-serialized (non-witness) coinbase
// transaction split into stratum coinb1/coinb2 parts.
//
// Note on serialization: mining.notify always carries the NON-witness
// serialization of the coinbase; the txid (which feeds the merkle root) is
// defined over that same serialization, so miners and pool agree byte-for-byte.
func BuildCoinbase(spec CoinbaseSpec) (*CoinbaseParts, error) {
	if spec.Height <= 0 {
		return nil, fmt.Errorf("coinbase: invalid height %d", spec.Height)
	}
	if spec.CoinbaseValue <= 0 {
		return nil, fmt.Errorf("coinbase: invalid coinbasevalue %d", spec.CoinbaseValue)
	}
	if len(spec.PayoutScript) == 0 {
		return nil, fmt.Errorf("coinbase: empty payout script")
	}
	if spec.ExtraNonce1Size <= 0 || spec.ExtraNonce2Size <= 0 {
		return nil, fmt.Errorf("coinbase: invalid extranonce sizes %d/%d", spec.ExtraNonce1Size, spec.ExtraNonce2Size)
	}
	txVersion := spec.TxVersion
	if txVersion == 0 {
		txVersion = 2
	}

	heightPush := EncodeHeightBIP34(spec.Height)
	tag := []byte(spec.Tag)
	enSize := spec.ExtraNonce1Size + spec.ExtraNonce2Size
	scriptSigLen := len(heightPush) + len(tag) + enSize
	if scriptSigLen > maxScriptSigLen {
		return nil, fmt.Errorf("coinbase: scriptSig %d bytes exceeds %d (tag too long?)", scriptSigLen, maxScriptSigLen)
	}

	// --- coinb1: everything before the extranonce placeholder ---
	coinb1 := make([]byte, 0, 64)
	coinb1 = appendUint32LE(coinb1, uint32(txVersion))
	coinb1 = append(coinb1, 0x01)                // input count
	coinb1 = append(coinb1, make([]byte, 32)...) // prevout hash = null
	coinb1 = appendUint32LE(coinb1, 0xffffffff)  // prevout index
	coinb1 = append(coinb1, byte(scriptSigLen))  // scriptSig length (always < 0xfd here)
	coinb1 = append(coinb1, heightPush...)
	coinb1 = append(coinb1, tag...)
	// <-- extranonce1 || extranonce2 goes here -->

	// --- coinb2: everything after the placeholder ---
	coinb2 := make([]byte, 0, 96)
	coinb2 = appendUint32LE(coinb2, 0xffffffff) // sequence

	outputs := [][]byte{}
	payout := make([]byte, 0, 9+1+len(spec.PayoutScript))
	payout = appendUint64LE(payout, uint64(spec.CoinbaseValue))
	payout = appendVarInt(payout, uint64(len(spec.PayoutScript)))
	payout = append(payout, spec.PayoutScript...)
	outputs = append(outputs, payout)

	if len(spec.WitnessCommitmentScript) > 0 {
		wc := make([]byte, 0, 9+1+len(spec.WitnessCommitmentScript))
		wc = appendUint64LE(wc, 0)
		wc = appendVarInt(wc, uint64(len(spec.WitnessCommitmentScript)))
		wc = append(wc, spec.WitnessCommitmentScript...)
		outputs = append(outputs, wc)
	}

	coinb2 = appendVarInt(coinb2, uint64(len(outputs)))
	for _, o := range outputs {
		coinb2 = append(coinb2, o...)
	}
	coinb2 = appendUint32LE(coinb2, 0) // locktime

	return &CoinbaseParts{Coinb1: coinb1, Coinb2: coinb2, ExtraNonceSize: enSize}, nil
}

// Assemble joins the parts with concrete extranonce bytes into the full
// serialized coinbase transaction.
func (p *CoinbaseParts) Assemble(extraNonce1, extraNonce2 []byte) ([]byte, error) {
	if len(extraNonce1)+len(extraNonce2) != p.ExtraNonceSize {
		return nil, fmt.Errorf("coinbase: extranonce is %d bytes, want %d",
			len(extraNonce1)+len(extraNonce2), p.ExtraNonceSize)
	}
	out := make([]byte, 0, len(p.Coinb1)+p.ExtraNonceSize+len(p.Coinb2))
	out = append(out, p.Coinb1...)
	out = append(out, extraNonce1...)
	out = append(out, extraNonce2...)
	out = append(out, p.Coinb2...)
	return out, nil
}

// EncodeHeightBIP34 encodes a block height as the BIP34 scriptSig push
// (minimal little-endian number push).
func EncodeHeightBIP34(height int64) []byte {
	if height == 0 {
		return []byte{0x00} // OP_0 (never used for real templates)
	}
	// Serialize as little-endian, minimal, with a padding byte if the high bit
	// of the top byte is set (script numbers are signed).
	var le []byte
	v := height
	for v > 0 {
		le = append(le, byte(v&0xff))
		v >>= 8
	}
	if le[len(le)-1]&0x80 != 0 {
		le = append(le, 0x00)
	}
	out := make([]byte, 0, 1+len(le))
	out = append(out, byte(len(le)))
	out = append(out, le...)
	return out
}

// DecodeHeightBIP34 parses a BIP34 height push from the start of a coinbase
// scriptSig. Returns the height and the number of bytes consumed.
func DecodeHeightBIP34(script []byte) (int64, int, error) {
	if len(script) == 0 {
		return 0, 0, fmt.Errorf("empty scriptSig")
	}
	n := int(script[0])
	if n == 0 || n > 8 || len(script) < 1+n {
		return 0, 0, fmt.Errorf("invalid BIP34 height push")
	}
	var h int64
	for i := n - 1; i >= 0; i-- {
		h = h<<8 | int64(script[1+i])
	}
	return h, 1 + n, nil
}

func appendUint32LE(b []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(b, buf[:]...)
}

func appendUint64LE(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

func appendVarInt(b []byte, v uint64) []byte {
	switch {
	case v < 0xfd:
		return append(b, byte(v))
	case v <= 0xffff:
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(v))
		return append(append(b, 0xfd), buf[:]...)
	case v <= 0xffffffff:
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(v))
		return append(append(b, 0xfe), buf[:]...)
	default:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], v)
		return append(append(b, 0xff), buf[:]...)
	}
}
