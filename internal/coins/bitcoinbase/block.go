package bitcoinbase

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// SubmitFields are the miner-supplied values from a mining.submit request,
// already hex-decoded/validated by ParseSubmitHex.
type SubmitFields struct {
	ExtraNonce1 []byte
	ExtraNonce2 []byte
	NTime       uint32
	Nonce       uint32
	// VersionBits are the rolled version bits (0 when the miner does not use
	// version rolling).
	VersionBits uint32
	HasVersion  bool
}

// ParseSubmitHex decodes and structurally validates the hex fields of a
// mining.submit request. ntime/nonce/version are 8-hex-char big-endian per the
// stratum convention.
func ParseSubmitHex(extraNonce1, extraNonce2, ntimeHex, nonceHex, versionHex string, wantEN2Size int) (*SubmitFields, error) {
	en1, err := hex.DecodeString(extraNonce1)
	if err != nil {
		return nil, fmt.Errorf("extranonce1 not hex: %w", err)
	}
	en2, err := hex.DecodeString(extraNonce2)
	if err != nil {
		return nil, fmt.Errorf("extranonce2 not hex: %w", err)
	}
	if len(en2) != wantEN2Size {
		return nil, fmt.Errorf("extranonce2 is %d bytes, want %d", len(en2), wantEN2Size)
	}
	ntime, err := parseHexUint32BE(ntimeHex)
	if err != nil {
		return nil, fmt.Errorf("ntime: %w", err)
	}
	nonce, err := parseHexUint32BE(nonceHex)
	if err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	f := &SubmitFields{ExtraNonce1: en1, ExtraNonce2: en2, NTime: ntime, Nonce: nonce}
	if versionHex != "" {
		vb, err := parseHexUint32BE(versionHex)
		if err != nil {
			return nil, fmt.Errorf("version: %w", err)
		}
		f.VersionBits = vb
		f.HasVersion = true
	}
	return f, nil
}

func parseHexUint32BE(s string) (uint32, error) {
	if len(s) != 8 {
		return 0, fmt.Errorf("%q is %d hex chars, want 8", s, len(s))
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(raw), nil
}

// BuildHeader serializes the 80-byte block header from a job, a concrete
// merkle root (little-endian), and submit fields. baseVersion is the
// template's version; when the miner rolled version bits, the header version
// is (base &^ mask) | (bits & mask).
func BuildHeader(job *Job, merkleRootLE []byte, f *SubmitFields, versionMask uint32) ([]byte, error) {
	if len(merkleRootLE) != 32 {
		return nil, fmt.Errorf("merkle root is %d bytes, want 32", len(merkleRootLE))
	}
	prevLE, err := HashLEFromDisplayHex(job.PrevHashDisplay)
	if err != nil {
		return nil, fmt.Errorf("job prevhash: %w", err)
	}
	baseVersionRaw, err := parseHexUint32BE(job.VersionHex)
	if err != nil {
		return nil, fmt.Errorf("job version: %w", err)
	}
	version := baseVersionRaw
	if f.HasVersion {
		version = (baseVersionRaw &^ versionMask) | (f.VersionBits & versionMask)
	}

	header := make([]byte, 0, 80)
	header = appendUint32LE(header, version)
	header = append(header, prevLE...)
	header = append(header, merkleRootLE...)
	header = appendUint32LE(header, f.NTime)
	bitsRaw, err := hex.DecodeString(job.BitsHex)
	if err != nil || len(bitsRaw) != 4 {
		return nil, fmt.Errorf("job bits %q invalid", job.BitsHex)
	}
	header = appendUint32LE(header, binary.BigEndian.Uint32(bitsRaw))
	header = appendUint32LE(header, f.Nonce)
	if len(header) != 80 {
		return nil, fmt.Errorf("header is %d bytes, want 80", len(header))
	}
	return header, nil
}

// WitnessifyCoinbase converts a legacy-serialized coinbase transaction into
// its witness serialization with the BIP141 reserved witness value (a single
// 32-zero-byte stack item). Required inside blocks that carry a witness
// commitment. The txid (merkle input) stays defined over the legacy bytes.
func WitnessifyCoinbase(legacy []byte) ([]byte, error) {
	if len(legacy) < 4+1+36+1+4+1+4 {
		return nil, fmt.Errorf("coinbase too short (%d bytes)", len(legacy))
	}
	out := make([]byte, 0, len(legacy)+2+2+32)
	out = append(out, legacy[:4]...)              // version
	out = append(out, 0x00, 0x01)                 // segwit marker + flag
	out = append(out, legacy[4:len(legacy)-4]...) // vin + vout
	out = append(out, 0x01, 0x20)                 // 1 witness item, 32 bytes
	out = append(out, make([]byte, 32)...)
	out = append(out, legacy[len(legacy)-4:]...) // locktime
	return out, nil
}

// AssembleBlock serializes a full block for submitblock: header, transaction
// count, the coinbase (witness-serialized when the job carries a witness
// commitment), then the template transactions' raw data in order.
func AssembleBlock(header []byte, coinbaseLegacy []byte, job *Job) (string, error) {
	if len(header) != 80 {
		return "", fmt.Errorf("header is %d bytes, want 80", len(header))
	}
	coinbase := coinbaseLegacy
	if job.HasWitnessCommitment {
		w, err := WitnessifyCoinbase(coinbaseLegacy)
		if err != nil {
			return "", err
		}
		coinbase = w
	}
	block := make([]byte, 0, 80+9+len(coinbase))
	block = append(block, header...)
	block = appendVarInt(block, uint64(1+len(job.TxDataHex)))
	block = append(block, coinbase...)
	for i, txHex := range job.TxDataHex {
		raw, err := hex.DecodeString(txHex)
		if err != nil {
			return "", fmt.Errorf("template tx %d not hex: %w", i, err)
		}
		block = append(block, raw...)
	}
	return hex.EncodeToString(block), nil
}
