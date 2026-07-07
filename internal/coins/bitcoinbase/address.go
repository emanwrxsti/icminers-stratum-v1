package bitcoinbase

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"
)

// AddressParams describes a Bitcoin-family chain's address encoding so the
// same logic serves BTC mainnet, testnet, and future bitcoinlike coins.
type AddressParams struct {
	// P2PKHVersion / P2SHVersion are the base58check version bytes.
	P2PKHVersion byte
	P2SHVersion  byte
	// Bech32HRP is the human-readable part ("bc" for mainnet). Empty disables
	// bech32 decoding for the chain.
	Bech32HRP string
}

// BTCMainNet are Bitcoin mainnet address parameters.
var BTCMainNet = AddressParams{P2PKHVersion: 0x00, P2SHVersion: 0x05, Bech32HRP: "bc"}

// BTCTestNet are Bitcoin testnet/signet address parameters.
var BTCTestNet = AddressParams{P2PKHVersion: 0x6f, P2SHVersion: 0xc4, Bech32HRP: "tb"}

// BTCRegTest are Bitcoin regtest address parameters.
var BTCRegTest = AddressParams{P2PKHVersion: 0x6f, P2SHVersion: 0xc4, Bech32HRP: "bcrt"}

// AddressToScript converts a payout address into its scriptPubKey. Supports
// base58check P2PKH/P2SH and bech32/bech32m segwit v0-v16 programs.
func AddressToScript(address string, params AddressParams) ([]byte, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("empty address")
	}
	if params.Bech32HRP != "" {
		lower := strings.ToLower(address)
		if strings.HasPrefix(lower, params.Bech32HRP+"1") {
			return segwitToScript(lower, params.Bech32HRP)
		}
	}
	return base58ToScript(address, params)
}

// --- base58check ---

const b58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

var b58Index = func() map[byte]int {
	m := make(map[byte]int, len(b58Alphabet))
	for i := 0; i < len(b58Alphabet); i++ {
		m[b58Alphabet[i]] = i
	}
	return m
}()

// Base58CheckDecode decodes a base58check string into version byte + payload.
func Base58CheckDecode(s string) (version byte, payload []byte, err error) {
	num := new(big.Int)
	radix := big.NewInt(58)
	for i := 0; i < len(s); i++ {
		d, ok := b58Index[s[i]]
		if !ok {
			return 0, nil, fmt.Errorf("invalid base58 character %q", s[i])
		}
		num.Mul(num, radix)
		num.Add(num, big.NewInt(int64(d)))
	}
	raw := num.Bytes()
	// Leading '1's encode leading zero bytes.
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	full := make([]byte, zeros+len(raw))
	copy(full[zeros:], raw)

	if len(full) < 5 {
		return 0, nil, fmt.Errorf("base58check string too short")
	}
	body, checksum := full[:len(full)-4], full[len(full)-4:]
	want := DoubleSHA256(body)[:4]
	if !bytes.Equal(checksum, want) {
		return 0, nil, fmt.Errorf("base58check checksum mismatch")
	}
	return body[0], body[1:], nil
}

func base58ToScript(address string, params AddressParams) ([]byte, error) {
	version, payload, err := Base58CheckDecode(address)
	if err != nil {
		return nil, fmt.Errorf("address %q: %w", address, err)
	}
	if len(payload) != 20 {
		return nil, fmt.Errorf("address %q: payload is %d bytes, want 20", address, len(payload))
	}
	switch version {
	case params.P2PKHVersion:
		// OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG
		script := make([]byte, 0, 25)
		script = append(script, 0x76, 0xa9, 0x14)
		script = append(script, payload...)
		script = append(script, 0x88, 0xac)
		return script, nil
	case params.P2SHVersion:
		// OP_HASH160 <20> OP_EQUAL
		script := make([]byte, 0, 23)
		script = append(script, 0xa9, 0x14)
		script = append(script, payload...)
		script = append(script, 0x87)
		return script, nil
	default:
		return nil, fmt.Errorf("address %q: unknown version byte 0x%02x", address, version)
	}
}

// --- bech32 / bech32m (BIP 173 / BIP 350) ---

const bech32Charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

var bech32Index = func() map[byte]int {
	m := make(map[byte]int, len(bech32Charset))
	for i := 0; i < len(bech32Charset); i++ {
		m[bech32Charset[i]] = i
	}
	return m
}()

func bech32Polymod(values []int) int {
	gen := []int{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	chk := 1
	for _, v := range values {
		top := chk >> 25
		chk = (chk&0x1ffffff)<<5 ^ v
		for i := 0; i < 5; i++ {
			if (top>>i)&1 == 1 {
				chk ^= gen[i]
			}
		}
	}
	return chk
}

func bech32HRPExpand(hrp string) []int {
	out := make([]int, 0, len(hrp)*2+1)
	for i := 0; i < len(hrp); i++ {
		out = append(out, int(hrp[i])>>5)
	}
	out = append(out, 0)
	for i := 0; i < len(hrp); i++ {
		out = append(out, int(hrp[i])&31)
	}
	return out
}

// bech32Decode returns the data part (5-bit groups, checksum stripped) and the
// checksum constant (1 = bech32, 0x2bc830a3 = bech32m).
func bech32Decode(s, expectHRP string) ([]int, int, error) {
	pos := strings.LastIndexByte(s, '1')
	if pos < 1 || pos+7 > len(s) {
		return nil, 0, fmt.Errorf("invalid bech32 separator position")
	}
	hrp, dataPart := s[:pos], s[pos+1:]
	if hrp != expectHRP {
		return nil, 0, fmt.Errorf("hrp %q does not match expected %q", hrp, expectHRP)
	}
	data := make([]int, 0, len(dataPart))
	for i := 0; i < len(dataPart); i++ {
		d, ok := bech32Index[dataPart[i]]
		if !ok {
			return nil, 0, fmt.Errorf("invalid bech32 character %q", dataPart[i])
		}
		data = append(data, d)
	}
	konst := bech32Polymod(append(bech32HRPExpand(hrp), data...))
	if konst != 1 && konst != 0x2bc830a3 {
		return nil, 0, fmt.Errorf("bech32 checksum mismatch")
	}
	return data[:len(data)-6], konst, nil
}

func convertBits(data []int, fromBits, toBits uint, pad bool) ([]byte, error) {
	acc, bits := 0, uint(0)
	out := make([]byte, 0, len(data))
	maxv := (1 << toBits) - 1
	for _, v := range data {
		if v < 0 || v>>fromBits != 0 {
			return nil, fmt.Errorf("invalid value %d for %d-bit group", v, fromBits)
		}
		acc = (acc << fromBits) | v
		bits += fromBits
		for bits >= toBits {
			bits -= toBits
			out = append(out, byte((acc>>bits)&maxv))
		}
	}
	if pad {
		if bits > 0 {
			out = append(out, byte((acc<<(toBits-bits))&maxv))
		}
	} else if bits >= fromBits || ((acc<<(toBits-bits))&maxv) != 0 {
		return nil, fmt.Errorf("invalid bech32 padding")
	}
	return out, nil
}

func segwitToScript(address, hrp string) ([]byte, error) {
	data, konst, err := bech32Decode(address, hrp)
	if err != nil {
		return nil, fmt.Errorf("address %q: %w", address, err)
	}
	if len(data) < 1 {
		return nil, fmt.Errorf("address %q: empty witness program", address)
	}
	version := data[0]
	program, err := convertBits(data[1:], 5, 8, false)
	if err != nil {
		return nil, fmt.Errorf("address %q: %w", address, err)
	}
	if version < 0 || version > 16 {
		return nil, fmt.Errorf("address %q: invalid witness version %d", address, version)
	}
	if len(program) < 2 || len(program) > 40 {
		return nil, fmt.Errorf("address %q: witness program is %d bytes", address, len(program))
	}
	if version == 0 && len(program) != 20 && len(program) != 32 {
		return nil, fmt.Errorf("address %q: v0 program must be 20 or 32 bytes", address)
	}
	// BIP 350: v0 must use bech32, v1+ must use bech32m.
	if version == 0 && konst != 1 {
		return nil, fmt.Errorf("address %q: v0 must use bech32 checksum", address)
	}
	if version != 0 && konst != 0x2bc830a3 {
		return nil, fmt.Errorf("address %q: v%d must use bech32m checksum", address, version)
	}

	opVersion := byte(0x00) // OP_0
	if version > 0 {
		opVersion = byte(0x50 + version) // OP_1..OP_16
	}
	script := make([]byte, 0, 2+len(program))
	script = append(script, opVersion, byte(len(program)))
	script = append(script, program...)
	return script, nil
}
