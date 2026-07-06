package session

import (
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
)

// ExtraNonce1Allocator hands out unique extranonce1 values. Each stratum node
// must produce extranonce1 values that never collide across its own sessions so
// that (extranonce1, extranonce2) search spaces do not overlap between miners.
//
// The allocator combines a per-node prefix with a monotonic counter. The prefix
// keeps two different regional nodes from colliding when they share a coin; the
// counter guarantees uniqueness within a node.
type ExtraNonce1Allocator struct {
	prefix  []byte
	counter atomic.Uint32
	size    int // total extranonce1 size in bytes
}

// NewExtraNonce1Allocator builds an allocator. size is the extranonce1 width in
// bytes (commonly 4). prefix is an optional per-node identifier; it is truncated
// or right-padded to leave room for the 4-byte counter.
func NewExtraNonce1Allocator(size int, prefix []byte) *ExtraNonce1Allocator {
	if size < 4 {
		size = 4
	}
	// Reserve the trailing 4 bytes for the counter.
	prefixLen := size - 4
	p := make([]byte, prefixLen)
	copy(p, prefix)
	return &ExtraNonce1Allocator{prefix: p, size: size}
}

// Next returns the next unique extranonce1 as a lowercase hex string.
func (a *ExtraNonce1Allocator) Next() string {
	n := a.counter.Add(1)
	buf := make([]byte, a.size)
	copy(buf, a.prefix)
	binary.BigEndian.PutUint32(buf[a.size-4:], n)
	return hex.EncodeToString(buf)
}

// Size returns the extranonce1 width in bytes.
func (a *ExtraNonce1Allocator) Size() int { return a.size }

// ExtraNonce2Size is the number of bytes miners are expected to roll. It is
// reported to miners in the mining.subscribe response.
const ExtraNonce2Size = 4
