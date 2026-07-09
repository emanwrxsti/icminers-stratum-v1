package bitcoinbase

import "crypto/sha512"

// SHA512_256D computes Radiant's proof-of-work hash of an 80-byte header:
// SHA-512/256 applied twice (SHA512/256d), a double application of SHA-512
// truncated to 256 bits. The result is a little-endian 32-byte digest, the
// same orientation as DoubleSHA256, so it drops straight into the existing
// target-comparison path.
func SHA512_256D(header []byte) ([]byte, error) {
	first := sha512.Sum512_256(header)
	second := sha512.Sum512_256(first[:])
	return second[:], nil
}
