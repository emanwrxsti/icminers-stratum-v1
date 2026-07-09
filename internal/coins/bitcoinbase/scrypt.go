package bitcoinbase

import "golang.org/x/crypto/scrypt"

// ScryptHash computes the Litecoin-family proof-of-work hash of an 80-byte
// header: scrypt(header, header, N=1024, r=1, p=1, dkLen=32). The result is a
// little-endian 32-byte digest, the same orientation as DoubleSHA256, so it
// drops straight into the existing target-comparison path.
//
// Note the header both keys and salts the KDF (the coin's PoW definition).
func ScryptHash(header []byte) ([]byte, error) {
	return scrypt.Key(header, header, 1024, 1, 1, 32)
}
