package scash

import (
	"crypto/sha256"
	"fmt"
)

// epochSeedTemplate and epochDuration mirror Scash consensus. The seed string
// is "Scash/RandomX/Epoch/<epoch>", hashed with SHA-256 to a 32-byte key; the
// epoch is floor(headerTime / epochDuration). The key changes each epoch and
// re-seeds the RandomX dataset.
const (
	epochSeedPrefix      = "Scash/RandomX/Epoch/"
	defaultEpochDuration = 7 * 24 * 60 * 60 // one week, in seconds
)

// EpochSeedHash derives the RandomX seed hash (epoch key) for a given header
// time and epoch duration, exactly matching Scash's GenerateEpochSeedHash:
//
//	epoch    = floor(unixTime / epochDuration)
//	seedText = "Scash/RandomX/Epoch/" + decimal(epoch)
//	seed     = SHA256(seedText)
//
// epochDuration <= 0 uses the Scash default (one week).
func EpochSeedHash(unixTime int64, epochDuration int64) []byte {
	if epochDuration <= 0 {
		epochDuration = defaultEpochDuration
	}
	epoch := unixTime / epochDuration
	if unixTime < 0 && unixTime%epochDuration != 0 {
		epoch-- // floor toward negative infinity (defensive; times are positive)
	}
	seedText := fmt.Sprintf("%s%d", epochSeedPrefix, epoch)
	sum := sha256.Sum256([]byte(seedText))
	return sum[:]
}

// EpochNumber returns the epoch index for a header time.
func EpochNumber(unixTime int64, epochDuration int64) int64 {
	if epochDuration <= 0 {
		epochDuration = defaultEpochDuration
	}
	return unixTime / epochDuration
}
