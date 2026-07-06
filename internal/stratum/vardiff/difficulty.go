// Package vardiff holds difficulty/target conversion helpers plus (in later
// stages) the per-worker variable-difficulty controller. The conversion math is
// consensus-adjacent, so it is isolated here and covered by known-answer tests.
package vardiff

import (
	"math/big"
)

// diff1Target is the pool "difficulty 1" target used by SHA256d chains:
//
//	0x00000000FFFF0000000000000000000000000000000000000000000000000000
//
// A share of difficulty D must hash below diff1Target / D.
var diff1Target = func() *big.Int {
	t, _ := new(big.Int).SetString(
		"00000000FFFF0000000000000000000000000000000000000000000000000000", 16)
	return t
}()

// Diff1Target returns a copy of the difficulty-1 target.
func Diff1Target() *big.Int { return new(big.Int).Set(diff1Target) }

// DifficultyToTarget converts a pool difficulty to its 256-bit target
// (target = diff1Target / difficulty). Difficulty must be > 0.
func DifficultyToTarget(difficulty float64) *big.Int {
	if difficulty <= 0 {
		return new(big.Int).Set(diff1Target)
	}
	// Use big.Float for precision, then truncate to an integer target.
	num := new(big.Float).SetInt(diff1Target)
	den := new(big.Float).SetFloat64(difficulty)
	res := new(big.Float).Quo(num, den)
	target, _ := res.Int(nil)
	return target
}

// TargetToDifficulty converts a 256-bit target back to a pool difficulty
// (difficulty = diff1Target / target). A zero/negative target yields 0.
func TargetToDifficulty(target *big.Int) float64 {
	if target == nil || target.Sign() <= 0 {
		return 0
	}
	num := new(big.Float).SetInt(diff1Target)
	den := new(big.Float).SetInt(target)
	res := new(big.Float).Quo(num, den)
	f, _ := res.Float64()
	return f
}

// HashToBig interprets a 32-byte hash as a big-endian 256-bit integer. Callers
// mining SHA256d must reverse the little-endian daemon hash first if needed;
// this helper assumes the caller has already produced big-endian bytes.
func HashToBig(hash []byte) *big.Int {
	return new(big.Int).SetBytes(hash)
}

// MeetsTarget reports whether a big-endian hash integer is at or below target
// (i.e. the share/block is valid for that target).
func MeetsTarget(hash *big.Int, target *big.Int) bool {
	return hash.Cmp(target) <= 0
}

// ShareDifficulty returns the effective difficulty of a big-endian hash integer
// (diff1Target / hash). Useful for scoring PPLNS/PROP by share difficulty.
func ShareDifficulty(hash *big.Int) float64 {
	return TargetToDifficulty(hash)
}
