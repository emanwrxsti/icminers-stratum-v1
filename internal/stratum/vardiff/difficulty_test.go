package vardiff

import (
	"math"
	"math/big"
	"testing"
)

func TestDifficultyOneTargetIsCanonical(t *testing.T) {
	// Known answer: difficulty 1 must map to the canonical SHA256d diff-1 target.
	want, _ := new(big.Int).SetString(
		"00000000FFFF0000000000000000000000000000000000000000000000000000", 16)
	got := DifficultyToTarget(1)
	if got.Cmp(want) != 0 {
		t.Fatalf("diff-1 target mismatch:\n got=%064x\nwant=%064x", got, want)
	}
}

func TestTargetToDifficultyRoundTrip(t *testing.T) {
	for _, diff := range []float64{1, 2, 16, 1024, 65536, 1e6} {
		target := DifficultyToTarget(diff)
		back := TargetToDifficulty(target)
		if rel := math.Abs(back-diff) / diff; rel > 1e-9 {
			t.Errorf("round trip diff=%g -> %g (rel err %g)", diff, back, rel)
		}
	}
}

func TestHigherDifficultyMeansSmallerTarget(t *testing.T) {
	low := DifficultyToTarget(1)
	high := DifficultyToTarget(1024)
	if high.Cmp(low) >= 0 {
		t.Fatalf("expected higher difficulty to yield smaller target")
	}
}

func TestMeetsTarget(t *testing.T) {
	target := DifficultyToTarget(1)
	// A hash equal to the target meets it; one above does not.
	if !MeetsTarget(new(big.Int).Set(target), target) {
		t.Error("hash equal to target should meet it")
	}
	above := new(big.Int).Add(target, big.NewInt(1))
	if MeetsTarget(above, target) {
		t.Error("hash above target should not meet it")
	}
	below := new(big.Int).Sub(target, big.NewInt(1))
	if !MeetsTarget(below, target) {
		t.Error("hash below target should meet it")
	}
}

func TestZeroDifficultyIsSafe(t *testing.T) {
	if DifficultyToTarget(0).Cmp(Diff1Target()) != 0 {
		t.Error("difficulty 0 should fall back to diff-1 target, not panic")
	}
	if TargetToDifficulty(big.NewInt(0)) != 0 {
		t.Error("zero target should yield 0 difficulty")
	}
}
