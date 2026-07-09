package spool

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestAppendDrainTruncate(t *testing.T) {
	sp, err := Open(filepath.Join(t.TempDir(), "s.jsonl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	for i := 0; i < 5; i++ {
		if err := sp.Append(fmt.Sprintf("subj.%d", i), []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if sp.Len() == 0 {
		t.Fatal("spool empty after appends")
	}

	var got []string
	if err := sp.Drain(func(subject string, payload []byte) error {
		got = append(got, subject+"="+string(payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || got[0] != `subj.0={"n":0}` || got[4] != `subj.4={"n":4}` {
		t.Fatalf("drained = %v", got)
	}
	if sp.Len() != 0 {
		t.Fatalf("spool not truncated: %d bytes", sp.Len())
	}
	// Draining an empty spool is a no-op.
	if err := sp.Drain(func(string, []byte) error { t.Fatal("called on empty"); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestDrainPartialFailureKeepsRemainder(t *testing.T) {
	sp, err := Open(filepath.Join(t.TempDir(), "s.jsonl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	for i := 0; i < 4; i++ {
		if err := sp.Append("s", []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("network gone")
	n := 0
	err = sp.Drain(func(_ string, payload []byte) error {
		if string(payload) == "2" {
			return boom
		}
		n++
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if n != 2 {
		t.Fatalf("replayed %d before failure, want 2", n)
	}
	// Remainder ("2","3") must survive for the next drain.
	var rest []string
	if err := sp.Drain(func(_ string, payload []byte) error {
		rest = append(rest, string(payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(rest) != 2 || rest[0] != "2" || rest[1] != "3" {
		t.Fatalf("remainder = %v", rest)
	}
	// New appends after a rewrite still work.
	if err := sp.Append("s", []byte("9")); err != nil {
		t.Fatal(err)
	}
	var last []string
	_ = sp.Drain(func(_ string, p []byte) error { last = append(last, string(p)); return nil })
	if len(last) != 1 || last[0] != "9" {
		t.Fatalf("post-rewrite append = %v", last)
	}
}

func TestSizeBound(t *testing.T) {
	sp, err := Open(filepath.Join(t.TempDir(), "s.jsonl"), 128)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	payload := []byte(`{"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`)
	var full bool
	for i := 0; i < 100; i++ {
		if err := sp.Append("s", payload); err != nil {
			if !errors.Is(err, ErrFull) {
				t.Fatalf("unexpected err: %v", err)
			}
			full = true
			break
		}
	}
	if !full {
		t.Fatal("spool never reported full")
	}
}

func TestReopenPersistedSpool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.jsonl")
	sp, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.Append("persist", []byte("alive")); err != nil {
		t.Fatal(err)
	}
	sp.Close()

	// A crash-restart must find the record.
	sp2, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Close()
	var got []string
	if err := sp2.Drain(func(subject string, payload []byte) error {
		got = append(got, subject+"="+string(payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "persist=alive" {
		t.Fatalf("reopened drain = %v", got)
	}
}

func TestDrainBatchSecondBatchFailure(t *testing.T) {
	sp, err := Open(filepath.Join(t.TempDir(), "s.jsonl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	for i := 0; i < 25; i++ {
		if err := sp.Append("s", []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}

	// Batches of 10 => [10,10,5]. Fail on the 2nd batch.
	var seen [][]Record
	calls := 0
	err = sp.DrainBatch(10, func(recs []Record) error {
		calls++
		if calls == 2 {
			return errors.New("db down")
		}
		seen = append(seen, recs)
		return nil
	})
	if err == nil {
		t.Fatal("expected error from failing batch")
	}
	if len(seen) != 1 || len(seen[0]) != 10 {
		t.Fatalf("committed batches = %v", seen)
	}
	// 15 records must remain (batch 2's 10 + batch 3's 5).
	var remaining int
	if err := sp.DrainBatch(10, func(recs []Record) error {
		remaining += len(recs)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if remaining != 15 {
		t.Fatalf("remaining = %d, want 15", remaining)
	}
	if sp.Len() != 0 {
		t.Fatalf("spool not empty after full drain: %d", sp.Len())
	}
}

// TestDrainBatchFullSuccess: every batch succeeds, so the spool is fully
// drained and left empty, and every record is delivered exactly once in order.
func TestDrainBatchFullSuccess(t *testing.T) {
	sp, err := Open(filepath.Join(t.TempDir(), "s.jsonl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	const n = 25
	for i := 0; i < n; i++ {
		if err := sp.Append("s", []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	// Batches of 10 => [10,10,5]; all succeed.
	var got []Record
	calls := 0
	if err := sp.DrainBatch(10, func(recs []Record) error {
		calls++
		got = append(got, recs...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("batch calls = %d, want 3", calls)
	}
	if len(got) != n {
		t.Fatalf("delivered %d records, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if string(got[i].Payload) != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("record %d out of order: %s", i, got[i].Payload)
		}
	}
	if sp.Len() != 0 {
		t.Fatalf("spool not empty after full success: %d bytes", sp.Len())
	}
}

// TestDrainBatchFirstBatchFailure: the FIRST batch fails, so nothing is
// committed — every record must remain on disk for the next attempt.
func TestDrainBatchFirstBatchFailure(t *testing.T) {
	sp, err := Open(filepath.Join(t.TempDir(), "s.jsonl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()

	const n = 25
	for i := 0; i < n; i++ {
		if err := sp.Append("s", []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	// Fail on the very first batch.
	calls := 0
	err = sp.DrainBatch(10, func(recs []Record) error {
		calls++
		return errors.New("db down on first batch")
	})
	if err == nil {
		t.Fatal("expected error from first-batch failure")
	}
	if calls != 1 {
		t.Fatalf("batch calls = %d, want 1 (must stop at the first failure)", calls)
	}
	// All 25 records must still be on disk; a clean drain recovers them in order.
	var got []Record
	if err := sp.DrainBatch(10, func(recs []Record) error {
		got = append(got, recs...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("recovered %d records after first-batch failure, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if string(got[i].Payload) != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("record %d out of order after retain: %s", i, got[i].Payload)
		}
	}
	if sp.Len() != 0 {
		t.Fatalf("spool not empty after recovery: %d bytes", sp.Len())
	}
}

func TestDrainBatchEmpty(t *testing.T) {
	sp, err := Open(filepath.Join(t.TempDir(), "s.jsonl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	called := false
	if err := sp.DrainBatch(10, func(recs []Record) error { called = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("callback invoked on empty spool")
	}
}
