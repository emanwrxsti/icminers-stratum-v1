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
