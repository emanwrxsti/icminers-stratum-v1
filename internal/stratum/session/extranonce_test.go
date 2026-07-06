package session

import (
	"sync"
	"testing"
)

func TestExtraNonce1Width(t *testing.T) {
	a := NewExtraNonce1Allocator(4, nil)
	en1 := a.Next()
	// 4 bytes -> 8 hex chars.
	if len(en1) != 8 {
		t.Fatalf("expected 8 hex chars, got %d (%q)", len(en1), en1)
	}
}

func TestExtraNonce1Unique(t *testing.T) {
	a := NewExtraNonce1Allocator(4, []byte{0xab, 0xcd})
	seen := make(map[string]bool)
	for i := 0; i < 100000; i++ {
		v := a.Next()
		if seen[v] {
			t.Fatalf("duplicate extranonce1 at i=%d: %q", i, v)
		}
		seen[v] = true
	}
}

func TestExtraNonce1PrefixApplied(t *testing.T) {
	a := NewExtraNonce1Allocator(6, []byte{0xde, 0xad})
	v := a.Next()
	// 6 bytes -> 12 hex chars, first 4 chars are the prefix "dead".
	if len(v) != 12 {
		t.Fatalf("expected 12 hex chars, got %d (%q)", len(v), v)
	}
	if v[:4] != "dead" {
		t.Errorf("expected prefix 'dead', got %q", v[:4])
	}
}

func TestExtraNonce1ConcurrentUnique(t *testing.T) {
	a := NewExtraNonce1Allocator(4, nil)
	const goroutines = 8
	const per = 10000

	var mu sync.Mutex
	seen := make(map[string]bool, goroutines*per)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]string, 0, per)
			for i := 0; i < per; i++ {
				local = append(local, a.Next())
			}
			mu.Lock()
			for _, v := range local {
				if seen[v] {
					t.Errorf("duplicate across goroutines: %q", v)
				}
				seen[v] = true
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != goroutines*per {
		t.Fatalf("expected %d unique, got %d", goroutines*per, len(seen))
	}
}
