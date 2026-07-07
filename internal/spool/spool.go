// Package spool provides a durable local queue for events that could not be
// published to NATS: newline-delimited JSON records appended to a file, then
// replayed and truncated once connectivity returns. A regional node keeps
// accepting shares through a complete NATS outage and loses nothing.
package spool

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Record is one spooled event: the NATS subject it was bound for plus the
// original payload (base64 in the JSONL encoding, so any bytes are safe).
type Record struct {
	Subject string `json:"subject"`
	Payload []byte `json:"payload"`
}

// Spool is a single append-only spool file. Safe for concurrent use.
type Spool struct {
	path string

	mu   sync.Mutex
	f    *os.File
	size int64
	max  int64
}

// DefaultMaxBytes bounds a spool file (256 MiB); beyond it, appends are
// rejected so a long outage cannot fill the disk.
const DefaultMaxBytes = 256 << 20

// Open creates/opens the spool file at path (parent directories included).
func Open(path string, maxBytes int64) (*Spool, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("spool: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("spool: open: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("spool: stat: %w", err)
	}
	return &Spool{path: path, f: f, size: st.Size(), max: maxBytes}, nil
}

// ErrFull is returned when the spool has hit its size bound.
var ErrFull = fmt.Errorf("spool full")

// Append durably queues one record.
func (s *Spool) Append(subject string, payload []byte) error {
	line, err := json.Marshal(Record{Subject: subject, Payload: payload})
	if err != nil {
		return fmt.Errorf("spool: marshal: %w", err)
	}
	line = append(line, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.size+int64(len(line)) > s.max {
		return ErrFull
	}
	n, err := s.f.Write(line)
	s.size += int64(n)
	if err != nil {
		return fmt.Errorf("spool: write: %w", err)
	}
	return s.f.Sync()
}

// Len returns the current spool size in bytes (0 = empty).
func (s *Spool) Len() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Drain replays every spooled record through fn in order. If fn returns an
// error, draining stops and already-replayed records stay consumed: the
// remainder is rewritten as the new spool. On full success the spool is
// truncated to empty.
func (s *Spool) Drain(fn func(subject string, payload []byte) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.size == 0 {
		return nil
	}

	// Read everything currently on disk.
	rf, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("spool: open for drain: %w", err)
	}
	var records []Record
	sc := bufio.NewScanner(rf)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // skip a torn/corrupt line rather than wedging the spool
		}
		records = append(records, rec)
	}
	scanErr := sc.Err()
	rf.Close()
	if scanErr != nil {
		return fmt.Errorf("spool: scan: %w", scanErr)
	}

	failedAt := -1
	var replayErr error
	for i, rec := range records {
		if err := fn(rec.Subject, rec.Payload); err != nil {
			failedAt = i
			replayErr = err
			break
		}
	}

	// Rewrite the file with whatever was not replayed.
	rest := []Record{}
	if failedAt >= 0 {
		rest = records[failedAt:]
	}
	if err := s.rewriteLocked(rest); err != nil {
		return err
	}
	return replayErr
}

// rewriteLocked atomically replaces the spool contents. Caller holds s.mu.
func (s *Spool) rewriteLocked(records []Record) error {
	tmp := s.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("spool: rewrite open: %w", err)
	}
	var size int64
	w := bufio.NewWriter(tf)
	for _, rec := range records {
		line, err := json.Marshal(rec)
		if err != nil {
			continue
		}
		line = append(line, '\n')
		n, err := w.Write(line)
		size += int64(n)
		if err != nil {
			tf.Close()
			return fmt.Errorf("spool: rewrite write: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		tf.Close()
		return fmt.Errorf("spool: rewrite flush: %w", err)
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return fmt.Errorf("spool: rewrite sync: %w", err)
	}
	tf.Close()
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("spool: rewrite rename: %w", err)
	}
	// Reopen the append handle on the new file.
	_ = s.f.Close()
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("spool: reopen: %w", err)
	}
	s.f = f
	s.size = size
	return nil
}

// Close releases the file handle.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
