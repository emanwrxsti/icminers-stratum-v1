package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
)

// ErrLineTooLong is returned when an inbound line exceeds the configured cap.
// The server treats this as an abuse signal (malformed/oversized spam).
var ErrLineTooLong = errors.New("stratum: line exceeds maximum length")

// Reader reads newline-delimited JSON-RPC requests from a stream, enforcing a
// hard per-line byte cap to defend against memory-abuse spam.
type Reader struct {
	sc      *bufio.Scanner
	maxLine int
}

// NewReader wraps r. maxLine must be > 0.
func NewReader(r io.Reader, maxLine int) *Reader {
	sc := bufio.NewScanner(r)
	// Give the scanner room up to maxLine+1 so we can positively detect an
	// over-limit line rather than silently truncating it.
	sc.Buffer(make([]byte, 0, 4096), maxLine+1)
	return &Reader{sc: sc, maxLine: maxLine}
}

// Read returns the next request. It returns io.EOF at end of stream and
// ErrLineTooLong if a line breaches the cap. A blank line yields a nil request
// and nil error so callers can skip keep-alive newlines.
func (r *Reader) Read() (*Request, error) {
	if !r.sc.Scan() {
		if err := r.sc.Err(); err != nil {
			if errors.Is(err, bufio.ErrTooLong) {
				return nil, ErrLineTooLong
			}
			return nil, err
		}
		return nil, io.EOF
	}
	line := r.sc.Bytes()
	if len(line) > r.maxLine {
		return nil, ErrLineTooLong
	}
	if len(trimSpace(line)) == 0 {
		return nil, nil
	}
	var req Request
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, &MalformedError{Cause: err}
	}
	return &req, nil
}

// MalformedError wraps a JSON decode failure so the server can distinguish
// malformed-JSON spam from transport errors and ban accordingly.
type MalformedError struct{ Cause error }

func (e *MalformedError) Error() string { return "stratum: malformed JSON: " + e.Cause.Error() }
func (e *MalformedError) Unwrap() error { return e.Cause }

// Writer serializes outbound messages as newline-delimited JSON.
type Writer struct {
	w io.Writer
}

// NewWriter wraps w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// WriteResponse encodes and flushes a response followed by a newline.
func (w *Writer) WriteResponse(resp *Response) error { return w.writeJSON(resp) }

// WriteNotification encodes and flushes a notification followed by a newline.
func (w *Writer) WriteNotification(n *Notification) error { return w.writeJSON(n) }

func (w *Writer) writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.w.Write(b)
	return err
}

func trimSpace(b []byte) []byte {
	start := 0
	for start < len(b) && isSpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}
