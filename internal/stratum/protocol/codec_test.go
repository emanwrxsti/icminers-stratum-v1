package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReaderParsesRequests(t *testing.T) {
	input := `{"id":1,"method":"mining.subscribe","params":["cgminer/4.9"]}` + "\n" +
		`{"id":2,"method":"mining.authorize","params":["addr.worker","x"]}` + "\n"
	r := NewReader(strings.NewReader(input), 64*1024)

	req1, err := r.Read()
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if req1.Method != MethodSubscribe {
		t.Errorf("method 1 = %q", req1.Method)
	}

	req2, err := r.Read()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if req2.Method != MethodAuthorize {
		t.Errorf("method 2 = %q", req2.Method)
	}

	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestReaderSkipsBlankLines(t *testing.T) {
	r := NewReader(strings.NewReader("\n\n"+`{"id":1,"method":"x"}`+"\n"), 1024)
	// First two reads are blank -> (nil, nil).
	for i := 0; i < 2; i++ {
		req, err := r.Read()
		if err != nil || req != nil {
			t.Fatalf("blank line %d: req=%v err=%v", i, req, err)
		}
	}
	req, err := r.Read()
	if err != nil || req == nil {
		t.Fatalf("expected request, got req=%v err=%v", req, err)
	}
}

func TestReaderRejectsMalformed(t *testing.T) {
	r := NewReader(strings.NewReader("{not json}\n"), 1024)
	_, err := r.Read()
	var me *MalformedError
	if !errors.As(err, &me) {
		t.Fatalf("expected MalformedError, got %v", err)
	}
}

func TestReaderEnforcesLineCap(t *testing.T) {
	long := `{"id":1,"method":"` + strings.Repeat("a", 200) + `"}`
	r := NewReader(strings.NewReader(long+"\n"), 64)
	_, err := r.Read()
	if !errors.Is(err, ErrLineTooLong) {
		t.Fatalf("expected ErrLineTooLong, got %v", err)
	}
}

func TestWriterResponseRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	id := json.RawMessage(`7`)
	if err := w.WriteResponse(OKResponse(id, true)); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(buf.String())

	var resp Response
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Result != true {
		t.Errorf("result = %v", resp.Result)
	}
	if resp.Error != nil {
		t.Errorf("unexpected error: %v", resp.Error)
	}
}

func TestErrorMarshalsAsArray(t *testing.T) {
	e := NewError(ErrUnauthorized, "nope")
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	// Must be a 3-element array [code, message, data].
	var arr []json.RawMessage
	if err := json.Unmarshal(b, &arr); err != nil {
		t.Fatalf("error did not marshal to array: %s", b)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d: %s", len(arr), b)
	}
}
