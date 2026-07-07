package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeDaemon builds an httptest server that decodes the JSON-RPC request and
// lets the handler produce a result or an error.
func fakeDaemon(t *testing.T, handler func(method string, params []json.RawMessage) (any, *Error)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, rpcErr := handler(req.Method, req.Params)
		env := map[string]any{"id": req.ID, "result": result, "error": rpcErr}
		if rpcErr != nil {
			env["result"] = nil
			// bitcoind uses 500 for method-level errors.
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(env)
	}))
}

func TestCallSuccess(t *testing.T) {
	srv := fakeDaemon(t, func(method string, params []json.RawMessage) (any, *Error) {
		if method != "getblockcount" {
			t.Fatalf("unexpected method %q", method)
		}
		return 840000, nil
	})
	defer srv.Close()

	c := New(Options{URL: srv.URL})
	var height int64
	if err := c.Call(context.Background(), "getblockcount", nil, &height); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if height != 840000 {
		t.Fatalf("height = %d, want 840000", height)
	}
}

func TestCallSendsParamsAndAuth(t *testing.T) {
	gotAuth := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		gotAuth = ok && user == "u" && pass == "p"
		var req struct {
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if len(req.Params) != 1 {
			t.Errorf("params len = %d, want 1", len(req.Params))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": true, "error": nil, "id": 1})
	}))
	defer srv.Close()

	c := New(Options{URL: srv.URL, User: "u", Password: "p"})
	var ok bool
	if err := c.Call(context.Background(), "submitblock", []any{"00ff"}, &ok); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if !gotAuth {
		t.Fatal("basic auth was not sent or did not match")
	}
}

func TestCallDaemonError(t *testing.T) {
	srv := fakeDaemon(t, func(method string, params []json.RawMessage) (any, *Error) {
		return nil, &Error{Code: -32601, Message: "Method not found"}
	})
	defer srv.Close()

	c := New(Options{URL: srv.URL})
	err := c.Call(context.Background(), "nosuchmethod", nil, nil)
	var rpcErr *Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if rpcErr.Code != -32601 {
		t.Fatalf("code = %d, want -32601", rpcErr.Code)
	}
}

func TestCallTransportError(t *testing.T) {
	c := New(Options{URL: "http://127.0.0.1:1", Timeout: 500 * time.Millisecond})
	if err := c.Call(context.Background(), "getblockcount", nil, nil); err == nil {
		t.Fatal("expected transport error, got nil")
	}
}

func TestCallContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c := New(Options{URL: srv.URL})
	if err := c.Call(ctx, "getblockcount", nil, nil); err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
}
