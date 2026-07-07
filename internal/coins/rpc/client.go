// Package rpc implements a minimal JSON-RPC 1.0/2.0-over-HTTP client for coin
// daemons (Bitcoin Core and compatible forks). It is deliberately small, uses
// only the standard library, and returns pool-scoped errors so a broken daemon
// degrades one pool without touching the rest of the process.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Error is a JSON-RPC error object returned by the daemon.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("daemon rpc error %d: %s", e.Code, e.Message)
}

// response is the daemon's JSON-RPC envelope.
type response struct {
	Result json.RawMessage `json:"result"`
	Error  *Error          `json:"error"`
	ID     json.RawMessage `json:"id"`
}

// request is the JSON-RPC envelope we send.
type request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

// Options configures a Client.
type Options struct {
	// URL is the daemon endpoint, e.g. "http://127.0.0.1:8332".
	URL string
	// User/Password are HTTP basic auth credentials (bitcoind rpcuser/rpcpassword).
	User     string
	Password string
	// Timeout bounds a single call including connect + response body. Zero
	// falls back to a safe default.
	Timeout time.Duration
}

// Client is a JSON-RPC-over-HTTP daemon client. It is safe for concurrent use.
type Client struct {
	url      string
	user     string
	password string
	http     *http.Client
	nextID   atomic.Uint64
}

// New builds a Client.
func New(opts Options) *Client {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &Client{
		url:      opts.URL,
		user:     opts.User,
		password: opts.Password,
		http:     &http.Client{Timeout: timeout},
	}
}

// Call performs one RPC. params may be nil (sent as []). If result is non-nil
// the daemon's result field is unmarshaled into it. A daemon-level error is
// returned as *Error; transport and decode failures are wrapped normally.
func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(request{
		JSONRPC: "1.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("rpc %s: marshal: %w", method, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("rpc %s: build request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.user != "" || c.password != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("rpc %s: %w", method, err)
	}
	defer resp.Body.Close()

	// bitcoind returns 500 with a JSON-RPC error body for method-level errors;
	// try to decode the envelope regardless of status, then fall back to a
	// status error if that fails.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return fmt.Errorf("rpc %s: read body: %w", method, err)
	}
	var env response
	if err := json.Unmarshal(raw, &env); err != nil {
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("rpc %s: http %d", method, resp.StatusCode)
		}
		return fmt.Errorf("rpc %s: decode: %w", method, err)
	}
	if env.Error != nil {
		return env.Error
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rpc %s: http %d", method, resp.StatusCode)
	}
	if result != nil {
		if err := json.Unmarshal(env.Result, result); err != nil {
			return fmt.Errorf("rpc %s: decode result: %w", method, err)
		}
	}
	return nil
}
