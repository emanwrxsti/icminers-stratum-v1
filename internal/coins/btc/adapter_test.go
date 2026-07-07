package btc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/bitcoinbase"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins/rpc"
)

// fakeCore simulates the subset of Bitcoin Core RPC the adapter uses.
type fakeCore struct {
	t *testing.T
	// getblockchaininfo
	ibd    bool
	blocks int64
	// getblocktemplate
	template map[string]any
	// submitblock
	submitResult *string // nil = accepted
	gotBlockHex  string
	failAll      bool
}

func (f *fakeCore) server() *httptest.Server {
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
		if f.failAll {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": req.ID, "result": nil,
				"error": map[string]any{"code": -9, "message": "daemon busted"},
			})
			return
		}
		var result any
		switch req.Method {
		case "getblockchaininfo":
			result = map[string]any{
				"chain": "main", "blocks": f.blocks, "headers": f.blocks,
				"initialblockdownload": f.ibd, "verificationprogress": 1.0,
			}
		case "getblocktemplate":
			result = f.template
		case "submitblock":
			var blockHex string
			if len(req.Params) > 0 {
				_ = json.Unmarshal(req.Params[0], &blockHex)
			}
			f.gotBlockHex = blockHex
			result = f.submitResult
		default:
			f.t.Errorf("unexpected rpc method %q", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": req.ID, "result": result, "error": nil})
	}))
}

func validTemplate() map[string]any {
	return map[string]any{
		"version":           536870912,
		"rules":             []string{"segwit"},
		"previousblockhash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
		"transactions": []map[string]any{
			{"data": "aa", "txid": strings.Repeat("11", 32)},
			{"data": "bb", "txid": strings.Repeat("22", 32)},
		},
		"coinbasevalue":              312500000,
		"mintime":                    1700000000,
		"curtime":                    1700000600,
		"bits":                       "1d00ffff",
		"height":                     840001,
		"default_witness_commitment": "6a24aa21a9ed" + strings.Repeat("cd", 32),
	}
}

func newTestAdapter(t *testing.T, url string) *Adapter {
	t.Helper()
	a, err := New(Options{
		RPC:         rpc.New(rpc.Options{URL: url}),
		Network:     "mainnet",
		PoolAddress: "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
		CoinbaseTag: "/ICMINERS/",
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNewRejectsBadConfig(t *testing.T) {
	client := rpc.New(rpc.Options{URL: "http://127.0.0.1:1"})
	if _, err := New(Options{RPC: nil, PoolAddress: "x"}); err == nil {
		t.Fatal("nil rpc: expected error")
	}
	if _, err := New(Options{RPC: client, Network: "moonnet", PoolAddress: "x"}); err == nil {
		t.Fatal("unknown network: expected error")
	}
	if _, err := New(Options{RPC: client, PoolAddress: "not-an-address"}); err == nil {
		t.Fatal("bad pool address: expected error")
	}
}

func TestHealth(t *testing.T) {
	f := &fakeCore{t: t, blocks: 840000}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)

	if err := a.Health(context.Background()); err != nil {
		t.Fatalf("healthy daemon reported unhealthy: %v", err)
	}

	f.ibd = true
	if err := a.Health(context.Background()); err == nil {
		t.Fatal("IBD daemon must be unhealthy")
	}

	f.failAll = true
	if err := a.Health(context.Background()); err == nil {
		t.Fatal("erroring daemon must be unhealthy")
	}
}

func TestGetBlockTemplateAndBuildJob(t *testing.T) {
	f := &fakeCore{t: t, blocks: 840000, template: validTemplate()}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)
	ctx := context.Background()

	tpl, err := a.GetBlockTemplate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tpl.Height != 840001 || tpl.Bits != "1d00ffff" || tpl.CoinbaseValue != 312500000 {
		t.Fatalf("template = %+v", tpl)
	}

	job, err := a.BuildMiningJob(ctx, tpl, "")
	if err != nil {
		t.Fatal(err)
	}
	if job.Height != 840001 {
		t.Fatalf("job height = %d", job.Height)
	}
	// Network target for diff-1 bits.
	wantTarget := "00000000ffff0000000000000000000000000000000000000000000000000000"
	if hex.EncodeToString(job.NetworkTarget) != wantTarget {
		t.Fatalf("network target = %x", job.NetworkTarget)
	}

	base, err := BitcoinbaseJob(job)
	if err != nil {
		t.Fatal(err)
	}
	if base.Height != 840001 || len(base.MerkleBranchHex) == 0 {
		t.Fatalf("bitcoinbase job = %+v", base)
	}
	// Witness commitment must be inside coinb2.
	if !strings.Contains(base.Coinb2Hex, "6a24aa21a9ed") {
		t.Fatal("witness commitment missing from coinb2")
	}
	// The coinbase must assemble with the configured extranonce widths.
	cb, err := base.CoinbaseParts.Assemble(make([]byte, 4), make([]byte, 4))
	if err != nil {
		t.Fatal(err)
	}
	if len(cb) == 0 {
		t.Fatal("empty assembled coinbase")
	}
}

func TestGetBlockTemplateInvalid(t *testing.T) {
	bad := validTemplate()
	delete(bad, "height")
	f := &fakeCore{t: t, template: bad}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)
	if _, err := a.GetBlockTemplate(context.Background()); err == nil {
		t.Fatal("invalid template: expected error")
	}
}

func TestSubmitBlock(t *testing.T) {
	f := &fakeCore{t: t}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)
	ctx := context.Background()

	if err := a.SubmitBlock(ctx, "00ff"); err != nil {
		t.Fatalf("accepted block reported error: %v", err)
	}
	if f.gotBlockHex != "00ff" {
		t.Fatalf("daemon received %q", f.gotBlockHex)
	}

	reason := "high-hash"
	f.submitResult = &reason
	if err := a.SubmitBlock(ctx, "00ff"); err == nil || !strings.Contains(err.Error(), "high-hash") {
		t.Fatalf("rejected block: err = %v", err)
	}
}

func TestHashHeaderAndAddresses(t *testing.T) {
	f := &fakeCore{t: t}
	srv := f.server()
	defer srv.Close()
	a := newTestAdapter(t, srv.URL)

	if _, err := a.HashHeader(make([]byte, 79)); err == nil {
		t.Fatal("79-byte header: expected error")
	}
	h, err := a.HashHeader(make([]byte, 80))
	if err != nil || len(h) != 32 {
		t.Fatalf("HashHeader: %v, len %d", err, len(h))
	}
	// Must equal package-level SHA256d.
	want := bitcoinbase.DoubleSHA256(make([]byte, 80))
	if hex.EncodeToString(h) != hex.EncodeToString(want) {
		t.Fatal("HashHeader != DoubleSHA256")
	}

	norm, err := a.NormalizeAddress("  BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4 ")
	if err != nil {
		t.Fatal(err)
	}
	if norm != "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4" {
		t.Fatalf("normalized = %q", norm)
	}
	if _, err := a.NormalizeAddress("garbage"); err == nil {
		t.Fatal("garbage address: expected error")
	}
}
