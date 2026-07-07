package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const validConfig = `{
  "mode": "all-in-one",
  "region": "us",
  "nodeId": "us-1",
  "logging": {"level": "debug"},
  "stratum": {
    "bindAddress": "0.0.0.0",
    "readTimeout": "5m",
    "ports": [
      {"port": 3032, "poolId": "flowcoin-shared", "difficulty": 1024},
      {"port": 3033, "poolId": "flowcoin-solo", "varDiff": true, "minDiff": 8, "maxDiff": 65536}
    ]
  },
  "pools": [
    {"id": "flowcoin-shared", "enabled": true, "paymentMode": "pplns", "initialState": "active"},
    {"id": "flowcoin-solo", "enabled": true, "paymentMode": "solo"}
  ]
}`

func TestLoadValid(t *testing.T) {
	cfg, err := Load(writeTemp(t, validConfig))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Mode != ModeAllInOne {
		t.Errorf("mode = %q", cfg.Mode)
	}
	if len(cfg.Pools) != 2 || len(cfg.Stratum.Ports) != 2 {
		t.Fatalf("unexpected pool/port counts")
	}
	if cfg.Stratum.ReadTimeout.D() != 5*time.Minute {
		t.Errorf("readTimeout = %v", cfg.Stratum.ReadTimeout.D())
	}
	// Default initial state applied to the solo pool.
	solo, ok := cfg.PoolByID("flowcoin-solo")
	if !ok || solo.InitialState != StateActive {
		t.Errorf("expected default active initial state, got %q", solo.InitialState)
	}
	// Default line cap applied.
	if cfg.Stratum.MaxLineBytes == 0 {
		t.Error("expected default maxLineBytes")
	}
}

func TestRejectsPortToUnknownPool(t *testing.T) {
	body := `{
      "mode":"all-in-one",
      "stratum":{"ports":[{"port":3032,"poolId":"ghost","difficulty":1}]},
      "pools":[{"id":"real","enabled":true}]
    }`
	if _, err := Load(writeTemp(t, body)); err == nil {
		t.Fatal("expected error for port mapping to unknown pool")
	}
}

func TestRejectsDuplicatePort(t *testing.T) {
	body := `{
      "mode":"all-in-one",
      "stratum":{"ports":[
        {"port":3032,"poolId":"a","difficulty":1},
        {"port":3032,"poolId":"a","difficulty":1}
      ]},
      "pools":[{"id":"a","enabled":true}]
    }`
	if _, err := Load(writeTemp(t, body)); err == nil {
		t.Fatal("expected error for duplicate port")
	}
}

func TestRejectsBadPaymentMode(t *testing.T) {
	body := `{
      "mode":"all-in-one",
      "pools":[{"id":"a","enabled":true,"paymentMode":"lottery"}]
    }`
	if _, err := Load(writeTemp(t, body)); err == nil {
		t.Fatal("expected error for invalid payment mode")
	}
}

func TestRejectsBadVardiffRange(t *testing.T) {
	body := `{
      "mode":"all-in-one",
      "stratum":{"ports":[{"port":3032,"poolId":"a","varDiff":true,"minDiff":100,"maxDiff":10}]},
      "pools":[{"id":"a","enabled":true}]
    }`
	if _, err := Load(writeTemp(t, body)); err == nil {
		t.Fatal("expected error for min>max vardiff")
	}
}

func TestRegionalRequiresRegion(t *testing.T) {
	body := `{"mode":"regional","pools":[{"id":"a","enabled":true}]}`
	if _, err := Load(writeTemp(t, body)); err == nil {
		t.Fatal("expected error: regional mode requires region")
	}
}
