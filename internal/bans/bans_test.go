package bans

import (
	"testing"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

func newTestManager(cfg Config) (*Manager, func(d time.Duration)) {
	cfg.Enabled = true
	m := NewManager(cfg, logging.New(logging.Options{Level: "error"}))
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }
	advance := func(d time.Duration) { now = now.Add(d) }
	return m, advance
}

func TestInvalidRatioBan(t *testing.T) {
	m, _ := newTestManager(Config{CheckThreshold: 10, InvalidPercent: 50})
	ip := "10.0.0.1"
	// 4 valid + 6 invalid over threshold 10 -> 60% >= 50% -> ban.
	for i := 0; i < 4; i++ {
		m.RecordShare(ip, true)
	}
	if m.IsBanned(ip) {
		t.Fatal("banned too early")
	}
	for i := 0; i < 6; i++ {
		m.RecordShare(ip, false)
	}
	if !m.IsBanned(ip) {
		t.Fatal("invalid ratio did not ban")
	}
	// A healthy miner never bans: 9 valid 1 invalid, window resets clean.
	good := "10.0.0.2"
	for round := 0; round < 5; round++ {
		for i := 0; i < 9; i++ {
			m.RecordShare(good, true)
		}
		m.RecordShare(good, false)
	}
	if m.IsBanned(good) {
		t.Fatal("healthy miner banned")
	}
}

func TestMalformedAndFailedAuthThresholds(t *testing.T) {
	m, _ := newTestManager(Config{MalformedThreshold: 3, FailedAuthThreshold: 2})
	ip := "10.0.0.3"
	if m.RecordMalformed(ip) || m.RecordMalformed(ip) {
		t.Fatal("banned before threshold")
	}
	if !m.RecordMalformed(ip) {
		t.Fatal("threshold did not ban")
	}
	if !m.IsBanned(ip) {
		t.Fatal("not banned after malformed flood")
	}

	ip2 := "10.0.0.4"
	if m.RecordFailedAuth(ip2) {
		t.Fatal("banned on first failed auth")
	}
	if !m.RecordFailedAuth(ip2) || !m.IsBanned(ip2) {
		t.Fatal("failed-auth threshold did not ban")
	}
}

func TestBanExpiry(t *testing.T) {
	m, advance := newTestManager(Config{BanDuration: 10 * time.Minute})
	m.Ban("10.0.0.5", "test")
	if !m.IsBanned("10.0.0.5") {
		t.Fatal("not banned")
	}
	if m.BannedCount() != 1 {
		t.Fatalf("count = %d", m.BannedCount())
	}
	advance(11 * time.Minute)
	if m.IsBanned("10.0.0.5") {
		t.Fatal("ban did not expire")
	}
	if m.BannedCount() != 0 {
		t.Fatalf("count after expiry = %d", m.BannedCount())
	}
}

func TestDisabledNeverBans(t *testing.T) {
	m := NewManager(Config{Enabled: false}, logging.New(logging.Options{Level: "error"}))
	for i := 0; i < 1000; i++ {
		m.RecordShare("ip", false)
		m.RecordMalformed("ip")
		m.RecordFailedAuth("ip")
	}
	m.Ban("ip", "manual")
	if m.IsBanned("ip") {
		t.Fatal("disabled manager banned")
	}
}
