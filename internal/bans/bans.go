// Package bans tracks per-IP misbehavior — invalid-share ratios, malformed
// protocol floods, repeated failed authorizations — and issues time-limited
// bans that the stratum accept loop enforces. Everything is in-memory and
// bounded; a restart forgives.
package bans

import (
	"sync"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// Config tunes banning.
type Config struct {
	Enabled bool
	// InvalidPercent bans an IP whose invalid-share ratio meets/exceeds this
	// percentage once CheckThreshold shares were seen (default 50).
	InvalidPercent float64
	// CheckThreshold is the minimum observed shares before the ratio is
	// judged (default 50).
	CheckThreshold int
	// MalformedThreshold bans after this many malformed lines (default 5).
	MalformedThreshold int
	// FailedAuthThreshold bans after this many failed authorizations
	// (default 10).
	FailedAuthThreshold int
	// BanDuration is how long a ban lasts (default 10m).
	BanDuration time.Duration
}

func (c *Config) defaults() {
	if c.InvalidPercent <= 0 {
		c.InvalidPercent = 50
	}
	if c.CheckThreshold <= 0 {
		c.CheckThreshold = 50
	}
	if c.MalformedThreshold <= 0 {
		c.MalformedThreshold = 5
	}
	if c.FailedAuthThreshold <= 0 {
		c.FailedAuthThreshold = 10
	}
	if c.BanDuration <= 0 {
		c.BanDuration = 10 * time.Minute
	}
}

// counters accumulate one IP's behavior inside the current judgment window.
type counters struct {
	valid      int
	invalid    int
	malformed  int
	failedAuth int
}

// maxTrackedIPs bounds memory; beyond it, oldest-idle entries are evicted on
// the next janitor pass and new IPs are still ban-checkable (banned map is
// separate and small).
const maxTrackedIPs = 100000

// Manager is the ban registry. Safe for concurrent use.
type Manager struct {
	cfg Config
	log *logging.Logger

	mu     sync.Mutex
	banned map[string]time.Time // ip -> expiry
	stats  map[string]*counters
	now    func() time.Time // test seam
}

// NewManager builds a Manager (works, but never bans, when disabled).
func NewManager(cfg Config, log *logging.Logger) *Manager {
	cfg.defaults()
	return &Manager{
		cfg:    cfg,
		log:    logging.Component(log, "bans"),
		banned: make(map[string]time.Time),
		stats:  make(map[string]*counters),
		now:    time.Now,
	}
}

// IsBanned reports whether the IP is currently banned. Expired bans are
// removed lazily.
func (m *Manager) IsBanned(ip string) bool {
	if !m.cfg.Enabled {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	exp, ok := m.banned[ip]
	if !ok {
		return false
	}
	if m.now().After(exp) {
		delete(m.banned, ip)
		return false
	}
	return true
}

// Ban bans an IP for the configured duration.
func (m *Manager) Ban(ip, reason string) {
	if !m.cfg.Enabled {
		return
	}
	m.mu.Lock()
	m.banned[ip] = m.now().Add(m.cfg.BanDuration)
	delete(m.stats, ip)
	m.mu.Unlock()
	m.log.Warn("IP banned", "ip", ip, "reason", reason, "duration", m.cfg.BanDuration)
}

func (m *Manager) get(ip string) *counters {
	c, ok := m.stats[ip]
	if !ok {
		if len(m.stats) >= maxTrackedIPs {
			// Bounded: drop tracking for the newcomer rather than growing.
			return nil
		}
		c = &counters{}
		m.stats[ip] = c
	}
	return c
}

// RecordShare records a share outcome and applies the invalid-ratio rule.
func (m *Manager) RecordShare(ip string, valid bool) {
	if !m.cfg.Enabled {
		return
	}
	m.mu.Lock()
	c := m.get(ip)
	if c == nil {
		m.mu.Unlock()
		return
	}
	if valid {
		c.valid++
	} else {
		c.invalid++
	}
	total := c.valid + c.invalid
	shouldBan := false
	if total >= m.cfg.CheckThreshold {
		ratio := float64(c.invalid) / float64(total) * 100
		if ratio >= m.cfg.InvalidPercent {
			shouldBan = true
		} else {
			// Window judged clean: reset so an old good history cannot mask a
			// later attack forever.
			c.valid, c.invalid = 0, 0
		}
	}
	m.mu.Unlock()
	if shouldBan {
		m.Ban(ip, "invalid share ratio")
	}
}

// RecordMalformed records one malformed protocol line; returns true when the
// IP just got banned (caller should drop the connection).
func (m *Manager) RecordMalformed(ip string) bool {
	if !m.cfg.Enabled {
		return false
	}
	m.mu.Lock()
	c := m.get(ip)
	if c == nil {
		m.mu.Unlock()
		return false
	}
	c.malformed++
	over := c.malformed >= m.cfg.MalformedThreshold
	m.mu.Unlock()
	if over {
		m.Ban(ip, "malformed flood")
	}
	return over
}

// RecordFailedAuth records one failed authorization; returns true when the IP
// just got banned.
func (m *Manager) RecordFailedAuth(ip string) bool {
	if !m.cfg.Enabled {
		return false
	}
	m.mu.Lock()
	c := m.get(ip)
	if c == nil {
		m.mu.Unlock()
		return false
	}
	c.failedAuth++
	over := c.failedAuth >= m.cfg.FailedAuthThreshold
	m.mu.Unlock()
	if over {
		m.Ban(ip, "failed authorizations")
	}
	return over
}

// BannedCount returns the number of active bans (metrics).
func (m *Manager) BannedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	n := 0
	for ip, exp := range m.banned {
		if now.After(exp) {
			delete(m.banned, ip)
			continue
		}
		n++
	}
	return n
}
