package session

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// Manager tracks all live sessions and enforces per-IP connection caps.
type Manager struct {
	mu           sync.RWMutex
	sessions     map[string]*Session
	perIP        map[string]int
	maxConnPerIP int
}

// NewManager builds a session manager. maxConnPerIP <= 0 disables the cap.
func NewManager(maxConnPerIP int) *Manager {
	return &Manager{
		sessions:     make(map[string]*Session),
		perIP:        make(map[string]int),
		maxConnPerIP: maxConnPerIP,
	}
}

// Add registers a session. It returns false if the per-IP cap would be
// exceeded, in which case the caller should close the connection.
func (m *Manager) Add(s *Session) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maxConnPerIP > 0 && m.perIP[s.RemoteIP] >= m.maxConnPerIP {
		return false
	}
	m.sessions[s.ID] = s
	m.perIP[s.RemoteIP]++
	return true
}

// Remove deregisters a session.
func (m *Manager) Remove(s *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[s.ID]; !ok {
		return
	}
	delete(m.sessions, s.ID)
	if n := m.perIP[s.RemoteIP]; n <= 1 {
		delete(m.perIP, s.RemoteIP)
	} else {
		m.perIP[s.RemoteIP] = n - 1
	}
}

// Count returns the number of live sessions.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// CountForPool returns the number of live sessions on a given pool.
func (m *Manager) CountForPool(poolID string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, s := range m.sessions {
		if s.PoolID == poolID {
			n++
		}
	}
	return n
}

// ForEachPool runs fn for every live session on the given pool. Used to close a
// single pool's sessions (e.g. on pause) without touching other pools.
// ForEach invokes fn for every live session across all pools.
func (m *Manager) ForEach(fn func(*Session)) {
	m.mu.RLock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.RUnlock()
	for _, s := range sessions {
		fn(s)
	}
}

func (m *Manager) ForEachPool(poolID string, fn func(*Session)) {
	m.mu.RLock()
	targets := make([]*Session, 0)
	for _, s := range m.sessions {
		if s.PoolID == poolID {
			targets = append(targets, s)
		}
	}
	m.mu.RUnlock()
	for _, s := range targets {
		fn(s)
	}
}

// NewSessionID returns a random 16-hex-char session id.
func NewSessionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
