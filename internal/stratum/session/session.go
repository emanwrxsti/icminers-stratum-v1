// Package session tracks stratum miner connections and their per-connection
// state (subscription, authorized workers, current difficulty). It contains no
// coin logic.
package session

import (
	"net"
	"sync"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/protocol"
)

// Session is the server-side state for one miner TCP connection.
type Session struct {
	// ID is a process-unique session id.
	ID string
	// PoolID is the pool this connection's port maps to.
	PoolID string
	// RemoteIP is the miner's source address (host only).
	RemoteIP string

	conn   net.Conn
	writer *protocol.Writer

	mu           sync.RWMutex
	subscribed   bool
	extraNonce1  string
	difficulty   float64
	authWorkers  map[string]bool
	userAgent    string
	connectedAt  time.Time
	lastActivity time.Time

	// writeMu serializes writes so notifications and responses never interleave
	// on the wire.
	writeMu sync.Mutex
}

// NewSession builds a session around an accepted connection.
func NewSession(id, poolID string, conn net.Conn) *Session {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		host = conn.RemoteAddr().String()
	}
	now := time.Now()
	return &Session{
		ID:           id,
		PoolID:       poolID,
		RemoteIP:     host,
		conn:         conn,
		writer:       protocol.NewWriter(conn),
		authWorkers:  make(map[string]bool),
		connectedAt:  now,
		lastActivity: now,
	}
}

// Subscribe marks the session subscribed and records its extranonce1.
func (s *Session) Subscribe(extraNonce1 string) {
	s.mu.Lock()
	s.subscribed = true
	s.extraNonce1 = extraNonce1
	s.mu.Unlock()
}

// IsSubscribed reports subscription status.
func (s *Session) IsSubscribed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subscribed
}

// ExtraNonce1 returns the assigned extranonce1.
func (s *Session) ExtraNonce1() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.extraNonce1
}

// Authorize records a worker as authorized on this session.
func (s *Session) Authorize(worker string) {
	s.mu.Lock()
	s.authWorkers[worker] = true
	s.mu.Unlock()
}

// IsAuthorized reports whether the given worker is authorized.
func (s *Session) IsAuthorized(worker string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authWorkers[worker]
}

// HasAnyAuthorized reports whether at least one worker is authorized.
func (s *Session) HasAnyAuthorized() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.authWorkers) > 0
}

// SetDifficulty records the session's current difficulty.
func (s *Session) SetDifficulty(d float64) {
	s.mu.Lock()
	s.difficulty = d
	s.mu.Unlock()
}

// Difficulty returns the session's current difficulty.
func (s *Session) Difficulty() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.difficulty
}

// UserAgent returns the miner's reported user agent.
func (s *Session) UserAgent() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.userAgent
}

// SetUserAgent stores the miner's reported user agent.
func (s *Session) SetUserAgent(ua string) {
	s.mu.Lock()
	s.userAgent = ua
	s.mu.Unlock()
}

// Touch updates the last-activity timestamp.
func (s *Session) Touch() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

// WriteResponse writes a JSON-RPC response, serialized against other writes.
func (s *Session) WriteResponse(resp *protocol.Response) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writer.WriteResponse(resp)
}

// WriteNotification writes a server notification, serialized against other
// writes.
func (s *Session) WriteNotification(n *protocol.Notification) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writer.WriteNotification(n)
}

// Conn exposes the underlying connection (for deadlines/close).
func (s *Session) Conn() net.Conn { return s.conn }

// Close closes the underlying connection.
func (s *Session) Close() error { return s.conn.Close() }
