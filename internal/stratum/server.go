// Package stratum wires the protocol codec, sessions, and the pool lifecycle
// manager into a running TCP server. Each configured port is an independent
// listener mapped to exactly one pool; a pool being disabled only affects the
// ports mapped to it.
package stratum

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/bans"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/protocol"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/session"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/vardiff"
)

// Version is reported to miners via client.get_version.
const Version = "GoStratumPool/0.1.0"

// maxMalformedPerConn is how many malformed/oversized lines a single connection
// may send before it is dropped. Full IP banning arrives in Stage 8.
const maxMalformedPerConn = 5

// Server is the stratum TCP front-end.
type Server struct {
	cfg       config.StratumConfig
	log       *logging.Logger
	sessions  *session.Manager
	lifecycle *pool.PoolLifecycleManager
	alloc     *session.ExtraNonce1Allocator
	handler   *Handler
	bans      *bans.Manager

	mu        sync.Mutex
	listeners []net.Listener
	wg        sync.WaitGroup
}

// NewServer builds a stratum server. nodePrefix seeds the extranonce1 allocator
// so different nodes do not collide on shared coins. registry supplies jobs
// and receives shares; it may be nil (handshake-only server, used in tests).
func NewServer(cfg config.StratumConfig, log *logging.Logger, lifecycle *pool.PoolLifecycleManager, nodePrefix []byte, registry *jobs.Registry) *Server {
	l := logging.Component(log, "stratum")
	sm := session.NewManager(cfg.MaxConnPerIP)
	alloc := session.NewExtraNonce1Allocator(4, nodePrefix)
	return &Server{
		cfg:       cfg,
		log:       l,
		sessions:  sm,
		lifecycle: lifecycle,
		alloc:     alloc,
		handler:   newHandlerForRegistry(l, lifecycle, alloc, registry),
	}
}

// newHandlerForRegistry adapts the nil-ness of the registry: a nil *Registry
// must become nil interfaces, not non-nil interfaces holding a nil pointer.
func newHandlerForRegistry(l *logging.Logger, lifecycle *pool.PoolLifecycleManager, alloc *session.ExtraNonce1Allocator, registry *jobs.Registry) *Handler {
	var js JobSource
	var sink ShareSink
	if registry != nil {
		js = registry
		sink = registry
	}
	return NewHandler(l, lifecycle, alloc, js, sink)
}

// SetHandlerCounters wires metrics hooks for share outcomes. Call before Start.
func (s *Server) SetHandlerCounters(c *HandlerCounters) {
	s.handler.Counters = c
}

// SetBanManager wires per-IP banning (nil disables). Call before Start.
func (s *Server) SetBanManager(b *bans.Manager) {
	s.bans = b
	s.handler.bans = b
}

// runVardiffLoop periodically retargets every vardiff session's difficulty
// and pushes mining.set_difficulty on changes.
func (s *Server) runVardiffLoop(ctx context.Context) {
	interval := time.Duration(s.cfg.VarDiffRetargetInterval)
	if interval <= 0 {
		interval = 60 * time.Second
	}
	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			s.sessions.ForEach(func(sess *session.Session) {
				ctrl, ok := sess.Vardiff.(*vardiff.Controller)
				if !ok || ctrl == nil {
					return
				}
				newDiff, changed := ctrl.Retarget(now, sess.Difficulty())
				if !changed {
					return
				}
				sess.UpdateDifficulty(newDiff)
				if err := sess.WriteNotification(&protocol.Notification{
					Method: protocol.MethodSetDifficulty,
					Params: []any{newDiff},
				}); err != nil {
					return
				}
				s.log.Debug("vardiff retarget",
					"session", sess.ID, "pool", sess.PoolID, "newDiff", newDiff)
			})
		}
	}
}

// BroadcastNotify implements jobs.Broadcaster: sends mining.notify to every
// subscribed session on ONE pool. Write failures only drop that session's
// connection state naturally on its next read; other pools are untouched.
func (s *Server) BroadcastNotify(poolID string, params []any) {
	n := &protocol.Notification{Method: protocol.MethodNotify, Params: params}
	sent := 0
	s.sessions.ForEachPool(poolID, func(sess *session.Session) {
		if !sess.IsSubscribed() {
			return
		}
		if err := sess.WriteNotification(n); err != nil {
			s.log.Debug("notify write failed", "pool", poolID, "session", sess.ID, "err", err)
			return
		}
		sent++
	})
	if sent > 0 {
		s.log.Debug("job broadcast", "pool", poolID, "sessions", sent)
	}
}

// Start opens all listeners and serves until ctx is cancelled. It returns once
// every listener is bound (or an error if binding fails); serving continues in
// the background.
func (s *Server) Start(ctx context.Context) error {
	for _, port := range s.cfg.Ports {
		port := port
		addr := fmt.Sprintf("%s:%d", s.cfg.BindAddress, port.Port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			s.closeListeners()
			return fmt.Errorf("listen %s: %w", addr, err)
		}
		s.mu.Lock()
		s.listeners = append(s.listeners, ln)
		s.mu.Unlock()

		s.wg.Add(1)
		go s.acceptLoop(ctx, ln, port)
		s.log.Info("stratum port listening", "addr", addr, "pool", port.PoolID, "vardiff", port.VarDiff)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runVardiffLoop(ctx)
	}()

	// Close listeners when the context is cancelled.
	go func() {
		<-ctx.Done()
		s.closeListeners()
	}()
	return nil
}

// Wait blocks until all accept loops have exited.
func (s *Server) Wait() { s.wg.Wait() }

// SessionCount returns the number of live sessions (for stats).
func (s *Server) SessionCount() int { return s.sessions.Count() }

func (s *Server) closeListeners() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ln := range s.listeners {
		_ = ln.Close()
	}
	s.listeners = nil
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener, port config.PortConfig) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			s.log.Debug("accept error", "err", err)
			return
		}

		// Banned IPs are refused before any protocol work.
		if s.bans != nil {
			host, _, splitErr := net.SplitHostPort(conn.RemoteAddr().String())
			if splitErr == nil && s.bans.IsBanned(host) {
				_ = conn.Close()
				continue
			}
		}

		// If the port's pool is disabled, refuse the connection cleanly.
		if st, err := s.lifecycle.GetPoolState(port.PoolID); err != nil || st == pool.StateDisabled {
			_ = conn.Close()
			continue
		}

		sess := session.NewSession(session.NewSessionID(), port.PoolID, conn)
		startDiff := port.Difficulty
		if port.VarDiff && startDiff <= 0 {
			startDiff = port.MinDiff
		}
		sess.SetDifficulty(startDiff)
		if port.VarDiff {
			sess.Vardiff = vardiff.NewController(vardiff.ControllerConfig{
				MinDiff:          port.MinDiff,
				MaxDiff:          port.MaxDiff,
				TargetInterval:   time.Duration(s.cfg.VarDiffTargetInterval),
				RetargetInterval: time.Duration(s.cfg.VarDiffRetargetInterval),
				VariancePercent:  s.cfg.VarDiffVariancePercent,
			}, time.Now())
		}

		if !s.sessions.Add(sess) {
			s.log.Debug("per-IP connection cap reached", "ip", sess.RemoteIP)
			_ = conn.Close()
			continue
		}

		s.wg.Add(1)
		go s.handleConn(ctx, sess)
	}
}

func (s *Server) handleConn(ctx context.Context, sess *session.Session) {
	defer s.wg.Done()
	defer func() {
		s.sessions.Remove(sess)
		_ = sess.Close()
	}()

	reader := protocol.NewReader(sess.Conn(), s.cfg.MaxLineBytes)
	readTimeout := s.cfg.ReadTimeout.D()
	malformed := 0

	for {
		if ctx.Err() != nil {
			return
		}
		if readTimeout > 0 {
			_ = sess.Conn().SetReadDeadline(time.Now().Add(readTimeout))
		}

		req, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			var malformedErr *protocol.MalformedError
			if errors.Is(err, protocol.ErrLineTooLong) || errors.As(err, &malformedErr) {
				malformed++
				s.log.Debug("malformed input", "ip", sess.RemoteIP, "count", malformed, "err", err)
				if s.bans != nil && s.bans.RecordMalformed(sess.RemoteIP) {
					s.log.Info("dropping connection: IP banned for malformed flood", "ip", sess.RemoteIP)
					return
				}
				if malformed >= maxMalformedPerConn {
					s.log.Info("dropping connection for malformed spam", "ip", sess.RemoteIP)
					return
				}
				continue
			}
			// transport error / deadline
			return
		}
		if req == nil {
			continue // blank keep-alive line
		}

		sess.Touch()
		if err := s.handler.Handle(ctx, sess, req); err != nil {
			s.log.Debug("handler error", "method", req.Method, "err", err)
			return
		}
	}
}
