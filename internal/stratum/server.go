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

	"github.com/icminers/gostratumpool/internal/config"
	"github.com/icminers/gostratumpool/internal/logging"
	"github.com/icminers/gostratumpool/internal/pool"
	"github.com/icminers/gostratumpool/internal/stratum/protocol"
	"github.com/icminers/gostratumpool/internal/stratum/session"
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

	mu        sync.Mutex
	listeners []net.Listener
	wg        sync.WaitGroup
}

// NewServer builds a stratum server. nodePrefix seeds the extranonce1 allocator
// so different nodes do not collide on shared coins.
func NewServer(cfg config.StratumConfig, log *logging.Logger, lifecycle *pool.PoolLifecycleManager, nodePrefix []byte) *Server {
	l := logging.Component(log, "stratum")
	sm := session.NewManager(cfg.MaxConnPerIP)
	alloc := session.NewExtraNonce1Allocator(4, nodePrefix)
	return &Server{
		cfg:       cfg,
		log:       l,
		sessions:  sm,
		lifecycle: lifecycle,
		alloc:     alloc,
		handler:   NewHandler(l, lifecycle, alloc),
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
