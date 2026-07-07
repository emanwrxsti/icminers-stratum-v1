// Package api serves the pool's HTTP interface: public endpoints (health,
// pools, live stats, DB-backed blocks/miners) and token-protected admin
// endpoints that drive the per-pool lifecycle manager — pause, resume, drain,
// maintenance, restart, disable — exactly as the isolation spec requires:
// every action targets ONE pool and cannot touch the others.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/config"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stats"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/storage/postgres"
)

// Options wires the server's collaborators.
type Options struct {
	Config *config.Config
	// Lifecycle drives per-pool admin actions and reports states.
	Lifecycle *pool.PoolLifecycleManager
	// Stats supplies live in-memory counters (may be nil).
	Stats *stats.Collector
	// Store supplies DB-backed history (may be nil: those endpoints 503).
	Store *postgres.Store
	// SessionCount reports live stratum connections (may be nil).
	SessionCount func() int
	// AdminToken guards /api/admin; empty disables admin routes entirely.
	AdminToken string

	// PublishCommand, when set (master / all-in-one with NATS), fans every
	// successful admin action out to the cluster so regional nodes apply it
	// to the same single pool. Signature: (poolID, action, message, graceSeconds).
	PublishCommand func(poolID, action, message string, graceSeconds int) error

	Log *logging.Logger
}

// Server is the HTTP API.
type Server struct {
	opts    Options
	mux     *http.ServeMux
	httpSrv *http.Server
	started time.Time
}

// New builds the server and its routes.
func New(opts Options) *Server {
	s := &Server{opts: opts, mux: http.NewServeMux(), started: time.Now()}
	s.routes()
	return s
}

// Handler exposes the mux (tests hit this directly).
func (s *Server) Handler() http.Handler { return s.mux }

// Start listens on bind until ctx is canceled. Non-blocking; failure to bind
// is returned immediately.
func (s *Server) Start(ctx context.Context, bind string) error {
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return fmt.Errorf("api: listen %s: %w", bind, err)
	}
	s.httpSrv = &http.Server{
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(shCtx)
	}()
	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.opts.Log.Error("api server error", "err", err)
		}
	}()
	s.opts.Log.Info("api listening", "bind", bind, "admin", s.opts.AdminToken != "")
	return nil
}

func (s *Server) routes() {
	// Public.
	s.mux.HandleFunc("GET /api/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/pools", s.handlePools)
	s.mux.HandleFunc("GET /api/pools/{id}", s.handlePool)
	s.mux.HandleFunc("GET /api/pools/{id}/blocks", s.handlePoolBlocks)
	s.mux.HandleFunc("GET /api/pools/{id}/miners", s.handlePoolMiners)
	s.mux.HandleFunc("GET /api/pools/{id}/miners/{miner}", s.handlePoolMiner)

	// Admin (per-pool lifecycle). Registered only when a token is configured.
	if s.opts.AdminToken != "" {
		admin := func(fn http.HandlerFunc) http.HandlerFunc { return s.requireAdmin(fn) }
		s.mux.HandleFunc("POST /api/admin/pools/{id}/pause", admin(s.adminAction(actionPause)))
		s.mux.HandleFunc("POST /api/admin/pools/{id}/resume", admin(s.adminAction(actionResume)))
		s.mux.HandleFunc("POST /api/admin/pools/{id}/drain", admin(s.adminAction(actionDrain)))
		s.mux.HandleFunc("POST /api/admin/pools/{id}/maintenance", admin(s.adminAction(actionMaintenance)))
		s.mux.HandleFunc("POST /api/admin/pools/{id}/restart", admin(s.adminAction(actionRestart)))
		s.mux.HandleFunc("POST /api/admin/pools/{id}/disable", admin(s.adminAction(actionDisable)))
		s.mux.HandleFunc("GET /api/admin/pools/{id}/state", admin(s.adminState))
	}
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errBody struct {
	Error string `json:"error"`
}

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		want := "Bearer " + s.opts.AdminToken
		if subtle.ConstantTimeCompare([]byte(auth), []byte(want)) != 1 {
			writeJSON(w, http.StatusUnauthorized, errBody{"unauthorized"})
			return
		}
		next(w, r)
	}
}

// --- public handlers ---

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	sessions := 0
	if s.opts.SessionCount != nil {
		sessions = s.opts.SessionCount()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"uptimeSeconds": int(time.Since(s.started).Seconds()),
		"region":        s.opts.Config.Region,
		"nodeId":        s.opts.Config.NodeID,
		"pools":         len(s.opts.Config.Pools),
		"sessions":      sessions,
		"database":      s.opts.Store != nil,
	})
}

// poolView is the public shape of one pool.
type poolView struct {
	ID                 string          `json:"id"`
	CoinSymbol         string          `json:"coinSymbol"`
	PaymentMode        string          `json:"paymentMode"`
	PoolFeePercent     float64         `json:"poolFeePercent"`
	State              string          `json:"state"`
	MaintenanceMessage string          `json:"maintenanceMessage,omitempty"`
	Ports              []portView      `json:"ports"`
	Live               stats.PoolStats `json:"live"`
}

type portView struct {
	Port       int     `json:"port"`
	VarDiff    bool    `json:"varDiff"`
	Difficulty float64 `json:"difficulty"`
}

func (s *Server) poolView(p config.PoolConfig) poolView {
	v := poolView{
		ID:             p.ID,
		CoinSymbol:     p.CoinSymbol,
		PaymentMode:    p.PaymentMode,
		PoolFeePercent: p.PoolFeePercent,
	}
	if st, err := s.opts.Lifecycle.GetPoolState(p.ID); err == nil {
		v.State = string(st)
		if st == pool.StateMaintenance {
			v.MaintenanceMessage = s.opts.Lifecycle.MaintenanceMessage(p.ID)
		}
	}
	for _, port := range s.opts.Config.Stratum.Ports {
		if port.PoolID == p.ID {
			v.Ports = append(v.Ports, portView{
				Port: port.Port, VarDiff: port.VarDiff, Difficulty: port.Difficulty,
			})
		}
	}
	if s.opts.Stats != nil {
		v.Live = s.opts.Stats.Pool(p.ID)
	}
	return v
}

func (s *Server) handlePools(w http.ResponseWriter, r *http.Request) {
	out := make([]poolView, 0, len(s.opts.Config.Pools))
	for _, p := range s.opts.Config.Pools {
		out = append(out, s.poolView(p))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findPool(id string) (config.PoolConfig, bool) {
	for _, p := range s.opts.Config.Pools {
		if p.ID == id {
			return p, true
		}
	}
	return config.PoolConfig{}, false
}

func (s *Server) handlePool(w http.ResponseWriter, r *http.Request) {
	p, ok := s.findPool(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, errBody{"unknown pool"})
		return
	}
	writeJSON(w, http.StatusOK, s.poolView(p))
}

func (s *Server) handlePoolBlocks(w http.ResponseWriter, r *http.Request) {
	p, ok := s.findPool(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, errBody{"unknown pool"})
		return
	}
	if s.opts.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, errBody{"database not configured"})
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	blocks, err := s.opts.Store.ListBlocks(r.Context(), p.ID, limit, offset)
	if err != nil {
		s.opts.Log.Error("list blocks failed", "pool", p.ID, "err", err)
		writeJSON(w, http.StatusInternalServerError, errBody{"query failed"})
		return
	}
	writeJSON(w, http.StatusOK, blocks)
}

func (s *Server) handlePoolMiners(w http.ResponseWriter, r *http.Request) {
	p, ok := s.findPool(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, errBody{"unknown pool"})
		return
	}
	if s.opts.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, errBody{"database not configured"})
		return
	}
	window := parseWindow(r.URL.Query().Get("window"), time.Hour)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	miners, err := s.opts.Store.TopMiners(r.Context(), p.ID, window, limit)
	if err != nil {
		s.opts.Log.Error("top miners failed", "pool", p.ID, "err", err)
		writeJSON(w, http.StatusInternalServerError, errBody{"query failed"})
		return
	}
	writeJSON(w, http.StatusOK, miners)
}

func (s *Server) handlePoolMiner(w http.ResponseWriter, r *http.Request) {
	p, ok := s.findPool(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, errBody{"unknown pool"})
		return
	}
	miner := r.PathValue("miner")
	out := map[string]any{"miner": miner}
	if s.opts.Stats != nil {
		if live, ok := s.opts.Stats.Miner(p.ID, miner); ok {
			out["live"] = live
		}
	}
	if s.opts.Store != nil {
		window := parseWindow(r.URL.Query().Get("window"), time.Hour)
		workers, err := s.opts.Store.MinerWorkers(r.Context(), p.ID, miner, window)
		if err != nil {
			s.opts.Log.Error("miner workers failed", "pool", p.ID, "err", err)
			writeJSON(w, http.StatusInternalServerError, errBody{"query failed"})
			return
		}
		out["workers"] = workers
	}
	writeJSON(w, http.StatusOK, out)
}

func parseWindow(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 || d > 30*24*time.Hour {
		return def
	}
	return d
}

// --- admin handlers ---

type adminActionKind int

const (
	actionPause adminActionKind = iota
	actionResume
	actionDrain
	actionMaintenance
	actionRestart
	actionDisable
)

// name returns the wire name of an admin action.
func (k adminActionKind) name() string {
	switch k {
	case actionPause:
		return "pause"
	case actionResume:
		return "resume"
	case actionDrain:
		return "drain"
	case actionMaintenance:
		return "maintenance"
	case actionRestart:
		return "restart"
	case actionDisable:
		return "disable"
	}
	return "unknown"
}

type adminRequestBody struct {
	// GracePeriodSeconds applies to drain (default 60).
	GracePeriodSeconds int `json:"gracePeriodSeconds"`
	// Message applies to maintenance.
	Message string `json:"message"`
}

func (s *Server) adminAction(kind adminActionKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolID := r.PathValue("id")
		var body adminRequestBody
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body) // empty body is fine
		}
		var err error
		switch kind {
		case actionPause:
			err = s.opts.Lifecycle.PausePool(poolID)
		case actionResume:
			err = s.opts.Lifecycle.ResumePool(poolID)
		case actionDrain:
			grace := time.Duration(body.GracePeriodSeconds) * time.Second
			if grace <= 0 {
				grace = 60 * time.Second
			}
			err = s.opts.Lifecycle.DrainPool(poolID, grace)
		case actionMaintenance:
			msg := strings.TrimSpace(body.Message)
			if msg == "" {
				msg = "pool is under maintenance"
			}
			err = s.opts.Lifecycle.PutPoolInMaintenance(poolID, msg)
		case actionRestart:
			err = s.opts.Lifecycle.RestartPool(poolID)
		case actionDisable:
			err = s.opts.Lifecycle.DisablePool(poolID)
		}
		// When this node is the command authority (publisher configured), the
		// command is ALWAYS fanned out to the cluster; local apply is
		// best-effort because the master may not host the pool (or hold it
		// disabled) while regionals run it.
		published := false
		if s.opts.PublishCommand != nil {
			if perr := s.opts.PublishCommand(poolID, kind.name(), body.Message, body.GracePeriodSeconds); perr != nil {
				s.opts.Log.Error("command publish failed", "pool", poolID, "action", kind.name(), "err", perr)
			} else {
				published = true
			}
		}
		if err != nil {
			if published {
				s.opts.Log.Info("admin action published cluster-wide (not applicable locally)",
					"pool", poolID, "action", kind.name(), "localErr", err)
				writeJSON(w, http.StatusAccepted, map[string]any{
					"poolId": poolID, "action": kind.name(),
					"published": true, "localError": err.Error(),
				})
				return
			}
			status := http.StatusConflict
			var unknown pool.ErrUnknownPool
			if errors.As(err, &unknown) {
				status = http.StatusNotFound
			}
			writeJSON(w, status, errBody{err.Error()})
			return
		}
		s.opts.Log.Info("admin action", "pool", poolID, "action", kind.name(),
			"published", published, "remote", r.RemoteAddr)
		s.adminStateReply(w, poolID)
	}
}

func (s *Server) adminState(w http.ResponseWriter, r *http.Request) {
	s.adminStateReply(w, r.PathValue("id"))
}

func (s *Server) adminStateReply(w http.ResponseWriter, poolID string) {
	st, err := s.opts.Lifecycle.GetPoolState(poolID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody{err.Error()})
		return
	}
	out := map[string]any{"poolId": poolID, "state": string(st)}
	if st == pool.StateMaintenance {
		out["maintenanceMessage"] = s.opts.Lifecycle.MaintenanceMessage(poolID)
	}
	writeJSON(w, http.StatusOK, out)
}
