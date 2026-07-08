package stratum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/bans"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/coins"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/protocol"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/session"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/stratum/vardiff"
)

// diffGraceWindow: after a vardiff RAISE, shares mined against the previous
// (lower) difficulty are still honored for this long.
const diffGraceWindow = 8 * time.Second

// JobSource supplies the current mining.notify parameters for a pool.
// Implemented by the jobs registry; nil means no job wiring yet.
type JobSource interface {
	CurrentNotify(poolID string) ([]any, bool)
}

// Handler dispatches inbound JSON-RPC requests. It is stateless per request and
// safe for concurrent use; all mutable state lives on the Session or the
// lifecycle manager.
type Handler struct {
	log       *logging.Logger
	lifecycle *pool.PoolLifecycleManager
	alloc     *session.ExtraNonce1Allocator
	jobSource JobSource
	shareSink ShareSink
	bans      *bans.Manager

	// Counters is an optional metrics hook (set by main).
	Counters *HandlerCounters
}

// HandlerCounters are optional share-outcome hooks for metrics.
type HandlerCounters struct {
	ShareAccepted func(poolID string)
	ShareRejected func(poolID string)
	BlockFound    func(poolID string)
}

// NewHandler builds a handler. jobSource/shareSink may be nil (no jobs wired).
func NewHandler(log *logging.Logger, lifecycle *pool.PoolLifecycleManager, alloc *session.ExtraNonce1Allocator, jobSource JobSource, shareSink ShareSink) *Handler {
	return &Handler{log: logging.Component(log, "handler"), lifecycle: lifecycle, alloc: alloc, jobSource: jobSource, shareSink: shareSink}
}

// Handle processes one request. A returned error means the connection should be
// closed; protocol-level failures are sent as JSON-RPC error responses and
// return nil.
func (h *Handler) Handle(ctx context.Context, sess *session.Session, req *protocol.Request) error {
	switch req.Method {
	case protocol.MethodSubscribe:
		return h.handleSubscribe(sess, req)
	case protocol.MethodAuthorize:
		return h.handleAuthorize(sess, req)
	case protocol.MethodConfigure:
		return h.handleConfigure(sess, req)
	case protocol.MethodGetVersion:
		return sess.WriteResponse(protocol.OKResponse(req.ID, Version))
	case protocol.MethodSubmit:
		return h.handleSubmit(ctx, sess, req)
	default:
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, "unknown method "+req.Method)))
	}
}

// handleSubscribe assigns an extranonce1 and returns the subscription details.
// Response shape: [[["mining.set_difficulty", subId],["mining.notify", subId]], extranonce1, extranonce2Size]
func (h *Handler) handleSubscribe(sess *session.Session, req *protocol.Request) error {
	// Params (optional): [userAgent, sessionId]
	var params []json.RawMessage
	if len(req.Params) > 0 {
		_ = json.Unmarshal(req.Params, &params)
	}
	if len(params) > 0 {
		var ua string
		if json.Unmarshal(params[0], &ua) == nil {
			sess.SetUserAgent(ua)
		}
	}

	en1 := h.alloc.Next()
	sess.Subscribe(en1)

	subDetails := []any{
		[]any{protocol.MethodSetDifficulty, sess.ID},
		[]any{protocol.MethodNotify, sess.ID},
	}
	result := []any{subDetails, en1, session.ExtraNonce2Size}
	return sess.WriteResponse(protocol.OKResponse(req.ID, result))
}

// handleAuthorize authorizes a worker, gated by the pool's lifecycle state.
// Params: [username, password]. username is typically "address.worker".
func (h *Handler) handleAuthorize(sess *session.Session, req *protocol.Request) error {
	if !sess.IsSubscribed() {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrNotSubscribed, "not subscribed")))
	}

	var params []string
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) < 1 {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, "invalid authorize params")))
	}
	worker := strings.TrimSpace(params[0])
	if worker == "" {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrUnauthorized, "empty worker")))
	}

	// Lifecycle gate: only active pools accept new authorizations. Maintenance
	// returns the configured maintenance message; other non-active states get a
	// clear reason. This only ever consults THIS pool's state.
	ok, err := h.lifecycle.AcceptsNewAuthorization(sess.PoolID)
	if err != nil {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, "unknown pool")))
	}
	if !ok {
		state, _ := h.lifecycle.GetPoolState(sess.PoolID)
		msg := h.rejectMessage(sess.PoolID, state)
		h.log.Debug("authorization rejected by lifecycle", "pool", sess.PoolID, "state", state, "worker", worker)
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrUnauthorized, msg)))
	}

	sess.Authorize(worker)

	if err := sess.WriteResponse(protocol.OKResponse(req.ID, true)); err != nil {
		return err
	}
	// Push the current difficulty, then the current job (if the pool has one)
	// so the miner can start hashing immediately.
	if err := h.sendSetDifficulty(sess, sess.Difficulty()); err != nil {
		return err
	}
	if h.jobSource != nil {
		if params, ok := h.jobSource.CurrentNotify(sess.PoolID); ok {
			return sess.WriteNotification(&protocol.Notification{
				Method: protocol.MethodNotify,
				Params: params,
			})
		}
	}
	return nil
}

// handleConfigure implements a minimal mining.configure for version-rolling
// (ASICBoost). We acknowledge the extension without enabling mask negotiation
// beyond echoing a conservative mask; full support arrives with share
// validation in Stage 3.
// Params: [ [extensions...], {params} ]
func (h *Handler) handleConfigure(sess *session.Session, req *protocol.Request) error {
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) < 1 {
		return sess.WriteResponse(protocol.OKResponse(req.ID, map[string]any{}))
	}
	var extensions []string
	_ = json.Unmarshal(params[0], &extensions)

	result := map[string]any{}
	for _, ext := range extensions {
		if ext == "version-rolling" {
			result["version-rolling"] = true
			result["version-rolling.mask"] = "1fffe000"
		}
	}
	return sess.WriteResponse(protocol.OKResponse(req.ID, result))
}

// ShareSink validates submitted shares. Implemented by the jobs registry.
type ShareSink interface {
	SubmitShare(ctx context.Context, poolID string, submit coins.ShareSubmit) (*coins.ShareResult, error)
}

// handleSubmit validates a share against the pool's remembered jobs.
// Params: [workerName, jobId, extranonce2, ntime, nonce, (versionBits)].
// The reply is computed entirely in memory; block submission and (Stage 4)
// persistence happen off this path.
func (h *Handler) handleSubmit(ctx context.Context, sess *session.Session, req *protocol.Request) error {
	if !sess.IsSubscribed() {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrNotSubscribed, "not subscribed")))
	}
	var params []string
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) < 5 {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, "invalid submit params")))
	}
	worker := strings.TrimSpace(params[0])
	if !sess.IsAuthorized(worker) {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrUnauthorized, "worker not authorized")))
	}
	// Reject shares for pools not in a share-accepting state (active/draining).
	state, err := h.lifecycle.GetPoolState(sess.PoolID)
	if err != nil {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, "unknown pool")))
	}
	if !state.AcceptsShares() {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, h.rejectMessage(sess.PoolID, state))))
	}
	if h.shareSink == nil {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrJobNotFound, "no job source wired for this pool")))
	}

	submit := coins.ShareSubmit{
		Worker:      worker,
		JobID:       params[1],
		ExtraNonce2: params[2],
		NTime:       params[3],
		Nonce:       params[4],
		ExtraNonce1: sess.ExtraNonce1(),
		WorkerDiff:  sess.EffectiveDifficulty(diffGraceWindow),
		UserAgent:   sess.UserAgent(),
		RemoteIP:    sess.RemoteIP,
	}
	if len(params) >= 6 {
		submit.VersionBits = params[5]
	}

	result, err := h.shareSink.SubmitShare(ctx, sess.PoolID, submit)
	if err != nil {
		if h.bans != nil {
			h.bans.RecordShare(sess.RemoteIP, false)
		}
		if h.Counters != nil && h.Counters.ShareRejected != nil {
			h.Counters.ShareRejected(sess.PoolID)
		}
		code := protocol.ErrOther
		switch {
		case errors.Is(err, jobs.ErrJobNotFound):
			code = protocol.ErrJobNotFound
		case errors.Is(err, jobs.ErrDuplicateShare):
			code = protocol.ErrDuplicateShare
		case errors.Is(err, jobs.ErrLowDifficulty):
			code = protocol.ErrLowDifficulty
		}
		h.log.Debug("share rejected", "pool", sess.PoolID, "worker", worker, "err", err)
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(code, err.Error())))
	}
	if h.bans != nil {
		h.bans.RecordShare(sess.RemoteIP, true)
	}
	if ctrl, ok := sess.Vardiff.(*vardiff.Controller); ok && ctrl != nil {
		ctrl.OnShare(time.Now())
	}
	if h.Counters != nil {
		if h.Counters.ShareAccepted != nil {
			h.Counters.ShareAccepted(sess.PoolID)
		}
		if result.BlockCandidate && h.Counters.BlockFound != nil {
			h.Counters.BlockFound(sess.PoolID)
		}
	}
	return sess.WriteResponse(protocol.OKResponse(req.ID, true))
}

func (h *Handler) sendSetDifficulty(sess *session.Session, diff float64) error {
	sess.SetDifficulty(diff)
	return sess.WriteNotification(&protocol.Notification{
		ID:     nil,
		Method: protocol.MethodSetDifficulty,
		Params: []any{diff},
	})
}

func (h *Handler) rejectMessage(poolID string, state pool.State) string {
	switch state {
	case pool.StateMaintenance:
		return h.lifecycle.MaintenanceMessage(poolID)
	case pool.StatePaused:
		return fmt.Sprintf("Pool %s is temporarily paused. Please reconnect later.", poolID)
	case pool.StateDisabled:
		return fmt.Sprintf("Pool %s is disabled.", poolID)
	case pool.StateDraining:
		return fmt.Sprintf("Pool %s is draining and not accepting new workers.", poolID)
	case pool.StateError:
		return fmt.Sprintf("Pool %s is temporarily unavailable.", poolID)
	default:
		return fmt.Sprintf("Pool %s is not accepting connections.", poolID)
	}
}
