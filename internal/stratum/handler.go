package stratum

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/icminers/gostratumpool/internal/logging"
	"github.com/icminers/gostratumpool/internal/pool"
	"github.com/icminers/gostratumpool/internal/stratum/protocol"
	"github.com/icminers/gostratumpool/internal/stratum/session"
)

// Handler dispatches inbound JSON-RPC requests. It is stateless per request and
// safe for concurrent use; all mutable state lives on the Session or the
// lifecycle manager.
type Handler struct {
	log       *logging.Logger
	lifecycle *pool.PoolLifecycleManager
	alloc     *session.ExtraNonce1Allocator
}

// NewHandler builds a handler.
func NewHandler(log *logging.Logger, lifecycle *pool.PoolLifecycleManager, alloc *session.ExtraNonce1Allocator) *Handler {
	return &Handler{log: logging.Component(log, "handler"), lifecycle: lifecycle, alloc: alloc}
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
		return h.handleSubmit(sess, req)
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
	// Push the current (static, Stage 1) difficulty so miners can start hashing.
	return h.sendSetDifficulty(sess, sess.Difficulty())
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

// handleSubmit is a placeholder until Stage 3. It validates the session is
// authorized and returns a clear "not yet accepting shares" error rather than
// silently dropping the submit, so miners get an honest signal.
func (h *Handler) handleSubmit(sess *session.Session, req *protocol.Request) error {
	if !sess.HasAnyAuthorized() {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrUnauthorized, "unauthorized")))
	}
	// Reject shares for pools not in a share-accepting state.
	state, err := h.lifecycle.GetPoolState(sess.PoolID)
	if err != nil {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, "unknown pool")))
	}
	if !state.AcceptsShares() {
		return sess.WriteResponse(protocol.ErrResponse(req.ID,
			protocol.NewError(protocol.ErrOther, h.rejectMessage(sess.PoolID, state))))
	}
	// Share validation lands in Stage 3 (needs the job manager from Stage 2).
	return sess.WriteResponse(protocol.ErrResponse(req.ID,
		protocol.NewError(protocol.ErrJobNotFound, "share validation not enabled yet (stage 3)")))
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
