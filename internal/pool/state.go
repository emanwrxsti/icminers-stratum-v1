// Package pool implements the multi-pool supervisor. Every coin/pool runs as an
// isolated service with its own lifecycle, context, and goroutines. A failure
// or maintenance action on one pool must never affect any other pool or the
// global stratum server, API, database writer, or NATS connection.
package pool

// State is a pool's lifecycle state. See the isolation spec for the full
// behavior of each state.
type State string

const (
	// StateActive accepts miners, sends jobs, accepts shares, submits blocks.
	StateActive State = "active"
	// StateDraining sends no new jobs but keeps accepting shares for existing
	// jobs during a grace period, then moves to maintenance.
	StateDraining State = "draining"
	// StateMaintenance keeps the server running but stops this pool's template
	// polling and job notifications and rejects new authorizations for it.
	StateMaintenance State = "maintenance"
	// StatePaused temporarily stops the pool and rejects its miner sessions.
	StatePaused State = "paused"
	// StateDisabled means the pool is not accepting traffic at all.
	StateDisabled State = "disabled"
	// StateError means the pool hit a daemon/config/adapter failure and is
	// unhealthy; it retries on a backoff. Only this pool is affected.
	StateError State = "error"
)

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	switch s {
	case StateActive, StateDraining, StateMaintenance, StatePaused, StateDisabled, StateError:
		return true
	default:
		return false
	}
}

// AcceptsNewAuthorization reports whether miners may authorize new workers on a
// pool in this state. Only active pools accept new workers; draining pools keep
// serving existing work but take no newcomers.
func (s State) AcceptsNewAuthorization() bool {
	return s == StateActive
}

// SendsNewJobs reports whether the pool should push new mining.notify jobs.
func (s State) SendsNewJobs() bool {
	return s == StateActive
}

// AcceptsShares reports whether valid shares for already-issued jobs should be
// accepted. Active and draining pools do; everything else does not.
func (s State) AcceptsShares() bool {
	return s == StateActive || s == StateDraining
}

// RunsTemplatePolling reports whether the template poller should run.
func (s State) RunsTemplatePolling() bool {
	return s == StateActive || s == StateDraining
}

// canTransition encodes the legal state machine. Unlisted transitions are
// rejected by the manager to avoid nonsensical jumps.
func canTransition(from, to State) bool {
	if from == to {
		return true
	}
	switch from {
	case StateActive:
		return to == StateDraining || to == StateMaintenance || to == StatePaused ||
			to == StateDisabled || to == StateError
	case StateDraining:
		return to == StateMaintenance || to == StatePaused || to == StateActive ||
			to == StateDisabled || to == StateError
	case StateMaintenance:
		return to == StateActive || to == StatePaused || to == StateDisabled ||
			to == StateError || to == StateDraining
	case StatePaused:
		return to == StateActive || to == StateMaintenance || to == StateDisabled ||
			to == StateError
	case StateDisabled:
		return to == StateActive
	case StateError:
		// Recovery is allowed back to active (e.g. daemon came back) or the
		// operator can pause/disable it.
		return to == StateActive || to == StatePaused || to == StateDisabled ||
			to == StateMaintenance
	default:
		return false
	}
}
