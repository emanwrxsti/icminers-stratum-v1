package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
)

// EventSink receives consumed events on the master. Implemented in cmd/stratumd
// over the postgres writer/store; tests use an in-memory sink. Implementations
// must be fast or internally queued (the share writer already is).
type EventSink interface {
	SinkShare(ev ShareEventMsg)
	SinkBlock(ev BlockEventMsg)
	SinkStateChange(ev StateChangeMsg)
}

// Consumer is the master-side durable JetStream consumer over POOLEVENTS.
type Consumer struct {
	client  *Client
	sink    EventSink
	log     *logging.Logger
	consCtx jetstream.ConsumeContext
}

// StartConsumer creates/binds the durable consumer and begins delivery.
// Delivery is at-least-once: shares may rarely be double-persisted on
// redelivery (acceptable for accounting; block inserts are idempotent).
func StartConsumer(ctx context.Context, client *Client, sink EventSink, log *logging.Logger) (*Consumer, error) {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cons, err := client.JS.CreateOrUpdateConsumer(cctx, StreamEvents, jetstream.ConsumerConfig{
		Durable:       "master-persist",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxAckPending: 10000,
	})
	if err != nil {
		return nil, fmt.Errorf("nats: create consumer: %w", err)
	}
	c := &Consumer{client: client, sink: sink, log: logging.Component(log, "nats-consumer")}
	consCtx, err := cons.Consume(c.handle)
	if err != nil {
		return nil, fmt.Errorf("nats: consume: %w", err)
	}
	c.consCtx = consCtx
	c.log.Info("master consumer started", "stream", StreamEvents)
	return c, nil
}

// handle routes one message by subject prefix.
func (c *Consumer) handle(msg jetstream.Msg) {
	subject := msg.Subject()
	data := msg.Data()
	switch {
	case strings.HasPrefix(subject, subjectSharePrefix):
		var ev ShareEventMsg
		if err := json.Unmarshal(data, &ev); err != nil {
			c.log.Error("bad share event; dropping", "subject", subject, "err", err)
			_ = msg.Ack() // poison message: ack away, do not redeliver forever
			return
		}
		c.sink.SinkShare(ev)
	case strings.HasPrefix(subject, subjectBlockPrefix):
		var ev BlockEventMsg
		if err := json.Unmarshal(data, &ev); err != nil {
			c.log.Error("bad block event; dropping", "subject", subject, "err", err)
			_ = msg.Ack()
			return
		}
		c.sink.SinkBlock(ev)
	case strings.HasPrefix(subject, subjectStatePrefix):
		var ev StateChangeMsg
		if err := json.Unmarshal(data, &ev); err != nil {
			c.log.Error("bad state event; dropping", "subject", subject, "err", err)
			_ = msg.Ack()
			return
		}
		c.sink.SinkStateChange(ev)
	default:
		c.log.Warn("unexpected subject", "subject", subject)
	}
	_ = msg.Ack()
}

// Stop halts delivery.
func (c *Consumer) Stop() {
	if c.consCtx != nil {
		c.consCtx.Stop()
	}
}

// --- lifecycle commands ---

// PublishCommand sends a lifecycle command for exactly one pool.
func (c *Client) PublishCommand(ctx context.Context, cmd CommandMsg) error {
	if cmd.PoolID == "" || cmd.Action == "" {
		return fmt.Errorf("nats: command requires poolId and action")
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("nats: marshal command: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := c.JS.Publish(pctx, CommandSubject(cmd.PoolID), payload); err != nil {
		return fmt.Errorf("nats: publish command: %w", err)
	}
	return nil
}

// CommandApplier applies a command to the LOCAL lifecycle manager. Split out
// as a function type for testability.
type CommandApplier func(cmd CommandMsg) error

// LifecycleApplier adapts a PoolLifecycleManager into a CommandApplier.
// Unknown pools are ignored (this node may simply not host that pool) and
// every action touches exactly one pool.
func LifecycleApplier(lm *pool.PoolLifecycleManager, log *logging.Logger) CommandApplier {
	l := logging.Component(log, "nats-commands")
	return func(cmd CommandMsg) error {
		var err error
		switch cmd.Action {
		case "pause":
			err = lm.PausePool(cmd.PoolID)
		case "resume":
			err = lm.ResumePool(cmd.PoolID)
		case "drain":
			grace := time.Duration(cmd.GracePeriodSeconds) * time.Second
			if grace <= 0 {
				grace = 60 * time.Second
			}
			err = lm.DrainPool(cmd.PoolID, grace)
		case "maintenance":
			msg := cmd.Message
			if msg == "" {
				msg = "pool is under maintenance"
			}
			err = lm.PutPoolInMaintenance(cmd.PoolID, msg)
		case "restart":
			err = lm.RestartPool(cmd.PoolID)
		case "disable":
			err = lm.DisablePool(cmd.PoolID)
		default:
			return fmt.Errorf("unknown command action %q", cmd.Action)
		}
		if err != nil {
			var unknown pool.ErrUnknownPool
			if errors.As(err, &unknown) {
				l.Debug("command for pool not hosted here", "pool", cmd.PoolID)
				return nil
			}
			return err
		}
		l.Info("applied remote command", "pool", cmd.PoolID, "action", cmd.Action, "origin", cmd.Origin)
		return nil
	}
}

// CommandSubscriber delivers lifecycle commands to the local applier.
type CommandSubscriber struct {
	consCtx jetstream.ConsumeContext
}

// StartCommandSubscriber attaches an ephemeral consumer over POOLCMD that
// receives only NEW commands (delivery from now), so a restarting node does
// not replay stale orders on top of its configured state.
func StartCommandSubscriber(ctx context.Context, client *Client, apply CommandApplier, log *logging.Logger) (*CommandSubscriber, error) {
	l := logging.Component(log, "nats-commands")
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cons, err := client.JS.CreateOrUpdateConsumer(cctx, StreamCommands, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("nats: create command consumer: %w", err)
	}
	consCtx, err := cons.Consume(func(msg jetstream.Msg) {
		var cmd CommandMsg
		if err := json.Unmarshal(msg.Data(), &cmd); err != nil {
			l.Error("bad command; dropping", "err", err)
			_ = msg.Ack()
			return
		}
		if err := apply(cmd); err != nil {
			l.Warn("command apply failed", "pool", cmd.PoolID, "action", cmd.Action, "err", err)
		}
		_ = msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("nats: consume commands: %w", err)
	}
	return &CommandSubscriber{consCtx: consCtx}, nil
}

// Stop halts delivery.
func (s *CommandSubscriber) Stop() {
	if s.consCtx != nil {
		s.consCtx.Stop()
	}
}
