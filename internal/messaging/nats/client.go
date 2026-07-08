// Package nats implements the master/regional messaging layer on NATS
// JetStream: regional stratum nodes publish accepted shares, found blocks, and
// pool-state changes; the master consumes and persists them and publishes
// per-pool lifecycle commands that regionals apply — always to exactly one
// pool. A disk spool keeps regional nodes lossless through NATS outages.
package nats

import (
	"context"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
)

// Subject layout. Region and poolID are individual tokens, so wildcard
// subscriptions per pool or per region are natural.
//
//	shares.<region>.<poolId>
//	blocks.<region>.<poolId>
//	poolstate.<region>.<poolId>
//	commands.pool.<poolId>
const (
	subjectSharePrefix = "shares."
	subjectBlockPrefix = "blocks."
	subjectStatePrefix = "poolstate."
	subjectCmdPrefix   = "commands.pool."
)

// ShareSubject builds the share subject for a region/pool.
func ShareSubject(region, poolID string) string {
	return subjectSharePrefix + region + "." + poolID
}

// BlockSubject builds the block subject for a region/pool.
func BlockSubject(region, poolID string) string {
	return subjectBlockPrefix + region + "." + poolID
}

// StateSubject builds the pool-state subject for a region/pool.
func StateSubject(region, poolID string) string {
	return subjectStatePrefix + region + "." + poolID
}

// CommandSubject builds the lifecycle-command subject for a pool.
func CommandSubject(poolID string) string {
	return subjectCmdPrefix + poolID
}

// Stream names.
const (
	// StreamEvents persists shares and blocks until the master consumes them.
	StreamEvents = "POOLEVENTS"
	// StreamCommands persists lifecycle commands briefly so a regional that
	// reconnects still sees recent orders.
	StreamCommands = "POOLCMD"
)

// ShareEventMsg is the wire form of an accepted share.
type ShareEventMsg struct {
	PoolID            string    `json:"poolId"`
	Region            string    `json:"region"`
	BlockHeight       int64     `json:"blockHeight"`
	ShareDiff         float64   `json:"shareDiff"`
	WorkerDiff        float64   `json:"workerDiff"`
	NetworkDifficulty float64   `json:"networkDifficulty"`
	Miner             string    `json:"miner"`
	Worker            string    `json:"worker"`
	UserAgent         string    `json:"userAgent"`
	IPAddress         string    `json:"ipAddress"`
	Created           time.Time `json:"created"`
	IsBlockCandidate  bool      `json:"isBlockCandidate"`
}

// BlockEventMsg is the wire form of a found block.
type BlockEventMsg struct {
	PoolID            string    `json:"poolId"`
	Region            string    `json:"region"`
	BlockHeight       int64     `json:"blockHeight"`
	NetworkDifficulty float64   `json:"networkDifficulty"`
	Miner             string    `json:"miner"`
	Worker            string    `json:"worker"`
	Hash              string    `json:"hash"`
	RewardSats        int64     `json:"rewardSats"`
	Created           time.Time `json:"created"`
}

// StateChangeMsg is the wire form of a pool lifecycle state change.
type StateChangeMsg struct {
	PoolID  string    `json:"poolId"`
	Region  string    `json:"region"`
	NodeID  string    `json:"nodeId"`
	From    string    `json:"from"`
	To      string    `json:"to"`
	Reason  string    `json:"reason"`
	Created time.Time `json:"created"`
}

// CommandMsg is a lifecycle command for exactly one pool.
type CommandMsg struct {
	PoolID string `json:"poolId"`
	// Action: pause | resume | drain | maintenance | restart | disable.
	Action string `json:"action"`
	// Message applies to maintenance.
	Message string `json:"message,omitempty"`
	// GracePeriodSeconds applies to drain.
	GracePeriodSeconds int `json:"gracePeriodSeconds,omitempty"`
	// Origin identifies the issuing node (for logs).
	Origin string `json:"origin,omitempty"`
}

// Options configure the client.
type Options struct {
	URLs   []string
	Name   string // connection name (node id)
	Region string
	Log    *logging.Logger

	// AsyncFailure is invoked for every async publish that fails to be acked
	// (spool hook). May be nil.
	AsyncFailure func(subject string, data []byte)
	// RequireStreams makes Connect fail when streams cannot be created
	// immediately (master mode). When false (regional), stream creation
	// retries in the background and the node starts regardless.
	RequireStreams bool
}

// Client wraps the NATS connection and JetStream context.
type Client struct {
	Conn   *natsgo.Conn
	JS     jetstream.JetStream
	region string
	log    *logging.Logger
}

// Connect dials NATS with unlimited reconnect and initializes JetStream and
// the streams. The initial dial retries as well, so a regional node can start
// before its NATS server does.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	log := logging.Component(opts.Log, "nats")
	url := natsgo.DefaultURL
	if len(opts.URLs) > 0 {
		url = ""
		for i, u := range opts.URLs {
			if i > 0 {
				url += ","
			}
			url += u
		}
	}
	conn, err := natsgo.Connect(url,
		natsgo.Name(opts.Name),
		natsgo.MaxReconnects(-1),
		natsgo.ReconnectWait(2*time.Second),
		natsgo.RetryOnFailedConnect(true),
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
			log.Warn("nats disconnected", "err", err)
		}),
		natsgo.ReconnectHandler(func(c *natsgo.Conn) {
			log.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}
	var jsOpts []jetstream.JetStreamOpt
	if opts.AsyncFailure != nil {
		fail := opts.AsyncFailure
		jsOpts = append(jsOpts, jetstream.WithPublishAsyncErrHandler(
			func(_ jetstream.JetStream, m *natsgo.Msg, err error) {
				log.Warn("async publish failed; handing to spool", "subject", m.Subject, "err", err)
				fail(m.Subject, m.Data)
			}))
	}
	js, err := jetstream.New(conn, jsOpts...)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("nats: jetstream: %w", err)
	}
	c := &Client{Conn: conn, JS: js, region: opts.Region, log: log}
	if err := c.ensureStreams(ctx); err != nil {
		if opts.RequireStreams {
			conn.Close()
			return nil, err
		}
		log.Warn("streams unavailable at startup; retrying in background", "err", err)
		go c.retryStreams(ctx)
	}
	return c, nil
}

// retryStreams keeps trying stream creation until it succeeds or ctx ends.
func (c *Client) retryStreams(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.ensureStreams(ctx); err == nil {
				c.log.Info("streams ensured")
				return
			}
		}
	}
}

// ensureStreams creates/updates the JetStream streams.
func (c *Client) ensureStreams(ctx context.Context) error {
	sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := c.JS.CreateOrUpdateStream(sctx, jetstream.StreamConfig{
		Name:     StreamEvents,
		Subjects: []string{"shares.>", "blocks.>", "poolstate.>"},
		Storage:  jetstream.FileStorage,
		// Retain until acknowledged by the master's durable consumer, bounded
		// by age so an abandoned deployment cannot grow forever.
		Retention: jetstream.LimitsPolicy,
		MaxAge:    72 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("nats: ensure %s: %w", StreamEvents, err)
	}
	_, err = c.JS.CreateOrUpdateStream(sctx, jetstream.StreamConfig{
		Name:      StreamCommands,
		Subjects:  []string{"commands.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    1 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("nats: ensure %s: %w", StreamCommands, err)
	}
	return nil
}

// Close drains and closes the connection.
func (c *Client) Close() {
	if c.Conn != nil && !c.Conn.IsClosed() {
		_ = c.Conn.Drain()
	}
}
