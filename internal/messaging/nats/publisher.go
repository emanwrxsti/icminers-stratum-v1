package nats

import (
	"context"
	"encoding/json"
	"time"

	"github.com/emanwrxsti/icminers-stratum-v1/internal/jobs"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/logging"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/pool"
	"github.com/emanwrxsti/icminers-stratum-v1/internal/spool"
)

// Publisher implements jobs.Recorder and pool.StateHook: it publishes share,
// block, and pool-state events to JetStream. Publish failures fall back to the
// disk spool; a background loop drains the spool whenever connectivity is
// back. Regional hot-path guarantee: RecordShare never blocks on the network
// (async JetStream publish; spool append on immediate failure).
type Publisher struct {
	client *Client
	sp     *spool.Spool
	nodeID string
	log    *logging.Logger
	cancel context.CancelFunc
	done   chan struct{}
}

// NewPublisher builds the publisher and starts the spool-drain loop.
func NewPublisher(client *Client, sp *spool.Spool, nodeID string, log *logging.Logger) *Publisher {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{
		client: client,
		sp:     sp,
		nodeID: nodeID,
		log:    logging.Component(log, "nats-publisher"),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go p.drainLoop(ctx)
	return p
}

// Close stops the drain loop.
func (p *Publisher) Close() {
	p.cancel()
	<-p.done
}

// publish tries an async JetStream publish; on immediate failure the payload
// goes to the spool.
func (p *Publisher) publish(subject string, payload []byte) {
	_, err := p.client.JS.PublishAsync(subject, payload)
	if err == nil {
		return
	}
	if p.sp == nil {
		p.log.Error("publish failed and no spool configured; event lost",
			"subject", subject, "err", err)
		return
	}
	if serr := p.sp.Append(subject, payload); serr != nil {
		p.log.Error("publish failed AND spool append failed; event lost",
			"subject", subject, "publishErr", err, "spoolErr", serr)
		return
	}
	p.log.Warn("publish failed; event spooled", "subject", subject, "err", err)
}

// RecordShare implements jobs.Recorder.
func (p *Publisher) RecordShare(ev jobs.ShareEvent) {
	msg := ShareEventMsg{
		PoolID:            ev.PoolID,
		Region:            p.client.region,
		BlockHeight:       ev.BlockHeight,
		ShareDiff:         ev.ShareDiff,
		WorkerDiff:        ev.WorkerDiff,
		NetworkDifficulty: ev.NetworkDifficulty,
		Miner:             ev.Miner,
		Worker:            ev.Worker,
		UserAgent:         ev.UserAgent,
		IPAddress:         ev.IPAddress,
		Created:           ev.Created,
		IsBlockCandidate:  ev.IsBlockCandidate,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		p.log.Error("marshal share event", "err", err)
		return
	}
	p.publish(ShareSubject(p.client.region, ev.PoolID), payload)
}

// RecordBlock implements jobs.Recorder.
func (p *Publisher) RecordBlock(ev jobs.BlockEvent) {
	msg := BlockEventMsg{
		PoolID:            ev.PoolID,
		Region:            p.client.region,
		BlockHeight:       ev.BlockHeight,
		NetworkDifficulty: ev.NetworkDifficulty,
		Miner:             ev.Miner,
		Worker:            ev.Worker,
		Hash:              ev.Hash,
		RewardSats:        ev.RewardSats,
		Created:           ev.Created,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		p.log.Error("marshal block event", "err", err)
		return
	}
	p.publish(BlockSubject(p.client.region, ev.PoolID), payload)
}

// OnPoolStateChange implements pool.StateHook: every lifecycle transition is
// published so the master (and dashboards) see per-pool state cluster-wide.
func (p *Publisher) OnPoolStateChange(poolID string, from, to pool.State, reason string) {
	msg := StateChangeMsg{
		PoolID:  poolID,
		Region:  p.client.region,
		NodeID:  p.nodeID,
		From:    string(from),
		To:      string(to),
		Reason:  reason,
		Created: time.Now().UTC(),
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}
	p.publish(StateSubject(p.client.region, poolID), payload)
}

// FlushSpool drains the spool synchronously (used at reconnect and in tests).
// Each record is republished with a synchronous publish so failures stop the
// drain and keep the remainder spooled.
func (p *Publisher) FlushSpool(ctx context.Context) error {
	if p.sp == nil || p.sp.Len() == 0 {
		return nil
	}
	return p.sp.Drain(func(subject string, payload []byte) error {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := p.client.JS.Publish(pctx, subject, payload)
		return err
	})
}

// drainLoop periodically flushes the spool while connected.
func (p *Publisher) drainLoop(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.sp == nil || p.sp.Len() == 0 || !p.client.Conn.IsConnected() {
				continue
			}
			if err := p.FlushSpool(ctx); err != nil {
				p.log.Warn("spool drain incomplete", "err", err)
			} else {
				p.log.Info("spool drained")
			}
		}
	}
}
