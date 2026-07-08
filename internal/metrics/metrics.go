// Package metrics is a tiny, dependency-free metrics registry rendering the
// Prometheus text exposition format. Counters are monotonically increasing
// atomics; gauges are sampled from callbacks at render time so live values
// (sessions, spool bytes, ban counts) never need push plumbing.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing metric.
type Counter struct {
	v atomic.Int64
}

// Inc adds 1.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n.
func (c *Counter) Add(n int64) { c.v.Add(n) }

// Value returns the current count.
func (c *Counter) Value() int64 { return c.v.Load() }

// GaugeFunc samples a value at render time.
type GaugeFunc func() float64

// Registry holds all metrics. Safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	counters map[string]*Counter // full key: name{labels}
	gauges   map[string]GaugeFunc
	help     map[string]string // metric name -> HELP text
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		counters: make(map[string]*Counter),
		gauges:   make(map[string]GaugeFunc),
		help:     make(map[string]string),
	}
}

// key renders name plus sorted label pairs: name{k="v",...}.
func key(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

// Counter returns (creating if needed) the counter for name+labels.
func (r *Registry) Counter(name, helpText string, labels map[string]string) *Counter {
	k := key(name, labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[k]; ok {
		return c
	}
	c := &Counter{}
	r.counters[k] = c
	if helpText != "" {
		r.help[name] = helpText
	}
	return c
}

// Gauge registers a sampled gauge for name+labels.
func (r *Registry) Gauge(name, helpText string, labels map[string]string, fn GaugeFunc) {
	k := key(name, labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[k] = fn
	if helpText != "" {
		r.help[name] = helpText
	}
}

// metricName strips the label part of a full key.
func metricName(k string) string {
	if i := strings.IndexByte(k, '{'); i >= 0 {
		return k[:i]
	}
	return k
}

// Render writes the Prometheus text format: HELP/TYPE per metric name, then
// every series sorted for deterministic output.
func (r *Registry) Render(w io.Writer) {
	r.mu.Lock()
	counterKeys := make([]string, 0, len(r.counters))
	for k := range r.counters {
		counterKeys = append(counterKeys, k)
	}
	gaugeKeys := make([]string, 0, len(r.gauges))
	for k := range r.gauges {
		gaugeKeys = append(gaugeKeys, k)
	}
	counterVals := make(map[string]int64, len(counterKeys))
	for k, c := range r.counters {
		counterVals[k] = c.Value()
	}
	gaugeFns := make(map[string]GaugeFunc, len(gaugeKeys))
	for k, fn := range r.gauges {
		gaugeFns[k] = fn
	}
	helpCopy := make(map[string]string, len(r.help))
	for k, v := range r.help {
		helpCopy[k] = v
	}
	r.mu.Unlock()

	sort.Strings(counterKeys)
	sort.Strings(gaugeKeys)

	seenHeader := map[string]bool{}
	writeHeader := func(k, typ string) {
		name := metricName(k)
		if seenHeader[name] {
			return
		}
		seenHeader[name] = true
		if h := helpCopy[name]; h != "" {
			fmt.Fprintf(w, "# HELP %s %s\n", name, h)
		}
		fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
	}
	for _, k := range counterKeys {
		writeHeader(k, "counter")
		fmt.Fprintf(w, "%s %d\n", k, counterVals[k])
	}
	for _, k := range gaugeKeys {
		writeHeader(k, "gauge")
		fmt.Fprintf(w, "%s %g\n", k, gaugeFns[k]())
	}
}
