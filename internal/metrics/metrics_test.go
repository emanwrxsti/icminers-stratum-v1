package metrics

import (
	"strings"
	"testing"
)

func TestRegistryRender(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("pool_shares_total", "Accepted shares.", map[string]string{"pool": "p1", "result": "accepted"})
	c.Add(5)
	r.Counter("pool_shares_total", "", map[string]string{"pool": "p1", "result": "rejected"}).Inc()
	r.Gauge("pool_sessions", "Live sessions.", nil, func() float64 { return 7 })

	// Same series twice returns the same counter.
	c2 := r.Counter("pool_shares_total", "", map[string]string{"result": "accepted", "pool": "p1"})
	if c2 != c {
		t.Fatal("label order changed series identity")
	}

	var b strings.Builder
	r.Render(&b)
	out := b.String()
	for _, want := range []string{
		"# HELP pool_shares_total Accepted shares.",
		"# TYPE pool_shares_total counter",
		`pool_shares_total{pool="p1",result="accepted"} 5`,
		`pool_shares_total{pool="p1",result="rejected"} 1`,
		"# TYPE pool_sessions gauge",
		"pool_sessions 7",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("render missing %q:\n%s", want, out)
		}
	}
	// HELP/TYPE emitted once per metric name.
	if strings.Count(out, "# TYPE pool_shares_total") != 1 {
		t.Fatalf("duplicate TYPE headers:\n%s", out)
	}
}
