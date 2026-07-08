package rewards

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// fakeSource is an in-memory ShareSource.
type fakeSource struct {
	// shares newest-last (chronological)
	shares []struct {
		miner   string
		diff    float64
		created time.Time
	}
	prevBlockTime time.Time
	hasPrev       bool
}

func (f *fakeSource) add(miner string, diff float64, created time.Time) {
	f.shares = append(f.shares, struct {
		miner   string
		diff    float64
		created time.Time
	}{miner, diff, created})
}

func (f *fakeSource) WorkBetween(_ context.Context, _ string, from, to time.Time) ([]MinerWork, error) {
	sums := map[string]float64{}
	for _, s := range f.shares {
		if s.created.After(from) && !s.created.After(to) {
			sums[s.miner] += s.diff
		}
	}
	return mapToWork(sums), nil
}

func (f *fakeSource) WorkBackward(_ context.Context, _ string, before time.Time, target float64) ([]MinerWork, error) {
	sums := map[string]float64{}
	var collected float64
	for i := len(f.shares) - 1; i >= 0 && collected < target; i-- {
		s := f.shares[i]
		if s.created.After(before) {
			continue
		}
		sums[s.miner] += s.diff
		collected += s.diff
	}
	return mapToWork(sums), nil
}

func (f *fakeSource) PreviousBlockTime(_ context.Context, _ string, _ time.Time) (time.Time, bool, error) {
	return f.prevBlockTime, f.hasPrev, nil
}

func mapToWork(sums map[string]float64) []MinerWork {
	out := []MinerWork{}
	for m, d := range sums {
		out = append(out, MinerWork{Miner: m, DiffSum: d})
	}
	return out
}

func creditMap(credits []Credit) map[string]int64 {
	m := map[string]int64{}
	for _, c := range credits {
		m[c.Miner] = c.AmountSats
	}
	return m
}

func sumCredits(credits []Credit) int64 {
	var t int64
	for _, c := range credits {
		t += c.AmountSats
	}
	return t
}

func TestApplyFee(t *testing.T) {
	d, f := ApplyFee(312500000, 1.0)
	if d != 309375000 || f != 3125000 {
		t.Fatalf("1%% fee: d=%d f=%d", d, f)
	}
	if d+f != 312500000 {
		t.Fatal("fee split does not conserve sats")
	}
	// Fee flooring favors miners: 0.5% of 101 sats = 0 fee.
	d, f = ApplyFee(101, 0.5)
	if f != 0 || d != 101 {
		t.Fatalf("tiny fee: d=%d f=%d", d, f)
	}
	d, f = ApplyFee(100, 0)
	if d != 100 || f != 0 {
		t.Fatal("zero fee changed amounts")
	}
	d, f = ApplyFee(100, 100)
	if d != 0 || f != 100 {
		t.Fatal("100%% fee wrong")
	}
}

// TestDistributeExactness: for many amounts and splits the credits must sum
// EXACTLY to the distributable amount.
func TestDistributeExactness(t *testing.T) {
	works := [][]MinerWork{
		{{"a", 1}, {"b", 1}, {"c", 1}},                       // thirds: remainder handling
		{{"a", 7}, {"b", 3}},                                 // 70/30
		{{"a", 1e12}, {"b", 1}},                              // extreme skew
		{{"a", 0.001}, {"b", 0.002}, {"c", 0.003}, {"d", 0}}, // tiny + zero
		{{"a", 5}}, // single
	}
	amounts := []int64{1, 2, 3, 100, 101, 312500000, 625000001}
	for wi, work := range works {
		for _, amt := range amounts {
			credits := distribute(amt, work)
			if got := sumCredits(credits); got != amt {
				t.Fatalf("work[%d] amount %d: credited %d", wi, amt, got)
			}
			for _, c := range credits {
				if c.AmountSats <= 0 {
					t.Fatalf("work[%d] amount %d: non-positive credit %+v", wi, amt, c)
				}
			}
		}
	}
	// Proportionality sanity: 70/30 of 100.
	cm := creditMap(distribute(100, []MinerWork{{"a", 7}, {"b", 3}}))
	if cm["a"] != 70 || cm["b"] != 30 {
		t.Fatalf("70/30 split = %v", cm)
	}
	// Zero-diff miner receives nothing.
	cm = creditMap(distribute(100, []MinerWork{{"a", 1}, {"b", 0}}))
	if _, ok := cm["b"]; ok {
		t.Fatal("zero-diff miner credited")
	}
}

func TestSolo(t *testing.T) {
	block := Block{Height: 1, Miner: "finder", PoolID: "p"}
	credits, err := Solo{}.Calculate(context.Background(), nil, block, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(credits) != 1 || credits[0].Miner != "finder" || credits[0].AmountSats != 1000 {
		t.Fatalf("solo credits = %v", credits)
	}
	if _, err := (Solo{}).Calculate(context.Background(), nil, Block{}, 1000); err == nil {
		t.Fatal("solo without finder must error")
	}
}

func TestProp(t *testing.T) {
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{}
	// Before the previous block: must NOT count.
	src.add("old", 500, base.Add(-time.Hour))
	// The round: alice 60, bob 40.
	src.add("alice", 60, base.Add(1*time.Minute))
	src.add("bob", 40, base.Add(2*time.Minute))
	// After this block: must NOT count.
	src.add("late", 999, base.Add(20*time.Minute))
	src.prevBlockTime, src.hasPrev = base, true

	block := Block{PoolID: "p", Height: 2, Miner: "alice", Created: base.Add(10 * time.Minute)}
	credits, err := Prop{}.Calculate(context.Background(), src, block, 100)
	if err != nil {
		t.Fatal(err)
	}
	cm := creditMap(credits)
	if cm["alice"] != 60 || cm["bob"] != 40 || len(cm) != 2 {
		t.Fatalf("prop credits = %v", cm)
	}

	// First block ever: everything before it counts.
	src2 := &fakeSource{}
	src2.add("a", 10, base)
	block2 := Block{PoolID: "p", Height: 1, Miner: "a", Created: base.Add(time.Minute)}
	credits, err = Prop{}.Calculate(context.Background(), src2, block2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if creditMap(credits)["a"] != 100 {
		t.Fatalf("first-block prop = %v", credits)
	}
}

func TestPPLNSWindow(t *testing.T) {
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{}
	// Chronological shares; PPLNS walks backward from the block. Network
	// difficulty 100, factor 2 -> window target 200 diff. Newest-first: carol
	// 100, bob 100 fills the window; alice's older 500 must be excluded.
	src.add("alice", 500, base.Add(1*time.Minute))
	src.add("bob", 100, base.Add(2*time.Minute))
	src.add("carol", 100, base.Add(3*time.Minute))
	block := Block{PoolID: "p", Height: 5, Miner: "carol",
		NetworkDifficulty: 100, Created: base.Add(4 * time.Minute)}

	credits, err := PPLNS{Factor: 2}.Calculate(context.Background(), src, block, 1000)
	if err != nil {
		t.Fatal(err)
	}
	cm := creditMap(credits)
	if cm["bob"] != 500 || cm["carol"] != 500 || len(cm) != 2 {
		t.Fatalf("pplns credits = %v (alice must be outside the window)", cm)
	}

	// Young pool: window under-filled -> proportional to what exists.
	src2 := &fakeSource{}
	src2.add("a", 30, base)
	src2.add("b", 10, base.Add(time.Minute))
	block2 := Block{PoolID: "p", Height: 1, Miner: "a",
		NetworkDifficulty: 1000, Created: base.Add(2 * time.Minute)}
	credits, err = PPLNS{Factor: 2}.Calculate(context.Background(), src2, block2, 100)
	if err != nil {
		t.Fatal(err)
	}
	cm = creditMap(credits)
	if cm["a"] != 75 || cm["b"] != 25 {
		t.Fatalf("underfilled pplns = %v", cm)
	}
	// Missing network difficulty must error, never divide by zero.
	if _, err := (PPLNS{Factor: 2}).Calculate(context.Background(), src2,
		Block{PoolID: "p", Created: base}, 100); err == nil {
		t.Fatal("pplns without network difficulty must error")
	}
}

func TestForPaymentMode(t *testing.T) {
	for mode, want := range map[string]string{"solo": "solo", "prop": "prop", "pplns": "pplns"} {
		c, err := ForPaymentMode(mode, 2)
		if err != nil || c.Name() != want {
			t.Fatalf("mode %s: %v %v", mode, c, err)
		}
	}
	if _, err := ForPaymentMode("pps", 2); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

// fakeChain implements ChainView.
type fakeChain struct {
	tip    int64
	hashes map[int64]string
}

func (f *fakeChain) TipHeight(context.Context) (int64, error) { return f.tip, nil }
func (f *fakeChain) BlockHashAt(_ context.Context, h int64) (string, bool, error) {
	hash, ok := f.hashes[h]
	return hash, ok, nil
}

func TestCheckBlock(t *testing.T) {
	ctx := context.Background()
	opts := ConfirmOptions{MaturityDepth: 100, OrphanDepth: 12}
	chain := &fakeChain{tip: 150, hashes: map[int64]string{100: "ours", 90: "theirs"}}

	// On chain, 51 confirmations: pending with progress.
	st, err := CheckBlock(ctx, chain, 100, "OURS", opts) // case-insensitive
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != "pending" || st.Confirmations != 51 || st.Progress != 0.51 {
		t.Fatalf("st = %+v", st)
	}

	// Matured.
	chain.tip = 199
	st, _ = CheckBlock(ctx, chain, 100, "ours", opts)
	if st.Status != "confirmed" || st.Confirmations != 100 || st.Progress != 1 {
		t.Fatalf("matured st = %+v", st)
	}

	// Different hash, deeply buried: orphaned.
	st, _ = CheckBlock(ctx, chain, 90, "ours", opts)
	if st.Status != "orphaned" {
		t.Fatalf("orphan st = %+v", st)
	}
	// Different hash, shallow: still pending (could reorg back).
	chain.tip = 95
	st, _ = CheckBlock(ctx, chain, 90, "ours", opts)
	if st.Status != "pending" {
		t.Fatalf("shallow mismatch st = %+v", st)
	}
	// Tip behind our height: pending.
	st, _ = CheckBlock(ctx, chain, 200, "x", opts)
	if st.Status != "pending" {
		t.Fatalf("future st = %+v", st)
	}
}

// TestProcessorEndToEndFakes drives a full confirm+reward pass over an
// in-memory BlockStore.
type memBlockStore struct {
	fakeSource
	blocks  []Block
	status  map[int64]string
	credits map[int64][]Credit
	fees    map[int64]int64
}

func newMemBlockStore() *memBlockStore {
	return &memBlockStore{
		status:  map[int64]string{},
		credits: map[int64][]Credit{},
		fees:    map[int64]int64{},
	}
}

func (m *memBlockStore) UnconfirmedBlocks(_ context.Context, _ string) ([]Block, error) {
	out := []Block{}
	for _, b := range m.blocks {
		if m.status[b.ID] == "pending" {
			out = append(out, b)
		}
	}
	return out, nil
}

func (m *memBlockStore) ConfirmedUnrewarded(_ context.Context, _ string) ([]Block, error) {
	out := []Block{}
	for _, b := range m.blocks {
		if m.status[b.ID] == "confirmed" && m.credits[b.ID] == nil {
			out = append(out, b)
		}
	}
	return out, nil
}

func (m *memBlockStore) UpdateBlockConfirmation(_ context.Context, id int64, status string, _ int64, _ float64) error {
	m.status[id] = status
	return nil
}

func (m *memBlockStore) CreditBlockRewards(_ context.Context, b Block, credits []Credit, fee int64) error {
	m.credits[b.ID] = credits
	m.fees[b.ID] = fee
	return nil
}

func TestProcessorPass(t *testing.T) {
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	store := newMemBlockStore()
	store.add("alice", 60, base.Add(1*time.Minute))
	store.add("bob", 40, base.Add(2*time.Minute))

	confirmed := Block{ID: 1, PoolID: "p", Height: 100, Miner: "alice", Hash: "goodhash",
		RewardSats: 312500000, NetworkDifficulty: 50, Created: base.Add(3 * time.Minute)}
	orphan := Block{ID: 2, PoolID: "p", Height: 90, Miner: "bob", Hash: "gonehash",
		RewardSats: 312500000, NetworkDifficulty: 50, Created: base.Add(4 * time.Minute)}
	store.blocks = []Block{confirmed, orphan}
	store.status[1] = "pending"
	store.status[2] = "pending"

	chain := &fakeChain{tip: 250, hashes: map[int64]string{100: "goodhash", 90: "otherhash"}}
	proc := NewProcessor(ProcessorOptions{
		PoolID:     "p",
		Calculator: PPLNS{Factor: 2},
		FeePercent: 1.0,
		Chain:      chain,
		Store:      store,
		Confirm:    ConfirmOptions{MaturityDepth: 100, OrphanDepth: 12},
		Log:        testLogger(),
	})
	if err := proc.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}

	if store.status[1] != "confirmed" {
		t.Fatalf("block 1 status = %s", store.status[1])
	}
	if store.status[2] != "orphaned" {
		t.Fatalf("block 2 status = %s", store.status[2])
	}
	credits := store.credits[1]
	if credits == nil {
		t.Fatal("confirmed block not rewarded")
	}
	distributable, fee := ApplyFee(confirmed.RewardSats, 1.0)
	if sumCredits(credits) != distributable || store.fees[1] != fee {
		t.Fatalf("credited %d (fee %d), want %d (%d)", sumCredits(credits), store.fees[1], distributable, fee)
	}
	cm := creditMap(credits)
	if cm["alice"] <= cm["bob"] {
		t.Fatalf("proportions wrong: %v", cm)
	}
	if store.credits[2] != nil {
		t.Fatal("orphaned block was rewarded")
	}
	// A second pass must be a no-op (already rewarded / orphaned).
	if err := proc.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(store.credits[1]) != fmt.Sprint(credits) {
		t.Fatal("second pass changed credits")
	}
}
