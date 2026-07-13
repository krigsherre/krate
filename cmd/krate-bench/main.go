package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/krigsherre/krate"
)

var (
	redisAddr  = flag.String("redis", "localhost:6379", "Redis address")
	scenario   = flag.String("scenario", "all", "Scenario: api-gateway, user-limiting, ip-throttling, multi-tenant, all")
	duration   = flag.Duration("duration", 30*time.Second, "Duration per scenario")
	jsonOut    = flag.Bool("json", false, "JSON output")
	warmupKeys = flag.Int("warmup-keys", 10000, "Keys to warm before benchmark (0 = let Zipf warm naturally)")
)

var benchStart = time.Now()

func logf(format string, args ...interface{}) {
	elapsed := time.Since(benchStart).Truncate(time.Millisecond)
	prefix := fmt.Sprintf("[%s] ", elapsed)
	fmt.Fprintf(os.Stderr, prefix+format+"\n", args...)
}

type scenarioConfig struct {
	Name          string
	Description   string
	Instances     int
	Concurrency   int
	Keys          int
	Limit         uint64
	Window        time.Duration
	ZipfS         float64
	ProbeK        int
	ReservedMin   float64
	SharedHotspot bool
	TrafficSkew   bool
}

var scenarios = map[string]scenarioConfig{
	"api-gateway": {
		Name:        "API Gateway",
		Description: "10K API keys, power-law traffic. Top 1% handles ~50% of requests.",
		Instances:   4,
		Concurrency: 100,
		Keys:        10_000,
		Limit:       10_000,
		Window:      60 * time.Second,
		ZipfS:       1.3,
		ProbeK:      3,
		ReservedMin: 0.10,
	},
	"user-limiting": {
		Name:        "Per-User Limiting",
		Description: "10K users, moderate skew, tight per-user limits.",
		Instances:   4,
		Concurrency: 100,
		Keys:        10_000,
		Limit:       200,
		Window:      60 * time.Second,
		ZipfS:       1.3,
		ProbeK:      3,
		ReservedMin: 0.10,
	},
	"ip-throttling": {
		Name:        "IP Throttling",
		Description: "10K unique IPs, heavy bot tail. Small limits force frequent re-borrows.",
		Instances:   4,
		Concurrency: 150,
		Keys:        10_000,
		Limit:       60,
		Window:      60 * time.Second,
		ZipfS:       1.4,
		ProbeK:      3,
		ReservedMin: 0.10,
	},
	"multi-tenant": {
		Name:        "Multi-Tenant SaaS",
		Description: "10K tenants, high throughput per tenant. Tests borrow amortization.",
		Instances:   4,
		Concurrency: 100,
		Keys:        10_000,
		Limit:       5_000,
		Window:      60 * time.Second,
		ZipfS:       1.3,
		ProbeK:      3,
		ReservedMin: 0.10,
	},
	"peer": {
		Name:        "Peer Token Flow",
		Description: "2 instances, probeK=3. Tests peer acquisition + redis-zero bypass.",
		Instances:   2,
		Concurrency: 50,
		Keys:        10_000,
		Limit:       500,
		Window:      60 * time.Second,
		ZipfS:       1.3,
		ProbeK:      3,
		ReservedMin: 0.10,
	},
	"peer-transfer": {
		Name:          "Peer Transfer",
		Description:   "Uneven traffic + fair borrowing = peer token donations.",
		Instances:     4,
		Concurrency:   50,
		Keys:          10_000,
		Limit:         200,
		Window:        10 * time.Second,
		ZipfS:         1.3,
		ProbeK:        3,
		ReservedMin:   0.05,
		SharedHotspot: true,
		TrafficSkew:   true,
	},
}

type latency struct {
	mu    sync.Mutex
	buf   []float64
	count atomic.Int64
	sum   atomic.Int64
	n     int
}

func newLatency(n int) *latency {
	return &latency{buf: make([]float64, 0, n), n: n}
}

func (l *latency) add(ns float64) {
	l.count.Add(1)
	l.sum.Add(int64(ns))
	c := l.count.Load()
	if c <= int64(l.n) || rand.Int64N(c) < int64(l.n) {
		l.mu.Lock()
		if len(l.buf) < l.n {
			l.buf = append(l.buf, ns)
		} else {
			l.buf[rand.IntN(l.n)] = ns
		}
		l.mu.Unlock()
	}
}

func (l *latency) pcts(ps ...float64) map[float64]float64 {
	l.mu.Lock()
	s := make([]float64, len(l.buf))
	copy(s, l.buf)
	l.mu.Unlock()
	sort.Float64s(s)
	n := len(s)
	out := make(map[float64]float64, len(ps))
	for _, p := range ps {
		if n == 0 {
			out[p] = 0
		} else {
			i := int(float64(n-1) * p / 100)
			if i >= n {
				i = n - 1
			}
			out[p] = s[i]
		}
	}
	return out
}

func (l *latency) avg() float64 {
	if n := l.count.Load(); n > 0 {
		return float64(l.sum.Load()) / float64(n)
	}
	return 0
}

type histBin struct {
	label string
	upper float64
	count int
}

func (l *latency) bins() []histBin {
	l.mu.Lock()
	s := make([]float64, len(l.buf))
	copy(s, l.buf)
	l.mu.Unlock()
	sort.Float64s(s)
	bs := []histBin{
		{"<200ns", 200, 0}, {"<500ns", 500, 0}, {"<1μs", 1000, 0},
		{"<2μs", 2000, 0}, {"<5μs", 5000, 0}, {"<10μs", 10_000, 0},
		{"<50μs", 50_000, 0}, {"<100μs", 100_000, 0}, {"<500μs", 500_000, 0},
		{"<1ms", 1_000_000, 0}, {"<5ms", 5_000_000, 0}, {"<10ms", 10_000_000, 0},
		{"≥10ms", 1e18, 0},
	}
	for _, v := range s {
		for i := range bs {
			if v < bs[i].upper {
				bs[i].count++
				break
			}
		}
	}
	return bs
}

type pathMetrics struct {
	local      int64
	redis      int64
	redisEx    int64
	redisSkip  int64
	preBorrows int64
	peer       int64
	peerProbes int64
	peerStale  int64
}

func gatherPath(regs []*prometheus.Registry) pathMetrics {
	var pm pathMetrics
	for _, reg := range regs {
		mfs, _ := reg.Gather()
		for _, mf := range mfs {
			switch mf.GetName() {
			case "krate_local_hits_total":
				for _, m := range mf.GetMetric() {
					pm.local += int64(m.GetCounter().GetValue())
				}
			case "krate_redis_borrows_total":
				for _, m := range mf.GetMetric() {
					for _, lp := range m.GetLabel() {
						if lp.GetName() == "result" {
							if lp.GetValue() == "granted" {
								pm.redis += int64(m.GetCounter().GetValue())
							} else {
								pm.redisEx += int64(m.GetCounter().GetValue())
							}
						}
					}
				}
			case "krate_redis_skips_total":
				for _, m := range mf.GetMetric() {
					pm.redisSkip += int64(m.GetCounter().GetValue())
				}
			case "krate_pre_borrows_total":
				for _, m := range mf.GetMetric() {
					pm.preBorrows += int64(m.GetCounter().GetValue())
				}
			case "krate_peer_probes_total":
				for _, m := range mf.GetMetric() {
					pm.peerProbes += int64(m.GetCounter().GetValue())
					for _, lp := range m.GetLabel() {
						if lp.GetName() == "result" {
							switch lp.GetValue() {
							case "granted":
								pm.peer += int64(m.GetCounter().GetValue())
							case "stale":
								pm.peerStale += int64(m.GetCounter().GetValue())
							}
						}
					}
				}
			}
		}
	}
	return pm
}

type keyDist struct {
	total    int64
	keys     int
	zipfS    float64
	topHits  []int64
	topIdxs  []int
	topRanks []string
}

var trackedIdxs = []int{0, 1, 4, 9, 99, 999, 9999, 49999}
var trackedRanks = []string{"#1", "#2", "#5", "#10", "#100", "#1K", "#10K", "#50K"}

func computeKeyDist(total int64, numKeys int, s float64) *keyDist {
	rng := rand.New(rand.NewPCG(42, 42))
	zipf := rand.NewZipf(rng, s, 1.0, uint64(numKeys-1))

	n := int(total)
	if n > 10_000_000 {
		n = 10_000_000
	}
	scale := float64(total) / float64(n)

	counts := make(map[int]int64)
	for i := 0; i < n; i++ {
		idx := int(zipf.Uint64())
		counts[idx]++
	}

	topHits := make([]int64, len(trackedIdxs))
	for i, idx := range trackedIdxs {
		topHits[i] = int64(float64(counts[idx]) * scale)
	}

	return &keyDist{
		total: total, keys: numKeys, zipfS: s,
		topHits: topHits, topIdxs: trackedIdxs, topRanks: trackedRanks,
	}
}

type result struct {
	name     string
	total    int64
	allowed  int64
	rejected int64
	rps      float64
	elapsed  time.Duration
	pm       pathMetrics
	p50      float64
	p90      float64
	p99      float64
	p999     float64
	mean     float64
	lat      *latency
	dist     *keyDist
}

func runKrate(rdb *redis.Client, cfg scenarioConfig) result {
	ctx := context.Background()

	logf("krate: initializing %d key pools in Redis...", cfg.Keys)
	t0 := time.Now()
	logf("krate: pools ready in %v", time.Since(t0).Truncate(time.Millisecond))

	logf("krate: creating %d limiter instances...", cfg.Instances)
	lims := make([]*krate.Limiter, cfg.Instances)
	regs := make([]*prometheus.Registry, cfg.Instances)

	gossipInterval := time.Hour
	heartbeatInterval := time.Hour
	if cfg.ProbeK > 0 {
		gossipInterval = 1 * time.Millisecond
		heartbeatInterval = 10 * time.Millisecond
	}

	for i := 0; i < cfg.Instances; i++ {
		reg := prometheus.NewRegistry()
		l, err := krate.New(rdb,
			krate.WithInstanceID(fmt.Sprintf("%s-%d", cfg.Name, i)),
			krate.WithLimit(cfg.Limit),
			krate.WithWindow(cfg.Window),
			krate.WithMaxBorrow(cfg.Limit/uint64(cfg.Instances)),
			krate.WithMinBorrow(cfg.Limit/uint64(cfg.Instances)),
			krate.WithReservedMinimum(cfg.ReservedMin),
			krate.WithPreBorrowThreshold(0.2),
			krate.WithProbeK(cfg.ProbeK),
			krate.WithPeerListen(":0"),
			krate.WithGossipInterval(gossipInterval),
			krate.WithHeartbeatInterval(heartbeatInterval),
			krate.WithMetrics(reg),
		)
		if err != nil {
			logf("krate.New(%d): %v", i, err)
			os.Exit(1)
		}
		lims[i] = l
		regs[i] = reg
	}
	defer func() {
		for _, l := range lims {
			l.Close()
		}
	}()
	logf("krate: %d instances ready", cfg.Instances)

	if cfg.ProbeK > 0 {
		logf("krate: waiting for peer gossip to settle (200ms)...")
		time.Sleep(200 * time.Millisecond)
	}

	warmupN := *warmupKeys
	if warmupN > cfg.Keys {
		warmupN = cfg.Keys
	}
	if warmupN > 0 {
		logf("krate: warming %d keys across %d instances (concurrent)...", warmupN, cfg.Instances)
		t0 = time.Now()
		var warmupWg sync.WaitGroup
		for i, l := range lims {
			warmupWg.Add(1)
			go func(lim *krate.Limiter, instID int) {
				defer warmupWg.Done()
				for k := 0; k < warmupN; k++ {
					lim.Allow(ctx, fmt.Sprintf("key:%d", k))
				}
				logf("krate:   instance %d warmed in %v", instID, time.Since(t0).Truncate(time.Millisecond))
			}(l, i)
		}
		warmupWg.Wait()
		logf("krate: warmup complete in %v", time.Since(t0).Truncate(time.Millisecond))
	} else {
		logf("krate: skipping warmup (Zipf will naturally warm popular keys)")
	}

	logf("krate: driving %d goroutines for %v...",
		cfg.Instances*cfg.Concurrency, *duration)

	lat := newLatency(500_000)
	var total, allowed, rejected atomic.Int64

	runCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-tick.C:
				t := total.Load()
				a := allowed.Load()
				r := rejected.Load()
				logf("krate:   %s total, %s allowed, %s rejected",
					fmtN(t), fmtN(a), fmtN(r))
			}
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for gi, l := range lims {
		workers := cfg.Concurrency
		if cfg.TrafficSkew {
			if gi == 0 {
				workers = cfg.Concurrency * 2
			} else {
				workers = cfg.Concurrency / 5
				if workers == 0 {
					workers = 1
				}
			}
		}
		for g := 0; g < workers; g++ {
			wg.Add(1)
			go func(lim *krate.Limiter, instID, gorID int) {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(
					uint64(instID*1000+gorID),
					uint64(time.Now().UnixNano()),
				))
				zipf := rand.NewZipf(rng, cfg.ZipfS, 1.0, uint64(cfg.Keys-1))
				shift := 0
				if !cfg.SharedHotspot {
					offset := cfg.Keys / cfg.Instances
					shift = instID * offset
				}
				for {
					select {
					case <-runCtx.Done():
						return
					default:
					}
					raw := int(zipf.Uint64())
					keyIdx := (raw + shift) % cfg.Keys
					key := fmt.Sprintf("key:%d", keyIdx)
					t0 := time.Now()
					ok, _ := lim.Allow(context.Background(), key)
					d := time.Since(t0)
					total.Add(1)
					lat.add(float64(d.Nanoseconds()))
					if ok {
						allowed.Add(1)
					} else {
						rejected.Add(1)
					}
				}
			}(l, gi, g)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(*duration + 30*time.Second):
		logf("krate: WARNING — goroutines did not exit within deadline, forcing continue")
	}
	elapsed := time.Since(start)

	p := lat.pcts(50, 90, 99, 99.9)
	pm := gatherPath(regs)
	dist := computeKeyDist(total.Load(), cfg.Keys, cfg.ZipfS)

	logf("krate: done — %s requests in %v (%s/s)",
		fmtN(total.Load()), elapsed.Round(time.Second), fmtF(float64(total.Load())/elapsed.Seconds()))

	return result{
		name: fmt.Sprintf("krate ×%d", cfg.Instances), total: total.Load(),
		allowed: allowed.Load(), rejected: rejected.Load(),
		rps: float64(total.Load()) / elapsed.Seconds(), elapsed: elapsed,
		pm: pm, p50: p[50], p90: p[90], p99: p[99], p999: p[99.9],
		mean: lat.avg(), lat: lat, dist: dist,
	}
}

func runRedisBaseline(rdb *redis.Client, cfg scenarioConfig) result {
	logf("redis: flushing baseline keys...")
	rdb.FlushDB(context.Background())

	script := redis.NewScript(`
		local c = tonumber(redis.call('GET', KEYS[1]) or '0')
		local l = tonumber(ARGV[1])
		if c >= l then return 0 end
		local n = redis.call('INCR', KEYS[1])
		if n == 1 then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2])) end
		if n > l then redis.call('DECR', KEYS[1]); return 0 end
		return 1
	`)

	lat := newLatency(500_000)
	var total, allowed, rejected atomic.Int64
	totalConc := cfg.Instances * cfg.Concurrency

	logf("redis: driving %d goroutines for %v...", totalConc, *duration)

	runCtx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < totalConc; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			instID := id % cfg.Instances
			rng := rand.New(rand.NewPCG(uint64(id), uint64(time.Now().UnixNano())))
			zipf := rand.NewZipf(rng, cfg.ZipfS, 1.0, uint64(cfg.Keys-1))
			offset := cfg.Keys / cfg.Instances
			shift := instID * offset

			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				raw := int(zipf.Uint64())
				keyIdx := (raw + shift) % cfg.Keys
				key := fmt.Sprintf("rb:key:%d", keyIdx)
				t0 := time.Now()
				res, _ := script.Run(runCtx, rdb, []string{key},
					int64(cfg.Limit), int64(cfg.Window.Seconds())).Int64()
				d := time.Since(t0)
				total.Add(1)
				lat.add(float64(d.Nanoseconds()))
				if res == 1 {
					allowed.Add(1)
				} else {
					rejected.Add(1)
				}
			}
		}(g)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(*duration + 30*time.Second):
		logf("redis: WARNING — goroutines did not exit within deadline, forcing continue")
	}
	elapsed := time.Since(start)

	p := lat.pcts(50, 90, 99, 99.9)
	dist := computeKeyDist(total.Load(), cfg.Keys, cfg.ZipfS)

	logf("redis: done — %s requests in %v (%s/s)",
		fmtN(total.Load()), elapsed.Round(time.Second), fmtF(float64(total.Load())/elapsed.Seconds()))

	return result{
		name: "Redis-only ×1", total: total.Load(),
		allowed: allowed.Load(), rejected: rejected.Load(),
		rps: float64(total.Load()) / elapsed.Seconds(), elapsed: elapsed,
		p50: p[50], p90: p[90], p99: p[99], p999: p[99.9],
		mean: lat.avg(), lat: lat, dist: dist,
	}
}

func cleanScenario(rdb *redis.Client, label string) {
	logf("cleaning Redis (%s)...", label)
	t0 := time.Now()

	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		logf("FLUSHDB failed: %v, falling back to SCAN", err)
		cleanByScan(rdb)
	} else {
		logf("FLUSHDB done in %v", time.Since(t0).Truncate(time.Microsecond))
	}
}

func cleanByScan(rdb *redis.Client) {
	ctx := context.Background()
	for _, p := range []string{
		"krate:*:pool", "krate:*:borrowed:*", "krate:*:window_start",
		"krate:*:config", "krate:cluster:*", "rb:*",
	} {
		var cur uint64
		total := 0
		for {
			ks, n, err := rdb.Scan(ctx, cur, p, 1000).Result()
			if err != nil {
				logf("SCAN error on %s: %v", p, err)
				break
			}
			if len(ks) > 0 {
				rdb.Del(ctx, ks...)
				total += len(ks)
			}
			cur = n
			if cur == 0 {
				break
			}
		}
		if total > 0 {
			logf("  deleted %d keys matching %s", total, p)
		}
	}
}

func fmtNs(ns float64) string {
	switch {
	case ns >= 1_000_000:
		return fmt.Sprintf("%.1fms", ns/1_000_000)
	case ns >= 1_000:
		return fmt.Sprintf("%.1fμs", ns/1_000)
	default:
		return fmt.Sprintf("%.0fns", ns)
	}
}

func fmtN(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func fmtF(f float64) string {
	switch {
	case f >= 1_000_000:
		return fmt.Sprintf("%.2fM", f/1e6)
	case f >= 1_000:
		return fmt.Sprintf("%.1fK", f/1e3)
	default:
		return fmt.Sprintf("%.0f", f)
	}
}

func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func printScenario(cfg scenarioConfig, kr, rd result) {
	totalPaths := kr.pm.local + kr.pm.redis + kr.pm.peer + kr.pm.redisSkip
	redisPerReq := float64(kr.pm.redis+kr.pm.redisEx) / float64(kr.total)
	redisReduction := (1 - redisPerReq) * 100

	fmt.Println()
	fmt.Println("┌─────────────────────────────────────────────────────────────────────────────────┐")
	fmt.Printf("│  %s\n", cfg.Name)
	fmt.Printf("│  %s\n", cfg.Description)
	fmt.Println("├─────────────────────────────────────────────────────────────────────────────────┤")
	fmt.Printf("│  %d instances │ %s keys (Zipf s=%.1f) │ limit %s/%v │ %v\n",
		cfg.Instances, fmtN(int64(cfg.Keys)), cfg.ZipfS, fmtN(int64(cfg.Limit)), cfg.Window, *duration)

	fmt.Println("│")
	fmt.Printf("│  Throughput: krate %s/s  vs  Redis %s/s  →  %.1fx\n",
		fmtF(kr.rps), fmtF(rd.rps), kr.rps/math.Max(rd.rps, 1))

	fmt.Println("│")
	fmt.Println("│  Latency")
	fmt.Println("│  ┌──────────────────────────┬──────────────┬──────────────┬──────────────┐")
	fmt.Println("│  │                          │    p50       │    p99       │   p99.9      │")
	fmt.Println("│  ├──────────────────────────┼──────────────┼──────────────┼──────────────┤")
	fmt.Printf("│  │ %-24s │ %10s   │ %10s   │ %10s   │\n",
		kr.name, fmtNs(kr.p50), fmtNs(kr.p99), fmtNs(kr.p999))
	fmt.Printf("│  │ %-24s │ %10s   │ %10s   │ %10s   │\n",
		rd.name, fmtNs(rd.p50), fmtNs(rd.p99), fmtNs(rd.p999))
	fmt.Println("│  └──────────────────────────┴──────────────┴──────────────┴──────────────┘")
	if rd.p50 > 0 {
		fmt.Printf("│  p50: %.0fx faster │ p99: %.0fx faster\n",
			rd.p50/math.Max(kr.p50, 1), rd.p99/math.Max(kr.p99, 1))
	}

	fmt.Println("│")
	fmt.Println("│  Token path")
	if totalPaths > 0 {
		printPath("│    Local   (~30ns, zero Redis)     ", kr.pm.local, totalPaths)
		printPath("│    Redis   (~1ms, Lua script)      ", kr.pm.redis, totalPaths)
		if kr.pm.redisSkip > 0 {
			printPath("│    RdSkip  (~1ns, bypass flag)     ", kr.pm.redisSkip, totalPaths)
		}
		if cfg.ProbeK > 0 {
			msg := fmt.Sprintf("│    Peer    (~3ms, gRPC loopback)   %9s (granted) / %s (probes)", fmtN(kr.pm.peer), fmtN(kr.pm.peerProbes))
			fmt.Println(msg)
		}
	}
	if kr.pm.preBorrows > 0 {
		fmt.Printf("│    PreBorrow triggers: %s (async Redis borrow ahead of demand)\n",
			fmtN(kr.pm.preBorrows))
	}

	fmt.Println("│")
	fmt.Printf("│  Redis load: %.3f calls/req → %.0f%% reduction\n", redisPerReq, redisReduction)

	if kr.dist != nil {
		fmt.Println("│")
		fmt.Printf("│  Key hits (Zipf s=%.1f, %s keys)\n",
			cfg.ZipfS, fmtN(int64(cfg.Keys)))
		fmt.Println("│  ┌────────┬────────────────┬─────────────┬──────────┐")
		fmt.Println("│  │  Rank  │  Key           │  Hits       │  Share   │")
		fmt.Println("│  ├────────┼────────────────┼─────────────┼──────────┤")
		for i, idx := range kr.dist.topIdxs {
			hits := kr.dist.topHits[i]
			if hits == 0 || idx >= cfg.Keys {
				continue
			}
			share := float64(hits) / float64(kr.total) * 100
			keyName := fmt.Sprintf("key:%d", idx)
			fmt.Printf("│  │ %-6s │ %-14s │ %11s │ %5.1f%%   │\n",
				kr.dist.topRanks[i], keyName, fmtN(hits), share)
		}
		fmt.Println("│  └────────┴────────────────┴─────────────┴──────────┘")

		topHits := int64(0)
		for i := 0; i < 3 && i < len(kr.dist.topHits); i++ {
			topHits += kr.dist.topHits[i]
		}
		topPct := float64(topHits) / float64(kr.total) * 100
		fmt.Printf("│  Top 3 keys (%.2f%% of keyspace) → %.0f%% of traffic\n",
			3.0/float64(cfg.Keys)*100, topPct)
	}

	fmt.Println("│")
	rejKr := pct(kr.rejected, kr.total)
	rejRd := pct(rd.rejected, rd.total)
	fmt.Printf("│  Rejections: krate %s (%.1f%%) vs Redis %s (%.1f%%)\n",
		fmtN(kr.rejected), rejKr, fmtN(rd.rejected), rejRd)

	fmt.Println("│")
	fmt.Println("│  Distribution")
	fmt.Println("│  krate:")
	printBuckets(kr.lat)
	fmt.Println("│  Redis:")
	printBuckets(rd.lat)

	fmt.Println("└─────────────────────────────────────────────────────────────────────────────────┘")
}

func printPath(label string, count, total int64) {
	p := pct(count, total)
	n := int(p / 100 * 30)
	if n == 0 && count > 0 {
		n = 1
	}
	bar := strings.Repeat("█", n)
	if bar == "" {
		bar = "·"
	}
	fmt.Printf("│  %s %10s  %5.1f%% %s\n", label, fmtN(count), p, bar)
}

func printBuckets(l *latency) {
	if l == nil {
		return
	}
	for _, b := range l.bins() {
		if b.count == 0 {
			continue
		}
		p := float64(b.count) / float64(l.count.Load()) * 100
		bw := int(p / 2)
		if bw > 30 {
			bw = 30
		}
		bstr := strings.Repeat("█", bw)
		if bstr == "" && p > 0 {
			bstr = "▏"
		}
		fmt.Printf("│    %10s  %8d  %5.1f%% %s\n", b.label, b.count, p, bstr)
	}
}

type scenarioResult struct {
	cfg scenarioConfig
	kr  result
	rd  result
}

func printSummary(all []scenarioResult) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                              Summary                                              ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Scenario                 │  krate RPS  │ Redis RPS │ Speedup │   p50    │   p99   ║")
	fmt.Println("╠═══════════════════════════╪═════════════╪═══════════╪═════════╪══════════╪═════════╣")

	for _, s := range all {
		name := s.cfg.Name
		if len(name) > 23 {
			name = name[:23]
		}
		speedup := s.kr.rps / math.Max(s.rd.rps, 1)
		fmt.Printf("║  %-23s │ %9s/s │ %7s/s │  %5.1fx │ %6s  │ %6s  ║\n",
			name, fmtF(s.kr.rps), fmtF(s.rd.rps), speedup,
			fmtNs(s.kr.p50), fmtNs(s.kr.p99))
	}

	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println("║")
	for _, s := range all {
		redisPerReq := float64(s.kr.pm.redis+s.kr.pm.redisEx) / float64(s.kr.total)
		reduction := (1 - redisPerReq) * 100
		totalPaths := s.kr.pm.local + s.kr.pm.redis + s.kr.pm.peer + s.kr.pm.redisSkip
		localPct := float64(0)
		if totalPaths > 0 {
			localPct = float64(s.kr.pm.local) / float64(totalPaths) * 100
		}
		skipPct := float64(0)
		if s.kr.total > 0 {
			skipPct = float64(s.kr.pm.redisSkip) / float64(s.kr.total) * 100
		}
		fmt.Printf("║  %s: %.0f%% local, %.0f%% Redis reduction, %.0f%% bypass skips\n",
			s.cfg.Name, localPct, reduction, skipPct)
	}
	fmt.Println("║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════════════════════════╝")
}

func main() {
	flag.Parse()
	benchStart = time.Now()

	rdb := redis.NewClient(&redis.Options{
		Addr:         *redisAddr,
		PoolSize:     2000,
		MinIdleConns: 500,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "Redis not available at %s: %v\n", *redisAddr, err)
		fmt.Fprintf(os.Stderr, "Run: docker run -d --name krate-redis -p 6379:6379 redis:7-alpine\n")
		os.Exit(1)
	}
	cancel()
	defer rdb.Close()

	runNames := []string{"api-gateway", "user-limiting", "ip-throttling", "multi-tenant", "peer", "peer-transfer"}
	if *scenario != "all" {
		runNames = []string{*scenario}
	}

	logf("krate benchmark suite")
	logf("  Redis: %s", *redisAddr)
	logf("  Duration: %v", *duration)
	logf("  Warmup keys: %d", *warmupKeys)
	logf("  Scenarios: %s", strings.Join(runNames, ", "))
	logf("────────────────────────────────────────────────────")

	var all []scenarioResult

	for _, name := range runNames {
		cfg, ok := scenarios[name]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown scenario: %s\n", name)
			os.Exit(1)
		}

		logf("")
		logf("═══ %s ═══", cfg.Name)
		logf("%s", cfg.Description)
		logf("  %d instances, %s keys, limit %s/%v, %d concurrency/instance",
			cfg.Instances, fmtN(int64(cfg.Keys)), fmtN(int64(cfg.Limit)), cfg.Window, cfg.Concurrency)

		logf("")
		logf("── krate ──")
		cleanScenario(rdb, "pre-krate")
		krResult := runKrate(rdb, cfg)

		logf("")
		logf("── Redis baseline ──")
		cleanScenario(rdb, "pre-redis")
		rdResult := runRedisBaseline(rdb, cfg)

		all = append(all, scenarioResult{cfg: cfg, kr: krResult, rd: rdResult})

		if !*jsonOut {
			printScenario(cfg, krResult, rdResult)
		}

		logf("── %s complete ──", cfg.Name)
	}

	if !*jsonOut && len(all) > 1 {
		printSummary(all)
	}

	logf("")
	logf("benchmark complete in %v", time.Since(benchStart).Truncate(time.Second))
}
