package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	RequestsTotal        *prometheus.CounterVec
	LocalHitsTotal       *prometheus.CounterVec
	RedisBorrowsTotal    *prometheus.CounterVec
	RedisSkipsTotal      *prometheus.CounterVec
	PreBorrowsTotal      *prometheus.CounterVec
	PeerProbesTotal      *prometheus.CounterVec
	PeerProbeStaleTotal  *prometheus.CounterVec
	TokensTransferred    *prometheus.CounterVec
	TokensReturned       prometheus.Counter
	WindowResetsTotal    *prometheus.CounterVec
	GossipsSentTotal     prometheus.Counter
	GossipsReceivedTotal prometheus.Counter
	RequestDuration      *prometheus.HistogramVec
	BorrowSize           prometheus.Histogram
	LocalTokensRemaining prometheus.Histogram
	LocalTokens          *prometheus.GaugeVec
	BorrowedTokens       *prometheus.GaugeVec
	KnownPeers           prometheus.Gauge
	CMSStaleness         *prometheus.GaugeVec
}

func NewCollector(reg prometheus.Registerer) *Collector {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	c := &Collector{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_requests_total",
			Help: "Total number of rate limit requests.",
		}, []string{"key", "result"}),

		LocalHitsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_local_hits_total",
			Help: "Total number of requests served from local token bucket.",
		}, []string{"key"}),

		RedisBorrowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_redis_borrows_total",
			Help: "Total number of Redis borrow attempts.",
		}, []string{"key", "result"}),

		RedisSkipsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_redis_skips_total",
			Help: "Total requests that bypassed Redis because the pool was known-exhausted.",
		}, []string{"key"}),

		PreBorrowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_pre_borrows_total",
			Help: "Total async pre-borrow goroutines triggered.",
		}, []string{"key"}),

		PeerProbesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_peer_probes_total",
			Help: "Total number of peer probe attempts.",
		}, []string{"key", "result"}),

		PeerProbeStaleTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_peer_probe_stale_total",
			Help: "Total number of stale peer detections.",
		}, []string{"key", "peer_id"}),

		TokensTransferred: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_tokens_transferred_total",
			Help: "Total tokens transferred between peers.",
		}, []string{"direction"}),

		TokensReturned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "krate_tokens_returned_total",
			Help: "Total tokens returned to Redis.",
		}),

		WindowResetsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krate_window_resets_total",
			Help: "Total number of rate limit window resets.",
		}, []string{"key"}),

		GossipsSentTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "krate_gossips_sent_total",
			Help: "Total gossip broadcasts sent.",
		}),

		GossipsReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "krate_gossips_received_total",
			Help: "Total gossip messages received.",
		}),

		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "krate_request_duration_seconds",
			Help: "Request duration in seconds by phase.",
			Buckets: prometheus.ExponentialBuckets(
				0.000001,
				4,
				12,
			),
		}, []string{"phase"}),

		BorrowSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "krate_borrow_size",
			Help:    "Distribution of borrow sizes from Redis.",
			Buckets: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000},
		}),

		LocalTokensRemaining: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "krate_local_tokens_remaining",
			Help:    "Distribution of local tokens remaining after requests.",
			Buckets: []float64{0, 10, 50, 100, 500, 1000, 5000, 10000},
		}),

		LocalTokens: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "krate_local_tokens",
			Help: "Current local token count per key.",
		}, []string{"key"}),

		BorrowedTokens: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "krate_borrowed_tokens",
			Help: "Current borrowed token count per key.",
		}, []string{"key"}),

		KnownPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "krate_known_peers",
			Help: "Number of known cluster peers.",
		}),

		CMSStaleness: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "krate_cms_staleness_seconds",
			Help: "Age of the most recent CMS snapshot from each peer.",
		}, []string{"peer_id"}),
	}

	reg.MustRegister(
		c.RequestsTotal,
		c.LocalHitsTotal,
		c.RedisBorrowsTotal,
		c.RedisSkipsTotal,
		c.PreBorrowsTotal,
		c.PeerProbesTotal,
		c.PeerProbeStaleTotal,
		c.TokensTransferred,
		c.TokensReturned,
		c.WindowResetsTotal,
		c.GossipsSentTotal,
		c.GossipsReceivedTotal,
		c.RequestDuration,
		c.BorrowSize,
		c.LocalTokensRemaining,
		c.LocalTokens,
		c.BorrowedTokens,
		c.KnownPeers,
		c.CMSStaleness,
	)

	return c
}
