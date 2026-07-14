package krate

import (
	"log/slog"
	"time"

	"github.com/krigsherre/krate/routing"
	"github.com/prometheus/client_golang/prometheus"
)

type Option func(*options)

type options struct {
	instanceID string

	window         time.Duration
	windowType     WindowType
	limit          uint64
	minBorrow      uint64
	maxBorrow      uint64
	adaptiveBorrow bool
	emaAlpha       float64

	leaseTTL     time.Duration
	peerListen   string
	peerGRPCAddr string
	peerStrategy PeerStrategy
	probeK       int
	probeTimeout time.Duration
	probeMode    ProbeMode

	gossipInterval time.Duration
	gossipAddr     string

	cmsWidth uint32
	cmsDepth uint32
	cmsSeed  uint64

	reservedMinimum   float64
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	preBorrowThreshold float64
	preBorrowEnabled   bool

	logger  *slog.Logger
	metrics prometheus.Registerer
	clock   Clock

	router        routing.Router
	maxGossipKeys int
	gzipCompression bool
}

func defaultOptions() options {
	return options{
		window:             60 * time.Second,
		windowType:         Fixed,
		limit:              10000,
		minBorrow:          100,
		maxBorrow:          1000,
		adaptiveBorrow:     true,
		emaAlpha:           0.3,
		leaseTTL:           30 * time.Second,
		peerListen:         ":7100",
		peerGRPCAddr:       "",
		peerStrategy:       Highest,
		probeK:             3,
		probeTimeout:       2 * time.Millisecond,
		probeMode:          Parallel,
		gossipInterval:     100 * time.Millisecond,
		cmsWidth:           256,
		cmsDepth:           4,
		cmsSeed:            42,
		reservedMinimum:    0.10,
		heartbeatInterval:  1 * time.Second,
		heartbeatTimeout:   5 * time.Second,
		preBorrowEnabled:   true,
		preBorrowThreshold: 0.20,
		router:             routing.NewDefaultRouter(),
		maxGossipKeys:      3000,
		gzipCompression:    false,
	}
}

func WithInstanceID(id string) Option           { return func(o *options) { o.instanceID = id } }
func WithWindow(d time.Duration) Option         { return func(o *options) { o.window = d } }
func WithWindowType(wt WindowType) Option       { return func(o *options) { o.windowType = wt } }
func WithLimit(n uint64) Option                 { return func(o *options) { o.limit = n } }
func WithMinBorrow(n uint64) Option             { return func(o *options) { o.minBorrow = n } }
func WithMaxBorrow(n uint64) Option             { return func(o *options) { o.maxBorrow = n } }
func WithAdaptiveBorrow(b bool) Option          { return func(o *options) { o.adaptiveBorrow = b } }
func WithEMAAlpha(a float64) Option             { return func(o *options) { o.emaAlpha = a } }
func WithLeaseTTL(d time.Duration) Option       { return func(o *options) { o.leaseTTL = d } }
func WithPeerListen(addr string) Option         { return func(o *options) { o.peerListen = addr } }
func WithPeerGRPCAddr(addr string) Option       { return func(o *options) { o.peerGRPCAddr = addr } }
func WithPeerStrategy(ps PeerStrategy) Option   { return func(o *options) { o.peerStrategy = ps } }
func WithProbeK(k int) Option                   { return func(o *options) { o.probeK = k } }
func WithProbeTimeout(d time.Duration) Option   { return func(o *options) { o.probeTimeout = d } }
func WithProbeMode(pm ProbeMode) Option         { return func(o *options) { o.probeMode = pm } }
func WithGossipInterval(d time.Duration) Option { return func(o *options) { o.gossipInterval = d } }
func WithGossipAddr(addr string) Option         { return func(o *options) { o.gossipAddr = addr } }
func WithCMSWidth(w uint32) Option              { return func(o *options) { o.cmsWidth = w } }
func WithCMSDepth(d uint32) Option              { return func(o *options) { o.cmsDepth = d } }
func WithCMSSeed(s uint64) Option               { return func(o *options) { o.cmsSeed = s } }
func WithReservedMinimum(f float64) Option      { return func(o *options) { o.reservedMinimum = f } }
func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *options) { o.heartbeatInterval = d }
}
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(o *options) { o.heartbeatTimeout = d }
}
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}
func WithMetrics(m prometheus.Registerer) Option {
	return func(o *options) { o.metrics = m }
}
func WithClock(c Clock) Option {
	return func(o *options) { o.clock = c }
}

func WithPreBorrowEnabled(b bool) Option {
	return func(o *options) { o.preBorrowEnabled = b }
}

func WithPreBorrowThreshold(f float64) Option {
	return func(o *options) {
		if f <= 0 {
			f = 0.01
		}
		if f > 1 {
			f = 1
		}
		o.preBorrowThreshold = f
	}
}

func WithRouter(r routing.Router) Option {
	return func(o *options) { o.router = r }
}

func WithMaxGossipKeys(n int) Option {
	return func(o *options) { o.maxGossipKeys = n }
}

func WithGzipCompression(b bool) Option {
	return func(o *options) { o.gzipCompression = b }
}
