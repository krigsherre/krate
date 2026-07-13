package krate

type WindowType int

const (
	Fixed WindowType = iota
	Sliding
)

type PeerStrategy int

const (
	Highest PeerStrategy = iota
	Random
)

type ProbeMode int

const (
	Parallel ProbeMode = iota
	Sequential
)

type PoolState struct {
	Remaining   uint64
	WindowStart int64
	Limit       uint64
	WindowMs    int64
}

type Member struct {
	ID       string
	Addr     string
	Metadata map[string]string
}

type PeerInfo struct {
	ID       string
	Addr     string
	GRPCAddr string
}
