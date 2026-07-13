package krate

import "errors"

var (
	ErrPoolExhausted  = errors.New("krate: redis pool exhausted")
	ErrNoPeerTokens   = errors.New("krate: no peer tokens available")
	ErrGlobalLimit    = errors.New("krate: global rate limit reached")
	ErrInstanceExists = errors.New("krate: instance already registered")
	ErrKeyNotFound    = errors.New("krate: rate limit key not found")
	ErrClosed         = errors.New("krate: limiter is closed")
	ErrCycle          = errors.New("krate: request cycle detected")
)
