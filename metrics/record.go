package metrics

import "time"

func (c *Collector) RecordAllowed(key string) {
	if c == nil {
		return
	}
	c.RequestsTotal.WithLabelValues(key, "allowed").Inc()
}

func (c *Collector) RecordRejected(key string) {
	if c == nil {
		return
	}
	c.RequestsTotal.WithLabelValues(key, "rejected").Inc()
}

func (c *Collector) RecordLocalHit(key string) {
	if c == nil {
		return
	}
	c.LocalHitsTotal.WithLabelValues(key).Inc()
}

func (c *Collector) RecordRedisBorrow(key string, granted bool) {
	if c == nil {
		return
	}
	result := "exhausted"
	if granted {
		result = "granted"
	}
	c.RedisBorrowsTotal.WithLabelValues(key, result).Inc()
}

func (c *Collector) RecordPeerProbe(key, result string) {
	if c == nil {
		return
	}
	c.PeerProbesTotal.WithLabelValues(key, result).Inc()
}

func (c *Collector) RecordPeerStale(key, peerID string) {
	if c == nil {
		return
	}
	c.PeerProbeStaleTotal.WithLabelValues(key, peerID).Inc()
}

func (c *Collector) RecordTokenSent(count uint64) {
	if c == nil {
		return
	}
	c.TokensTransferred.WithLabelValues("sent").Add(float64(count))
}

func (c *Collector) RecordTokenReceived(count uint64) {
	if c == nil {
		return
	}
	c.TokensTransferred.WithLabelValues("received").Add(float64(count))
}

func (c *Collector) RecordTokensReturned(count uint64) {
	if c == nil {
		return
	}
	c.TokensReturned.Add(float64(count))
}

func (c *Collector) RecordWindowReset(key string) {
	if c == nil {
		return
	}
	c.WindowResetsTotal.WithLabelValues(key).Inc()
}

func (c *Collector) RecordGossipSent() {
	if c == nil {
		return
	}
	c.GossipsSentTotal.Inc()
}

func (c *Collector) RecordGossipReceived() {
	if c == nil {
		return
	}
	c.GossipsReceivedTotal.Inc()
}

func (c *Collector) ObserveRequestDuration(phase string, d time.Duration) {
	if c == nil {
		return
	}
	c.RequestDuration.WithLabelValues(phase).Observe(d.Seconds())
}

func (c *Collector) SetLocalTokens(key string, tokens uint64) {
	if c == nil {
		return
	}
	c.LocalTokens.WithLabelValues(key).Set(float64(tokens))
}

func (c *Collector) SetBorrowedTokens(key string, tokens uint64) {
	if c == nil {
		return
	}
	c.BorrowedTokens.WithLabelValues(key).Set(float64(tokens))
}

func (c *Collector) SetKnownPeers(count int) {
	if c == nil {
		return
	}
	c.KnownPeers.Set(float64(count))
}

func (c *Collector) ObserveBorrowSize(size uint64) {
	if c == nil {
		return
	}
	c.BorrowSize.Observe(float64(size))
}

func (c *Collector) ObserveLocalTokensRemaining(tokens uint64) {
	if c == nil {
		return
	}
	c.LocalTokensRemaining.Observe(float64(tokens))
}

func (c *Collector) SetCMSStaleness(peerID string, ageSeconds float64) {
	if c == nil {
		return
	}
	c.CMSStaleness.WithLabelValues(peerID).Set(ageSeconds)
}

func (c *Collector) RecordRedisSkip(key string) {
	if c == nil {
		return
	}
	c.RedisSkipsTotal.WithLabelValues(key).Inc()
}

func (c *Collector) RecordPreBorrow(key string) {
	if c == nil {
		return
	}
	c.PreBorrowsTotal.WithLabelValues(key).Inc()
}
