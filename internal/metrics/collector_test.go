package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func newTestCollector(t *testing.T) (*Collector, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := NewCollector(reg)
	return c, reg
}

func gatherNames(t *testing.T, reg *prometheus.Registry) map[string]bool {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	return names
}

func gatherFamilies(t *testing.T, reg *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func TestCollectorRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewCollector(reg)
}

func TestRecordAllowed(t *testing.T) {
	c, reg := newTestCollector(t)

	c.RecordAllowed("api:login")
	c.RecordAllowed("api:login")
	c.RecordAllowed("api:signup")

	families := gatherFamilies(t, reg)
	mf, ok := families["krate_requests_total"]
	if !ok {
		t.Fatal("krate_requests_total not found")
	}

	for _, m := range mf.GetMetric() {
		labels := make(map[string]string)
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		if labels["key"] == "api:login" && labels["result"] == "allowed" {
			if m.GetCounter().GetValue() != 2 {
				t.Errorf("api:login allowed = %v, want 2", m.GetCounter().GetValue())
			}
		}
		if labels["key"] == "api:signup" && labels["result"] == "allowed" {
			if m.GetCounter().GetValue() != 1 {
				t.Errorf("api:signup allowed = %v, want 1", m.GetCounter().GetValue())
			}
		}
	}
}

func TestNilCollectorSafety(t *testing.T) {
	var c *Collector

	c.RecordAllowed("k")
	c.RecordRejected("k")
	c.RecordLocalHit("k")
	c.RecordRedisBorrow("k", true)
	c.RecordRedisBorrow("k", false)
	c.RecordPeerProbe("k", "granted")
	c.RecordPeerStale("k", "p1")
	c.RecordTokenSent(100)
	c.RecordTokenReceived(100)
	c.RecordTokensReturned(100)
	c.RecordWindowReset("k")
	c.RecordGossipSent()
	c.RecordGossipReceived()
	c.ObserveRequestDuration("local", 5*time.Millisecond)
	c.SetLocalTokens("k", 100)
	c.SetBorrowedTokens("k", 50)
	c.SetKnownPeers(3)
	c.ObserveBorrowSize(500)
	c.ObserveLocalTokensRemaining(100)
	c.SetCMSStaleness("p1", 1.5)
}

func TestAllMetricsRegistered(t *testing.T) {
	c, reg := newTestCollector(t)

	c.RequestsTotal.WithLabelValues("k", "allowed").Inc()
	c.LocalHitsTotal.WithLabelValues("k").Inc()
	c.RedisBorrowsTotal.WithLabelValues("k", "granted").Inc()
	c.PeerProbesTotal.WithLabelValues("k", "granted").Inc()
	c.PeerProbeStaleTotal.WithLabelValues("k", "p").Inc()
	c.TokensTransferred.WithLabelValues("sent").Inc()
	c.TokensReturned.Inc()
	c.WindowResetsTotal.WithLabelValues("k").Inc()
	c.GossipsSentTotal.Inc()
	c.GossipsReceivedTotal.Inc()
	c.RequestDuration.WithLabelValues("local").Observe(0.001)
	c.BorrowSize.Observe(100)
	c.LocalTokensRemaining.Observe(50)
	c.LocalTokens.WithLabelValues("k").Set(1)
	c.BorrowedTokens.WithLabelValues("k").Set(1)
	c.KnownPeers.Set(1)
	c.CMSStaleness.WithLabelValues("p").Set(0.1)

	names := gatherNames(t, reg)

	expected := []string{
		"krate_requests_total",
		"krate_local_hits_total",
		"krate_redis_borrows_total",
		"krate_peer_probes_total",
		"krate_peer_probe_stale_total",
		"krate_tokens_transferred_total",
		"krate_tokens_returned_total",
		"krate_window_resets_total",
		"krate_gossips_sent_total",
		"krate_gossips_received_total",
		"krate_request_duration_seconds",
		"krate_borrow_size",
		"krate_local_tokens_remaining",
		"krate_local_tokens",
		"krate_borrowed_tokens",
		"krate_known_peers",
		"krate_cms_staleness_seconds",
	}

	for _, name := range expected {
		if !names[name] {
			t.Errorf("metric %q not registered", name)
		}
	}

	if len(names) != len(expected) {
		for name := range names {
			found := false
			for _, e := range expected {
				if name == e {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("unexpected metric %q registered", name)
			}
		}
	}
}

func TestRecordRequestDuration(t *testing.T) {
	c, reg := newTestCollector(t)

	c.ObserveRequestDuration("local", 1*time.Millisecond)
	c.ObserveRequestDuration("local", 2*time.Millisecond)
	c.ObserveRequestDuration("redis", 10*time.Millisecond)
	c.ObserveRequestDuration("peer", 5*time.Millisecond)

	families := gatherFamilies(t, reg)
	mf, ok := families["krate_request_duration_seconds"]
	if !ok {
		t.Fatal("krate_request_duration_seconds not found")
	}

	phases := make(map[string]uint64)
	for _, m := range mf.GetMetric() {
		var phase string
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "phase" {
				phase = lp.GetValue()
			}
		}
		phases[phase] = m.GetHistogram().GetSampleCount()
	}

	if phases["local"] != 2 {
		t.Errorf("local sample count = %d, want 2", phases["local"])
	}
	if phases["redis"] != 1 {
		t.Errorf("redis sample count = %d, want 1", phases["redis"])
	}
	if phases["peer"] != 1 {
		t.Errorf("peer sample count = %d, want 1", phases["peer"])
	}
}

func TestRecordRedisBorrow(t *testing.T) {
	c, reg := newTestCollector(t)

	c.RecordRedisBorrow("api:login", true)
	c.RecordRedisBorrow("api:login", true)
	c.RecordRedisBorrow("api:login", false)

	families := gatherFamilies(t, reg)
	mf := families["krate_redis_borrows_total"]

	for _, m := range mf.GetMetric() {
		labels := make(map[string]string)
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		val := m.GetCounter().GetValue()
		if labels["result"] == "granted" && val != 2 {
			t.Errorf("granted = %v, want 2", val)
		}
		if labels["result"] == "exhausted" && val != 1 {
			t.Errorf("exhausted = %v, want 1", val)
		}
	}
}

func TestGatherProducesValidText(t *testing.T) {
	c, reg := newTestCollector(t)

	c.RecordAllowed("test")
	c.ObserveRequestDuration("local", 5*time.Millisecond)
	c.SetKnownPeers(3)
	c.ObserveBorrowSize(250)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var sb strings.Builder
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&sb, mf); err != nil {
			t.Errorf("MetricFamilyToText(%s): %v", mf.GetName(), err)
		}
	}

	output := sb.String()
	if !strings.Contains(output, "krate_requests_total") {
		t.Error("text output missing krate_requests_total")
	}
	if !strings.Contains(output, "krate_known_peers") {
		t.Error("text output missing krate_known_peers")
	}
}
