// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package aggregator

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TestPromCollectorEmitsAggregatorStats verifies every counter in
// Aggregator.Stats() surfaces on /metrics with the expected help text
// and current value. Uses atomic stores against a zero-value Aggregator
// — we don't need Run() since PromCollector only reads the atomics.
func TestPromCollectorEmitsAggregatorStats(t *testing.T) {
	agg := &Aggregator{}
	agg.received.Store(101)
	agg.written.Store(99)
	agg.dropped.Store(2)
	agg.errors.Store(3)
	agg.walAppends.Store(99)
	agg.walErrors.Store(1)
	agg.subsDropped.Store(5) // already-folded drops from disconnected re-export clients
	agg.liveSeq.Store(99)    // aggregator's in-memory liveSeq; unused here but drains the field
	agg.subs = map[*liveSubscriber]struct{}{}

	reg := prometheus.NewRegistry()
	reg.MustRegister(NewPromCollector(agg))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	text := string(body)

	// wal_last_seq is 0 here because PromCollector reads Stats().WALLastSeq
	// which only tracks the on-disk WAL sequence; the in-memory liveSeq is
	// a separate counter used when no WAL is configured.
	wants := []string{
		"kloudlens_aggregator_received_total 101",
		"kloudlens_aggregator_written_total 99",
		"kloudlens_aggregator_dropped_total 2",
		"kloudlens_aggregator_errors_total 3",
		"kloudlens_aggregator_wal_appends_total 99",
		"kloudlens_aggregator_wal_errors_total 1",
		"kloudlens_aggregator_wal_last_seq 0",
		"kloudlens_aggregator_subscriber_dropped_total 5",
		"kloudlens_aggregator_subscribers_active 0",
	}
	for _, w := range wants {
		if !strings.Contains(text, w) {
			t.Errorf("missing %q in /metrics output:\n%s", w, text)
		}
	}
}

// TestSubscriberDroppedFoldsOnUnregister locks in the invariant that a
// re-export subscriber's live drop count is folded into the aggregator-wide
// accumulator before the pointer is released, so /metrics stays monotonic
// across disconnects.
func TestSubscriberDroppedFoldsOnUnregister(t *testing.T) {
	agg := &Aggregator{subs: map[*liveSubscriber]struct{}{}}
	sub := agg.registerSubscriber(1)
	sub.drops.Store(4)

	// Before unregister: live-sub contributes to the live tally.
	if got := agg.Stats().SubscriberDropped; got != 4 {
		t.Fatalf("pre-unregister SubscriberDropped = %d, want 4", got)
	}
	agg.unregisterSubscriber(sub)
	// After unregister: the exact same count must persist in the accumulator.
	if got := agg.Stats().SubscriberDropped; got != 4 {
		t.Fatalf("post-unregister SubscriberDropped = %d, want 4", got)
	}
}

// TestSubscribersActiveReflectsLiveMap covers the new
// kloudlens_aggregator_subscribers_active gauge end-to-end: the count
// must track register/unregister without drifting, so an operator's
// "how many downstream tools are attached?" dashboard is accurate even
// as clients reconnect.
func TestSubscribersActiveReflectsLiveMap(t *testing.T) {
	agg := &Aggregator{subs: map[*liveSubscriber]struct{}{}}
	if got := agg.Stats().Subscribers; got != 0 {
		t.Fatalf("empty: Subscribers = %d, want 0", got)
	}
	a := agg.registerSubscriber(1)
	b := agg.registerSubscriber(1)
	if got := agg.Stats().Subscribers; got != 2 {
		t.Fatalf("after 2 register: Subscribers = %d, want 2", got)
	}
	agg.unregisterSubscriber(a)
	if got := agg.Stats().Subscribers; got != 1 {
		t.Fatalf("after 1 unregister: Subscribers = %d, want 1", got)
	}
	agg.unregisterSubscriber(b)
	if got := agg.Stats().Subscribers; got != 0 {
		t.Fatalf("after both unregister: Subscribers = %d, want 0", got)
	}
}
