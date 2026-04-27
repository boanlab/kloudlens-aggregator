// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package main

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator"
)

// TestStatsPlaintextCoversEveryStatsField locks in that the /stats
// plaintext line carries every field Aggregator.Stats() exposes. Any new
// Stats field that isn't reflected in this line is a regression.
func TestStatsPlaintextCoversEveryStatsField(t *testing.T) {
	agg, err := aggregator.New(aggregator.Config{
		Agents: []aggregator.AgentEndpoint{{Name: "a", Addr: "127.0.0.1:0"}},
		Out:    io.Discard,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := buildMetricsMux(agg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/stats", nil)
	h.ServeHTTP(rec, req)
	body := rec.Body.String()

	// Every token the plaintext endpoint documents as a contract. Order
	// isn't asserted — callers grep for `key=` — but absence is.
	wantTokens := []string{
		"received=",
		"written=",
		"dropped=",
		"errors=",
		"wal_appends=",
		"wal_errors=",
		"wal_last_seq=",
		"subscriber_dropped=",
		"subscribers=",
	}
	for _, tok := range wantTokens {
		if !strings.Contains(body, tok) {
			t.Errorf("/stats missing %q: %s", tok, body)
		}
	}
	// Empty aggregator: nothing received, no subs connected.
	if !strings.Contains(body, "received=0") || !strings.Contains(body, "subscribers=0") {
		t.Errorf("/stats zero-state incorrect: %s", body)
	}
}

func TestHealthzReadyzOK(t *testing.T) {
	agg, err := aggregator.New(aggregator.Config{
		Agents: []aggregator.AgentEndpoint{{Name: "a", Addr: "127.0.0.1:0"}},
		Out:    io.Discard,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := buildMetricsMux(agg)

	for _, p := range []string{"/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		if rec.Code != 200 || rec.Body.String() != "ok" {
			t.Errorf("%s = %d %q, want 200 ok", p, rec.Code, rec.Body.String())
		}
	}
}
