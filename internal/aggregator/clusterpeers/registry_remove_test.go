// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package clusterpeers

import (
	"testing"
	"time"
)

// TestRemoveExactRetiresBeforeTTL verifies a withdraw drops an exact listener
// immediately, well within its TTL, closing the wrong-peer window that TTL-only
// retirement leaves open when a pod IP:port is reused by a new process.
func TestRemoveExactRetiresBeforeTTL(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestRegistry(t, clk, nil)
	r.Observe(lis("10.0.0.5:8080", "node-b"))

	if _, _, ok := r.Join("10.0.0.5:8080", "node-a"); !ok {
		t.Fatal("expected hit before withdraw")
	}
	// Withdraw the listener (binder exited) — no clock advance, TTL not reached.
	r.Remove(Listener{Addr: "10.0.0.5:8080"})
	if _, how, ok := r.Join("10.0.0.5:8080", "node-a"); ok || how != HowMiss {
		t.Fatalf("expected miss after withdraw, got ok=%v how=%v", ok, how)
	}
	if e, _ := r.Stats(); e != 0 {
		t.Errorf("withdrawn entry not removed: exact=%d", e)
	}
}

// TestRemoveWildcardScopedToPod verifies a wildcard withdraw retires only the
// named pod's entry, keyed by (port, namespace, pod) exactly as Observe indexed
// it, and leaves a co-bound pod on the same port intact.
func TestRemoveWildcardScopedToPod(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestRegistry(t, clk, nil)
	w := lis("0.0.0.0:9090", "node-c") // Namespace "prod", Pod "app-0"
	w.Wildcard = true
	r.Observe(w)

	if _, _, ok := r.Join("10.1.2.3:9090", "node-a"); !ok {
		t.Fatal("expected wildcard hit before withdraw")
	}
	// A withdraw for a DIFFERENT pod on the same port must not touch app-0.
	// (Addr is the wildcard bind addr, exactly as the agent sends it.)
	r.Remove(Listener{Addr: "0.0.0.0:9090", Port: "9090", Wildcard: true, Namespace: "prod", Pod: "other"})
	if _, _, ok := r.Join("10.1.2.3:9090", "node-a"); !ok {
		t.Fatal("unrelated-pod withdraw wrongly retired app-0")
	}
	// Withdraw the actual pod: now it misses.
	r.Remove(Listener{Addr: "0.0.0.0:9090", Port: "9090", Wildcard: true, Namespace: "prod", Pod: "app-0"})
	if _, how, ok := r.Join("10.1.2.3:9090", "node-a"); ok || how != HowMiss {
		t.Fatalf("expected miss after withdraw, got ok=%v how=%v", ok, how)
	}
}
