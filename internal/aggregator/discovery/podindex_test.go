// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package discovery

import (
	"encoding/json"
	"testing"
)

func TestPodIndexIdentityAndPortFixup(t *testing.T) {
	idx := NewPodEndpointIndex()
	idx.Replace([]PodEndpointEntry{
		{IP: "10.0.0.5", Ports: []int32{8080}},
	})

	// 1. A connect already at the live endpoint addr resolves to itself.
	if got, ok := idx.ResolvePodAddr("10.0.0.5:8080"); !ok || got != "10.0.0.5:8080" {
		t.Errorf("identity resolve: got %q ok=%v", got, ok)
	}
	// 2. A connect to the pod IP on a non-target port (DNAT / service port) is
	//    folded to the single target port.
	if got, ok := idx.ResolvePodAddr("10.0.0.5:80"); !ok || got != "10.0.0.5:8080" {
		t.Errorf("port fixup resolve: got %q ok=%v", got, ok)
	}
	if idx.Len() != 1 {
		t.Errorf("Len=%d, want 1", idx.Len())
	}
}

func TestPodIndexPodForIP(t *testing.T) {
	idx := NewPodEndpointIndex()
	idx.Replace([]PodEndpointEntry{
		{IP: "10.244.2.10", Ports: []int32{6379}, Namespace: "prod", Pod: "server-a"},
		{IP: "10.244.2.111", Ports: []int32{6379}, Namespace: "prod", Pod: "server-b"},
		{IP: "10.244.2.200", Ports: []int32{5000}}, // no targetRef → not placeable
	})

	// A known pod IP maps to its identity, so a wildcard bind on that IP resolves
	// to the specific pod (not a shared per-port key).
	if ns, pod, ok := idx.PodForIP("10.244.2.10"); !ok || ns != "prod" || pod != "server-a" {
		t.Errorf("PodForIP(.10)=%q/%q ok=%v, want prod/server-a", ns, pod, ok)
	}
	if _, pod, ok := idx.PodForIP("10.244.2.111"); !ok || pod != "server-b" {
		t.Errorf("PodForIP(.111) pod=%q ok=%v, want server-b", pod, ok)
	}
	// An IP with no pod reference, and an unknown IP, are honest misses.
	if _, _, ok := idx.PodForIP("10.244.2.200"); ok {
		t.Error("endpoint without a targetRef must not be placeable")
	}
	if _, _, ok := idx.PodForIP("10.244.9.9"); ok {
		t.Error("unknown IP must miss")
	}

	// A subsequent Replace that drops server-a (pod deleted) evicts it: PodForIP
	// then misses, so a connect to its old IP can no longer attribute to it.
	idx.Replace([]PodEndpointEntry{
		{IP: "10.244.2.111", Ports: []int32{6379}, Namespace: "prod", Pod: "server-b"},
	})
	if _, _, ok := idx.PodForIP("10.244.2.10"); ok {
		t.Error("deleted pod IP must be dropped from the index")
	}
}

func TestPodIndexServiceVIPMisses(t *testing.T) {
	idx := NewPodEndpointIndex()
	idx.Replace([]PodEndpointEntry{{IP: "10.0.0.5", Ports: []int32{8080}}})

	// A Service ClusterIP is not a pod endpoint IP → honest miss, no guess.
	if got, ok := idx.ResolvePodAddr("10.96.0.10:80"); ok {
		t.Errorf("Service VIP must miss, got %q", got)
	}
	// Garbage input misses.
	if _, ok := idx.ResolvePodAddr("not-an-addr"); ok {
		t.Error("malformed addr must miss")
	}
}

func TestPodIndexMultiPortAmbiguous(t *testing.T) {
	idx := NewPodEndpointIndex()
	idx.Replace([]PodEndpointEntry{{IP: "10.0.0.6", Ports: []int32{8080, 9090}}})

	// A matching port resolves to itself.
	if got, ok := idx.ResolvePodAddr("10.0.0.6:9090"); !ok || got != "10.0.0.6:9090" {
		t.Errorf("matching port: got %q ok=%v", got, ok)
	}
	// A non-matching port on a multi-port IP is ambiguous → miss (no fabrication).
	if got, ok := idx.ResolvePodAddr("10.0.0.6:80"); ok {
		t.Errorf("ambiguous multi-port fixup must miss, got %q", got)
	}
}

func TestPodIndexReplaceDropsStale(t *testing.T) {
	idx := NewPodEndpointIndex()
	idx.Replace([]PodEndpointEntry{{IP: "10.0.0.5", Ports: []int32{8080}}})
	idx.Replace([]PodEndpointEntry{{IP: "10.0.0.9", Ports: []int32{7000}}})

	if _, ok := idx.ResolvePodAddr("10.0.0.5:8080"); ok {
		t.Error("stale endpoint should be dropped after Replace")
	}
	if _, ok := idx.ResolvePodAddr("10.0.0.9:7000"); !ok {
		t.Error("new endpoint should resolve after Replace")
	}
	if idx.Len() != 1 {
		t.Errorf("Len=%d, want 1", idx.Len())
	}
}

// A pod with no Service has no EndpointSlice, so slice-derived identity cannot
// name it. This is the case that produced zero attributed edges in a 600-connect
// campaign: every destination was a bare pod, so every wildcard bind stayed
// unattributable. The pod watch has to cover it, without displacing the
// slice-derived entries that a Service-backed pod still supplies.
func TestPodForIPResolvesServicelessPods(t *testing.T) {
	idx := NewPodEndpointIndex()

	idx.Replace([]PodEndpointEntry{
		{IP: "10.244.1.5", Ports: []int32{6379}, Namespace: "shop", Pod: "redis-svc-backed"},
	})
	idx.ReplacePodIdentities([]PodIdentity{
		{IP: "10.244.2.9", Namespace: "oracle", Pod: "redis-bare"},
	})

	if _, pod, ok := idx.PodForIP("10.244.2.9"); !ok || pod != "redis-bare" {
		t.Errorf("Service-less pod: got (%q, %v), want (redis-bare, true)", pod, ok)
	}
	if _, pod, ok := idx.PodForIP("10.244.1.5"); !ok || pod != "redis-svc-backed" {
		t.Errorf("slice-derived entry was displaced: got (%q, %v)", pod, ok)
	}
	if _, _, ok := idx.PodForIP("10.244.9.9"); ok {
		t.Error("unknown IP resolved; an unseen pod must stay an honest miss")
	}

	// Each source reconciles whole-state independently; neither may clear the
	// other. Refreshing the slice side must leave the pod-watch side intact.
	idx.Replace(nil)
	if _, pod, ok := idx.PodForIP("10.244.2.9"); !ok || pod != "redis-bare" {
		t.Errorf("slice reconcile wiped pod-watch identity: got (%q, %v)", pod, ok)
	}
	idx.ReplacePodIdentities(nil)
	if _, _, ok := idx.PodForIP("10.244.2.9"); ok {
		t.Error("pod removed from the cluster still resolves")
	}
}

// hostNetwork pods carry the NODE's IP, so indexing them would name every
// host-network flow on that node after whichever pod was seen last. Pending and
// terminated pods likewise must not claim an IP that may already be reassigned.
func TestApplyPodsSkipsHostNetworkAndNonRunning(t *testing.T) {
	idx := NewPodEndpointIndex()
	w := &ServiceWatcher{PodIndex: idx}
	w.applyPods(map[string]json.RawMessage{
		"a": json.RawMessage(`{"metadata":{"name":"host-agent","namespace":"kube-system"},
		                       "spec":{"hostNetwork":true},
		                       "status":{"podIP":"10.20.0.202","phase":"Running"}}`),
		"b": json.RawMessage(`{"metadata":{"name":"starting","namespace":"shop"},
		                       "status":{"podIP":"10.244.3.1","phase":"Pending"}}`),
		"c": json.RawMessage(`{"metadata":{"name":"live","namespace":"shop"},
		                       "status":{"podIP":"10.244.3.2","phase":"Running",
		                                 "podIPs":[{"ip":"10.244.3.2"},{"ip":"fd00::2"}]}}`),
	})
	for _, ip := range []string{"10.20.0.202", "10.244.3.1"} {
		if _, _, ok := idx.PodForIP(ip); ok {
			t.Errorf("%s should not be indexed", ip)
		}
	}
	for _, ip := range []string{"10.244.3.2", "fd00::2"} {
		if _, pod, ok := idx.PodForIP(ip); !ok || pod != "live" {
			t.Errorf("%s: got (%q, %v), want (live, true)", ip, pod, ok)
		}
	}
}
