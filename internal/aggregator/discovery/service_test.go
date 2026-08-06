// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// rawState turns JSON object literals into the watcher's ns/name-keyed state map.
func rawState(t *testing.T, objs ...string) map[string]json.RawMessage {
	t.Helper()
	state := make(map[string]json.RawMessage)
	for i, o := range objs {
		raw := json.RawMessage(o)
		key, ok := keyOf(raw)
		if !ok {
			t.Fatalf("obj %d has no metadata name: %s", i, o)
		}
		state[key] = raw
	}
	return state
}

// buildIndex applies Service + EndpointSlice JSON literals into a fresh index.
func buildIndex(t *testing.T, services, slices []string) *ServiceIndex {
	t.Helper()
	idx := NewServiceIndex()
	w := &ServiceWatcher{Index: idx}
	w.applyServices(rawState(t, services...))
	w.applySlices(rawState(t, slices...))
	return idx
}

const webService = `{"metadata":{"namespace":"prod","name":"web"},
	"spec":{"clusterIP":"10.96.0.10","clusterIPs":["10.96.0.10"],
	"ports":[{"name":"http","port":80}]}}`

func TestServiceIndexPortToTargetPort(t *testing.T) {
	// Service exposes port 80 (named "http"); the EndpointSlice resolves that
	// name to the pod target port 8080. A connect to ClusterIP:80 must fold to
	// podIP:8080 (the kube-proxy DNAT the caller then attributes).
	slice := `{"metadata":{"namespace":"prod","name":"web-abc",
		"labels":{"kubernetes.io/service-name":"web"}},
		"ports":[{"name":"http","port":8080}],
		"endpoints":[{"addresses":["10.0.0.5"],"conditions":{"ready":true}}]}`
	idx := buildIndex(t, []string{webService}, []string{slice})

	backends, ns, name, ok := idx.ResolveVIP("10.96.0.10:80")
	if !ok {
		t.Fatal("expected VIP to resolve")
	}
	if ns != "prod" || name != "web" {
		t.Errorf("service ref wrong: %s/%s", ns, name)
	}
	if len(backends) != 1 || backends[0] != "10.0.0.5:8080" {
		t.Errorf("port→targetPort mapping wrong: %+v", backends)
	}
	if idx.NumServices() != 1 {
		t.Errorf("NumServices=%d, want 1", idx.NumServices())
	}
}

func TestServiceIndexMultiBackendReadyFilter(t *testing.T) {
	// Three replicas: two ready, one not. Backends are the two ready pods, sorted.
	slice := `{"metadata":{"namespace":"prod","name":"web-abc",
		"labels":{"kubernetes.io/service-name":"web"}},
		"ports":[{"name":"http","port":8080}],
		"endpoints":[
			{"addresses":["10.0.0.6"],"conditions":{"ready":true}},
			{"addresses":["10.0.0.5"],"conditions":{"ready":true}},
			{"addresses":["10.0.0.7"],"conditions":{"ready":false}}]}`
	idx := buildIndex(t, []string{webService}, []string{slice})

	backends, _, _, ok := idx.ResolveVIP("10.96.0.10:80")
	if !ok {
		t.Fatal("expected VIP to resolve")
	}
	if len(backends) != 2 || backends[0] != "10.0.0.5:8080" || backends[1] != "10.0.0.6:8080" {
		t.Errorf("ready backends (sorted) wrong: %+v", backends)
	}
}

func TestServiceIndexUnknownClusterIPMiss(t *testing.T) {
	slice := `{"metadata":{"namespace":"prod","name":"web-abc",
		"labels":{"kubernetes.io/service-name":"web"}},
		"ports":[{"name":"http","port":8080}],
		"endpoints":[{"addresses":["10.0.0.5"],"conditions":{"ready":true}}]}`
	idx := buildIndex(t, []string{webService}, []string{slice})

	// Unknown ClusterIP → miss.
	if _, _, _, ok := idx.ResolveVIP("10.96.9.9:80"); ok {
		t.Error("unknown ClusterIP must miss")
	}
	// Known ClusterIP but a port the Service does not expose → miss.
	if _, _, _, ok := idx.ResolveVIP("10.96.0.10:443"); ok {
		t.Error("unexposed port must miss")
	}
	// Malformed addr → miss.
	if _, _, _, ok := idx.ResolveVIP("not-an-addr"); ok {
		t.Error("malformed addr must miss")
	}
}

func TestServiceIndexNoBackendsMiss(t *testing.T) {
	// Service present, but no EndpointSlice → no backends → miss (never a guess).
	idx := buildIndex(t, []string{webService}, nil)
	if _, _, _, ok := idx.ResolveVIP("10.96.0.10:80"); ok {
		t.Error("service with no backends must miss")
	}
}

func TestServiceIndexHeadlessSkipped(t *testing.T) {
	// A headless Service (clusterIP "None") has no VIP to resolve.
	headless := `{"metadata":{"namespace":"prod","name":"db"},
		"spec":{"clusterIP":"None","clusterIPs":["None"],
		"ports":[{"name":"pg","port":5432}]}}`
	slice := `{"metadata":{"namespace":"prod","name":"db-1",
		"labels":{"kubernetes.io/service-name":"db"}},
		"ports":[{"name":"pg","port":5432}],
		"endpoints":[{"addresses":["10.0.0.9"],"conditions":{"ready":true}}]}`
	idx := buildIndex(t, []string{headless}, []string{slice})
	if idx.NumServices() != 0 {
		t.Errorf("headless service must not be indexed: NumServices=%d", idx.NumServices())
	}
}

func TestServiceIndexUnnamedPort(t *testing.T) {
	// A single unnamed Service port joins to the unnamed EndpointSlice port.
	svc := `{"metadata":{"namespace":"prod","name":"cache"},
		"spec":{"clusterIP":"10.96.0.20","ports":[{"port":6379}]}}`
	slice := `{"metadata":{"namespace":"prod","name":"cache-1",
		"labels":{"kubernetes.io/service-name":"cache"}},
		"ports":[{"port":6379}],
		"endpoints":[{"addresses":["10.0.0.11"],"conditions":{"ready":true}}]}`
	idx := buildIndex(t, []string{svc}, []string{slice})

	backends, _, _, ok := idx.ResolveVIP("10.96.0.20:6379")
	if !ok || len(backends) != 1 || backends[0] != "10.0.0.11:6379" {
		t.Errorf("unnamed-port resolve wrong: %+v ok=%v", backends, ok)
	}
}

// TestServiceWatcherListPopulatesIndex drives the ServiceWatcher's list path
// against a test apiserver serving one Service and its EndpointSlice, then
// confirms the index resolves the VIP.
func TestServiceWatcherListPopulatesIndex(t *testing.T) {
	svcItem := json.RawMessage(webService)
	sliceItem := json.RawMessage(`{"metadata":{"namespace":"prod","name":"web-abc",
		"labels":{"kubernetes.io/service-name":"web"}},
		"ports":[{"name":"http","port":8080}],
		"endpoints":[{"addresses":["10.0.0.5"],"conditions":{"ready":true}}]}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" {
			<-r.Context().Done() // block: the apply must come from the initial list
			return
		}
		var resp rawList
		resp.Metadata.ResourceVersion = "7"
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/v1/services"):
			resp.Items = []json.RawMessage{svcItem}
		case strings.Contains(r.URL.Path, "endpointslices"):
			resp.Items = []json.RawMessage{sliceItem}
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	tokFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokFile, []byte("fake-token"), 0o600); err != nil {
		t.Fatal(err)
	}

	idx := NewServiceIndex()
	w := &ServiceWatcher{APIServer: srv.URL, TokenFile: tokFile, Index: idx, HTTPClient: srv.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, _, ok := idx.ResolveVIP("10.96.0.10:80"); ok {
			return // populated
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("watcher did not populate the index within 2s")
}

// TestApplySlicesFeedsPodIndex pins Fix A: the cluster-wide Service +
// EndpointSlice watch must fold every ready endpoint's pod IP into the
// PodEndpointIndex, keyed to the pod identity from targetRef. This is what
// makes the direct (pod-IP) cross-node join resolvable — without it PodForIP
// never places a workload pod IP and every wildcard-bind join misses.
func TestApplySlicesFeedsPodIndex(t *testing.T) {
	// A redis Deployment's EndpointSlice: pod IP 10.244.2.106 backed by pod
	// redis-single-abc in ns xnode, serving :6379.
	slice := `{"metadata":{"namespace":"xnode","name":"redis-single-svc-x1",
		"labels":{"kubernetes.io/service-name":"redis-single-svc"}},
		"ports":[{"name":"","port":6379}],
		"endpoints":[{"addresses":["10.244.2.106"],"conditions":{"ready":true},
			"targetRef":{"kind":"Pod","namespace":"xnode","name":"redis-single-abc"}}]}`

	podIdx := NewPodEndpointIndex()
	w := &ServiceWatcher{Index: NewServiceIndex(), PodIndex: podIdx}
	w.applySlices(rawState(t, slice))

	// The concrete pod IP:port resolves to itself (it is a real endpoint)...
	if _, ok := podIdx.ResolvePodAddr("10.244.2.106:6379"); !ok {
		t.Fatal("pod IP:port should be a known endpoint after applySlices")
	}
	// ...and carries the pod identity from targetRef.
	ns, pod, ok := podIdx.PodForIP("10.244.2.106")
	if !ok {
		t.Fatal("PodForIP must place the workload pod IP the cluster watch saw")
	}
	if ns != "xnode" || pod != "redis-single-abc" {
		t.Fatalf("pod identity wrong: %s/%s, want xnode/redis-single-abc", ns, pod)
	}
}

// TestApplySlicesPodIndexDropsDeletedPod pins the churn/eviction guarantee:
// Replace is whole-state, so a pod whose EndpointSlice endpoint went away drops
// out of the index — a later connect to its stale IP then misses honestly
// instead of resolving to a same-port survivor.
func TestApplySlicesPodIndexDropsDeletedPod(t *testing.T) {
	twoReady := `{"metadata":{"namespace":"xnode","name":"redis-svc-x1",
		"labels":{"kubernetes.io/service-name":"redis-svc"}},
		"ports":[{"name":"","port":6379}],
		"endpoints":[
			{"addresses":["10.244.2.106"],"conditions":{"ready":true},
			 "targetRef":{"kind":"Pod","namespace":"xnode","name":"ra"}},
			{"addresses":["10.244.2.116"],"conditions":{"ready":true},
			 "targetRef":{"kind":"Pod","namespace":"xnode","name":"rb"}}]}`
	podIdx := NewPodEndpointIndex()
	w := &ServiceWatcher{Index: NewServiceIndex(), PodIndex: podIdx}
	w.applySlices(rawState(t, twoReady))

	if _, _, ok := podIdx.PodForIP("10.244.2.116"); !ok {
		t.Fatal("rb should be placeable while ready")
	}

	// rb deleted: its endpoint disappears from the slice state.
	onlyRA := `{"metadata":{"namespace":"xnode","name":"redis-svc-x1",
		"labels":{"kubernetes.io/service-name":"redis-svc"}},
		"ports":[{"name":"","port":6379}],
		"endpoints":[
			{"addresses":["10.244.2.106"],"conditions":{"ready":true},
			 "targetRef":{"kind":"Pod","namespace":"xnode","name":"ra"}}]}`
	w.applySlices(rawState(t, onlyRA))

	if _, _, ok := podIdx.PodForIP("10.244.2.116"); ok {
		t.Fatal("rb's stale IP must drop out of the index after it leaves the slice")
	}
	if ns, pod, ok := podIdx.PodForIP("10.244.2.106"); !ok || pod != "ra" || ns != "xnode" {
		t.Fatalf("ra must remain placeable: (%s/%s, %v)", ns, pod, ok)
	}
}
