// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package clusterpeers

import (
	"testing"
	"time"

	pb "github.com/boanlab/kloudlens/protobuf"
)

// fakeClock drives registry TTL deterministically.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestRegistry(t *testing.T, clk *fakeClock, resolver EndpointResolver) *Registry {
	t.Helper()
	r := NewRegistry(30*time.Second, resolver, nil)
	r.now = clk.now
	return r
}

// newTestRegistrySvc builds a registry with a Service-VIP resolver wired.
func newTestRegistrySvc(t *testing.T, clk *fakeClock, svc ServiceResolver) *Registry {
	t.Helper()
	r := NewRegistry(30*time.Second, nil, svc)
	r.now = clk.now
	return r
}

func lis(addr, node string) Listener {
	return Listener{Addr: addr, Port: portOf("", addr), PID: "42", Process: "svc",
		NodeName: node, Namespace: "prod", Pod: "app-0", Container: "app"}
}

func TestJoinExactCrossNode(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestRegistry(t, clk, nil)
	r.Observe(lis("10.0.0.5:8080", "node-b"))

	got, how, ok := r.Join("10.0.0.5:8080", "node-a")
	if !ok || how != HowDirect {
		t.Fatalf("exact join: ok=%v how=%v, want true/direct", ok, how)
	}
	if got.NodeName != "node-b" || got.Pod != "app-0" {
		t.Errorf("resolved listener wrong: %+v", got)
	}
	if how.Via() != "direct" {
		t.Errorf("via=%q, want direct", how.Via())
	}
}

func TestJoinTTLExpiry(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestRegistry(t, clk, nil)
	r.Observe(lis("10.0.0.5:8080", "node-b"))

	// Within TTL: hit.
	if _, _, ok := r.Join("10.0.0.5:8080", "node-a"); !ok {
		t.Fatal("expected hit within TTL")
	}
	// Past TTL: miss, and the entry is swept.
	clk.advance(31 * time.Second)
	if _, how, ok := r.Join("10.0.0.5:8080", "node-a"); ok || how != HowMiss {
		t.Fatalf("expected miss after TTL, got ok=%v how=%v", ok, how)
	}
	if e, _ := r.Stats(); e != 0 {
		t.Errorf("expired entry not swept: exact=%d", e)
	}
}

func TestJoinWildcard(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestRegistry(t, clk, nil)
	w := lis("0.0.0.0:9090", "node-c")
	w.Wildcard = true
	r.Observe(w)

	// A concrete pod IP on the wildcard port resolves via the port index.
	got, how, ok := r.Join("10.1.2.3:9090", "node-a")
	if !ok || how != HowWildcard {
		t.Fatalf("wildcard join: ok=%v how=%v, want true/wildcard", ok, how)
	}
	if got.NodeName != "node-c" {
		t.Errorf("wildcard listener wrong: %+v", got)
	}
	// Wildcard maps onto via="direct" (no EndpointSlice consulted).
	if how.Via() != "direct" {
		t.Errorf("via=%q, want direct", how.Via())
	}
	// A different port does not match the wildcard.
	if _, _, ok := r.Join("10.1.2.3:9091", "node-a"); ok {
		t.Error("wildcard should not match a different port")
	}
}

// lisPod is lis with a caller-chosen pod name (the wildcard index keys on it).
func lisPod(addr, node, pod string) Listener {
	l := lis(addr, node)
	l.Pod = pod
	return l
}

// podLocResolver is a resolver that also implements PodLocator, mapping a pod IP
// to its identity. Its ResolvePodAddr is inert (no DNAT fold); it exists so
// NewRegistry derives a PodLocator and the wildcard path can resolve per pod.
type podLocResolver struct{ ips map[string][2]string } // ip -> {namespace, pod}

func (p *podLocResolver) ResolvePodAddr(string) (string, bool) { return "", false }
func (p *podLocResolver) PodForIP(ip string) (string, string, bool) {
	v, ok := p.ips[ip]
	if !ok {
		return "", "", false
	}
	return v[0], v[1], true
}

// TestJoinWildcardBaseSinglePod is the GATE: the single-live-pod base case.
// One pod binds 0.0.0.0:6379 and is in the pod index;
// a connect to that pod's concrete IP:port MUST join and attribute that pod,
// cross-node, via the wildcard path. If this ever misses, cross-node attribution
// is blind for the common case, so it is guarded first.
func TestJoinWildcardBaseSinglePod(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{
		"10.244.2.113": {"prod", "server-single"},
	}}
	r := newTestRegistry(t, clk, loc)

	w := lisPod("0.0.0.0:6379", "node-x", "server-single")
	w.Wildcard = true
	r.Observe(w)

	got, how, ok := r.Join("10.244.2.113:6379", "node-a")
	if !ok || how != HowWildcard || got.Pod != "server-single" {
		t.Fatalf("base single-pod wildcard join: ok=%v how=%v pod=%q, want true/wildcard/server-single",
			ok, how, got.Pod)
	}
	if got.NodeName != "node-x" {
		t.Errorf("resolved listener wrong: %+v", got)
	}
	// A concrete pod-IP connect maps onto via="direct" on the edge.
	if how.Via() != "direct" {
		t.Errorf("via=%q, want direct", how.Via())
	}
}

// TestJoinWildcardIdentityMismatch locks the regression fix: the advertising
// agent's pod identity (namespace "production") and the pod index's targetRef
// identity (namespace "prod") disagree for the SAME physical pod. The connect IP
// still resolves to a confirmed-live pod, so the sole live wildcard listener on
// the port must still join rather than miss. Without this fallback the base
// cross-node join is lost whenever the two identity sources drift.
func TestJoinWildcardIdentityMismatch(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{
		"10.244.2.113": {"prod", "server-single"},
	}}
	r := newTestRegistry(t, clk, loc)

	w := lisPod("0.0.0.0:6379", "node-x", "server-single")
	w.Namespace = "production" // differs from the index's "prod" for the same pod
	w.Wildcard = true
	r.Observe(w)

	got, how, ok := r.Join("10.244.2.113:6379", "node-a")
	if !ok || how != HowWildcard || got.Pod != "server-single" {
		t.Fatalf("identity-mismatch wildcard join: ok=%v how=%v pod=%q, want true/wildcard/server-single",
			ok, how, got.Pod)
	}
}

// TestJoinWildcardChurnDistinctPods: two pods on one node both bind
// 0.0.0.0:6379 with different pod IPs. A connect to each pod's IP must resolve
// to that pod; the shared wildcard port must not collapse them onto one
// ":6379" key where the last writer wins.
func TestJoinWildcardChurnDistinctPods(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{
		"10.244.2.10":  {"prod", "server-a"},
		"10.244.2.111": {"prod", "server-b"},
	}}
	r := newTestRegistry(t, clk, loc)

	a := lisPod("0.0.0.0:6379", "node-x", "server-a")
	a.Wildcard = true
	b := lisPod("0.0.0.0:6379", "node-x", "server-b")
	b.Wildcard = true
	r.Observe(a)
	r.Observe(b) // both bind the same wildcard port; must NOT overwrite server-a

	// A connect to server-b's pod IP resolves to server-b.
	got, how, ok := r.Join("10.244.2.111:6379", "node-a")
	if !ok || how != HowWildcard || got.Pod != "server-b" {
		t.Fatalf("connect to .111 => ok=%v how=%v pod=%q, want true/wildcard/server-b", ok, how, got.Pod)
	}
	// A direct pod-IP connect still reports via="direct" on the edge.
	if how.Via() != "direct" {
		t.Errorf("via=%q, want direct", how.Via())
	}
	// A connect to server-a's pod IP resolves to server-a (no collision).
	got, _, ok = r.Join("10.244.2.10:6379", "node-a")
	if !ok || got.Pod != "server-a" {
		t.Fatalf("connect to .10 => ok=%v pod=%q, want true/server-a", ok, got.Pod)
	}
	// An IP the locator cannot place, on the same port, must MISS (never a
	// fall-through to some same-port pod).
	if _, _, ok := r.Join("10.244.2.222:6379", "node-a"); ok {
		t.Error("unknown pod IP on a wildcard port must miss, not attribute a same-port pod")
	}
	// Both pods are tracked as distinct live entries.
	if _, w := r.Stats(); w != 2 {
		t.Errorf("wild entries=%d, want 2", w)
	}
}

// TestJoinWildcardStaleEviction proves a deleted pod is never attributed: once
// the pod-index drops its IP (EndpointSlice removed it) OR its advertise TTL
// expires, a later connect to that IP misses.
func TestJoinWildcardStaleEviction(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{"10.244.2.10": {"prod", "server-a"}}}
	r := newTestRegistry(t, clk, loc)

	a := lisPod("0.0.0.0:6379", "node-x", "server-a")
	a.Wildcard = true
	r.Observe(a)
	if _, _, ok := r.Join("10.244.2.10:6379", "node-a"); !ok {
		t.Fatal("expected a hit while the pod is live")
	}

	// Case 1: the pod is deleted; the pod-index drops its IP BEFORE the advertise
	// TTL lapses. The connect can no longer be placed on server-a, and there is
	// no shared per-port key to wrongly attribute it to the still-registered
	// (but now stale) server-a entry → miss.
	delete(loc.ips, "10.244.2.10")
	if l, how, ok := r.Join("10.244.2.10:6379", "node-a"); ok {
		t.Fatalf("deleted pod (index dropped its IP) must not resolve, got pod=%q how=%v", l.Pod, how)
	}

	// Case 2: the IP reappears in the index but the advertise TTL has since
	// lapsed (agent stopped advertising the deleted pod) → still a miss, and the
	// stale entry is swept.
	loc.ips["10.244.2.10"] = [2]string{"prod", "server-a"}
	clk.advance(31 * time.Second)
	if _, _, ok := r.Join("10.244.2.10:6379", "node-a"); ok {
		t.Fatal("TTL-expired wildcard entry must not resolve")
	}
	if _, w := r.Stats(); w != 0 {
		t.Errorf("expired wildcard entry not swept: wild=%d", w)
	}
}

func TestJoinMiss(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	r := newTestRegistry(t, clk, nil)
	r.Observe(lis("10.0.0.5:8080", "node-b"))

	if _, how, ok := r.Join("10.9.9.9:1234", "node-a"); ok || how != HowMiss {
		t.Fatalf("expected miss for unknown addr, got ok=%v how=%v", ok, how)
	}
}

// stubResolver folds a fixed connect addr to a pod addr (models DNAT fixup).
type stubResolver struct{ from, to string }

func (s stubResolver) ResolvePodAddr(addr string) (string, bool) {
	if addr == s.from {
		return s.to, true
	}
	return "", false
}

func (s stubResolver) Len() int { return 1 }

func TestJoinEndpointsliceDNAT(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	// Connect hit an endpoint addr on the service port; the resolver folds it
	// to the pod's real bind addr, which the registry holds exactly.
	r := newTestRegistry(t, clk, stubResolver{from: "10.0.0.5:80", to: "10.0.0.5:8080"})
	r.Observe(lis("10.0.0.5:8080", "node-b"))

	got, how, ok := r.Join("10.0.0.5:80", "node-a")
	if !ok || how != HowEndpointslice {
		t.Fatalf("endpointslice join: ok=%v how=%v, want true/endpointslice", ok, how)
	}
	if got.NodeName != "node-b" {
		t.Errorf("resolved listener wrong: %+v", got)
	}
	if how.Via() != "endpointslice" {
		t.Errorf("via=%q, want endpointslice", how.Via())
	}
}

// stubSvcResolver models a Service-VIP → backend listener set resolution.
type stubSvcResolver struct {
	m map[string]svcHit
}

type svcHit struct {
	backends []string
	ns, name string
}

func (s stubSvcResolver) ResolveVIP(addr string) ([]string, string, string, bool) {
	h, ok := s.m[addr]
	if !ok {
		return nil, "", "", false
	}
	return h.backends, h.ns, h.name, true
}

func (s stubSvcResolver) NumServices() int { return len(s.m) }

func backendListener(addr, node, pod, process, image string) Listener {
	return Listener{Addr: addr, Port: portOf("", addr), PID: "42", Process: process,
		Image: image, NodeName: node, Namespace: "prod", Pod: pod, Container: "app"}
}

func TestJoinServiceVIPSingleBackend(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	svc := stubSvcResolver{m: map[string]svcHit{
		"10.96.0.10:80": {backends: []string{"10.0.0.5:8080"}, ns: "prod", name: "web"},
	}}
	r := newTestRegistrySvc(t, clk, svc)
	r.Observe(backendListener("10.0.0.5:8080", "node-b", "web-0", "nginx", "nginx:1"))

	got, how, ok := r.Join("10.96.0.10:80", "node-a")
	if !ok || how != HowServiceVIP {
		t.Fatalf("vip join: ok=%v how=%v, want true/service-vip", ok, how)
	}
	if how.Via() != "service-vip" {
		t.Errorf("via=%q, want service-vip", how.Via())
	}
	// Single backend → the exact remote process AND pod are resolved.
	if got.Process != "nginx" || got.Pod != "web-0" || got.NodeName != "node-b" || got.Image != "nginx:1" {
		t.Errorf("single-backend resolution wrong: %+v", got)
	}
	if got.Service.String() != "prod/web" {
		t.Errorf("peer service=%q, want prod/web", got.Service.String())
	}
	if got.ReplicaAmbiguous {
		t.Error("single backend must not be marked replica-ambiguous")
	}
}

func TestJoinServiceVIPMultiBackendAmbiguous(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	svc := stubSvcResolver{m: map[string]svcHit{
		"10.96.0.10:80": {backends: []string{"10.0.0.5:8080", "10.0.0.6:8080"}, ns: "prod", name: "web"},
	}}
	r := newTestRegistrySvc(t, clk, svc)
	// Two replicas of one workload: same process + image, distinct pods/nodes.
	r.Observe(backendListener("10.0.0.5:8080", "node-b", "web-0", "nginx", "nginx:1"))
	r.Observe(backendListener("10.0.0.6:8080", "node-c", "web-1", "nginx", "nginx:1"))

	got, how, ok := r.Join("10.96.0.10:80", "node-a")
	if !ok || how != HowServiceVIP {
		t.Fatalf("vip join: ok=%v how=%v, want true/service-vip", ok, how)
	}
	// The process + image (the workload identity) are resolved correctly...
	if got.Process != "nginx" || got.Image != "nginx:1" || got.Service.String() != "prod/web" {
		t.Errorf("multi-backend workload identity wrong: %+v", got)
	}
	// ...but the specific replica pod is honestly marked ambiguous.
	if !got.ReplicaAmbiguous || got.ReplicaCount != 2 {
		t.Errorf("replica ambiguity not marked: ambiguous=%v count=%d", got.ReplicaAmbiguous, got.ReplicaCount)
	}
}

func TestJoinServiceVIPUnknownClusterIPMiss(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	svc := stubSvcResolver{m: map[string]svcHit{
		"10.96.0.10:80": {backends: []string{"10.0.0.5:8080"}, ns: "prod", name: "web"},
	}}
	r := newTestRegistrySvc(t, clk, svc)
	r.Observe(backendListener("10.0.0.5:8080", "node-b", "web-0", "nginx", "nginx:1"))

	// A ClusterIP the Service watch never saw → honest miss, no fabricated peer.
	if _, how, ok := r.Join("10.96.0.99:80", "node-a"); ok || how != HowMiss {
		t.Fatalf("unknown ClusterIP must miss, got ok=%v how=%v", ok, how)
	}
}

func TestJoinServiceVIPDisagreeingBackendsMiss(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	svc := stubSvcResolver{m: map[string]svcHit{
		"10.96.0.10:80": {backends: []string{"10.0.0.5:8080", "10.0.0.6:8080"}, ns: "prod", name: "web"},
	}}
	r := newTestRegistrySvc(t, clk, svc)
	// Defensive case: backends disagree on process/image (should not happen for a
	// real Service). We must not guess across distinct workloads → miss.
	r.Observe(backendListener("10.0.0.5:8080", "node-b", "web-0", "nginx", "nginx:1"))
	r.Observe(backendListener("10.0.0.6:8080", "node-c", "redis-0", "redis", "redis:7"))

	if _, how, ok := r.Join("10.96.0.10:80", "node-a"); ok || how != HowMiss {
		t.Fatalf("disagreeing backends must miss, got ok=%v how=%v", ok, how)
	}
}

func TestJoinServiceVIPNoLiveBackendMiss(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	svc := stubSvcResolver{m: map[string]svcHit{
		"10.96.0.10:80": {backends: []string{"10.0.0.5:8080"}, ns: "prod", name: "web"},
	}}
	r := newTestRegistrySvc(t, clk, svc)
	// Service resolves, but no backend has advertised a live listener → miss.
	if _, how, ok := r.Join("10.96.0.10:80", "node-a"); ok || how != HowMiss {
		t.Fatalf("no live backend must miss, got ok=%v how=%v", ok, how)
	}
}

func TestPeerEdgeServiceVIPSameNode(t *testing.T) {
	src := &pb.IntentEvent{
		Kind:       KindNetworkExchange,
		Attributes: map[string]string{AttrPeer: "10.96.0.10:80"},
		Meta:       &pb.ContainerMeta{NodeName: "node-a", Pod: "client-0"},
	}
	// Single backend that happens to run on the connector's node → same-node.
	l := backendListener("10.0.0.5:8080", "node-a", "web-0", "nginx", "nginx:1")
	l.Service = ServiceRef{Namespace: "prod", Name: "web"}

	edge := PeerEdge(src, l, HowServiceVIP, "node-a")
	a := edge.GetAttributes()
	if a["attribution"] != "same-node" || a["via"] != "service-vip" || a["peer_service"] != "prod/web" {
		t.Errorf("same-node vip edge wrong: %+v", a)
	}
	if _, ok := a["peer_replica_ambiguous"]; ok {
		t.Errorf("single backend must not carry replica ambiguity: %+v", a)
	}
}

func TestPeerEdgeServiceVIPMultiBackend(t *testing.T) {
	src := &pb.IntentEvent{
		Kind:       KindNetworkExchange,
		Attributes: map[string]string{AttrPeer: "10.96.0.10:80"},
		Meta:       &pb.ContainerMeta{NodeName: "node-a", Pod: "client-0"},
	}
	l := backendListener("10.0.0.5:8080", "node-b", "web-0", "nginx", "nginx:1")
	l.Service = ServiceRef{Namespace: "prod", Name: "web"}
	l.ReplicaAmbiguous = true
	l.ReplicaCount = 3

	a := PeerEdge(src, l, HowServiceVIP, "node-a").GetAttributes()
	if a["attribution"] != "cross-node" || a["peer_process"] != "nginx" || a["peer_image"] != "nginx:1" {
		t.Errorf("multi-backend vip edge identity wrong: %+v", a)
	}
	if a["peer_replica_ambiguous"] != "true" || a["peer_replica_count"] != "3" {
		t.Errorf("replica ambiguity attributes wrong: %+v", a)
	}
	if a["peer_service"] != "prod/web" {
		t.Errorf("peer_service wrong: %+v", a)
	}
}

func TestListenerFromAdvertise(t *testing.T) {
	ev := &pb.IntentEvent{
		Kind: KindListenerAdvertise,
		Attributes: map[string]string{
			"addr": "10.0.0.5:8080", "port": "8080", "pid": "99",
			"process": "nginx", "wildcard": "false",
		},
		Meta: &pb.ContainerMeta{
			NodeName: "node-b", Namespace: "prod", Pod: "web-0",
			Container: "web", ContainerId: "abc", Image: "nginx:1",
		},
	}
	l, ok := ListenerFromAdvertise(ev)
	if !ok {
		t.Fatal("well-formed advertise should parse")
	}
	if l.Addr != "10.0.0.5:8080" || l.PID != "99" || l.Process != "nginx" ||
		l.NodeName != "node-b" || l.Pod != "web-0" || l.Image != "nginx:1" || l.Wildcard {
		t.Errorf("parsed listener wrong: %+v", l)
	}

	// Wrong kind and empty addr are rejected.
	if _, ok := ListenerFromAdvertise(&pb.IntentEvent{Kind: "NetworkExchange"}); ok {
		t.Error("non-advertise kind should be rejected")
	}
	if _, ok := ListenerFromAdvertise(&pb.IntentEvent{Kind: KindListenerAdvertise}); ok {
		t.Error("advertise with empty addr should be rejected")
	}
}

func TestPeerEdgeAttribution(t *testing.T) {
	src := &pb.IntentEvent{
		Kind:                 KindNetworkExchange,
		Attributes:           map[string]string{AttrPeer: "10.0.0.5:8080"},
		Meta:                 &pb.ContainerMeta{NodeName: "node-a", Pod: "client-0"},
		ContributingEventIds: []string{"ev-1", "ev-2"},
	}
	l := lis("10.0.0.5:8080", "node-b")

	edge := PeerEdge(src, l, HowDirect, "node-a")
	a := edge.GetAttributes()
	if edge.GetKind() != KindClusterPeerEdge {
		t.Fatalf("kind=%q, want ClusterPeerEdge", edge.GetKind())
	}
	if a["attribution"] != "cross-node" || a["via"] != "direct" ||
		a["peer_node"] != "node-b" || a["peer_pod"] != "app-0" || a["addr"] != "10.0.0.5:8080" {
		t.Errorf("edge attributes wrong: %+v", a)
	}
	if edge.GetMeta().GetPod() != "client-0" {
		t.Errorf("edge meta should be the connecting side, got %+v", edge.GetMeta())
	}
	if len(edge.GetContributingEventIds()) != 2 {
		t.Errorf("contributing ids not carried: %+v", edge.GetContributingEventIds())
	}

	// Same node → same-node attribution (redundant confirm).
	same := PeerEdge(src, lis("10.0.0.5:8080", "node-a"), HowDirect, "node-a")
	if same.GetAttributes()["attribution"] != "same-node" {
		t.Errorf("same-node attribution not set: %+v", same.GetAttributes())
	}
}

// TestJoinWildcardNeverAnswersForAnotherPod: with several pods bound to
// 0.0.0.0:80 across nodes, returning a port's "sole" live entry when the
// destination pod's own entry is absent attributes the flow to the wrong
// workload. A located pod whose entry is missing must be an honest miss.
func TestJoinWildcardNeverAnswersForAnotherPod(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{
		"10.244.0.168": {"oracle", "nginx-alpaca"}, // the connect destination
	}}
	r := newTestRegistry(t, clk, loc)

	// Only the OTHER node's pod advertised :80.
	other := lisPod("0.0.0.0:80", "camel", "nginx-camel")
	other.Namespace = "oracle"
	other.Wildcard = true
	r.Observe(other)

	got, how, ok := r.Join("10.244.0.168:80", "boar")
	if ok {
		t.Fatalf("resolved a flow to nginx-alpaca as %q/%q (how=%v); want an honest miss",
			got.Pod, got.NodeName, how)
	}
}
