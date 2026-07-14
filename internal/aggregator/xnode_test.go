// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/clusterpeers"
	pb "github.com/boanlab/kloudlens/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func advertiseEnv(addr, port, node, pod string, wildcard bool) *pb.EventEnvelope {
	wc := "false"
	if wildcard {
		wc = "true"
	}
	return &pb.EventEnvelope{Payload: &pb.EventEnvelope_Intent{Intent: &pb.IntentEvent{
		Kind: clusterpeers.KindListenerAdvertise,
		Attributes: map[string]string{
			"addr": addr, "port": port, "pid": "42", "process": "svc", "wildcard": wc,
		},
		Meta: &pb.ContainerMeta{NodeName: node, Namespace: "prod", Pod: pod, Container: "app"},
	}}}
}

func netExchangeEnv(id, peer, node string, attrs map[string]string) *pb.EventEnvelope {
	a := map[string]string{clusterpeers.AttrPeer: peer}
	for k, v := range attrs {
		a[k] = v
	}
	return &pb.EventEnvelope{Payload: &pb.EventEnvelope_Intent{Intent: &pb.IntentEvent{
		IntentId: id, Kind: clusterpeers.KindNetworkExchange, Attributes: a,
		Meta: &pb.ContainerMeta{NodeName: node, Namespace: "prod", Pod: "client-0"},
	}}}
}

// ndjsonEdges parses the NDJSON output and returns every ClusterPeerEdge's
// attribute map, in order.
func ndjsonEdges(t *testing.T, raw []byte) []map[string]string {
	t.Helper()
	type line struct {
		Envelope struct {
			Intent struct {
				Kind       string            `json:"kind"`
				Attributes map[string]string `json:"attributes"`
			} `json:"intent"`
		} `json:"envelope"`
	}
	var edges []map[string]string
	dec := json.NewDecoder(bytes.NewReader(raw))
	for dec.More() {
		var l line
		if err := dec.Decode(&l); err != nil {
			t.Fatalf("decode ndjson: %v\n%s", err, raw)
		}
		if l.Envelope.Intent.Kind == clusterpeers.KindClusterPeerEdge {
			edges = append(edges, l.Envelope.Intent.Attributes)
		}
	}
	return edges
}

func runXNode(t *testing.T, resolver clusterpeers.EndpointResolver, feed func(*fakeAgent), wantWritten uint64) (*Aggregator, []byte) {
	t.Helper()
	return runXNodeFull(t, resolver, nil, feed, wantWritten)
}

func runXNodeFull(t *testing.T, resolver clusterpeers.EndpointResolver, svc clusterpeers.ServiceResolver, feed func(*fakeAgent), wantWritten uint64) (*Aggregator, []byte) {
	t.Helper()
	a1 := newFakeAgent(t, "agent-1")
	t.Cleanup(a1.stop)

	var buf safeBuf
	agg, err := New(Config{
		Agents: []AgentEndpoint{{
			Name: "agent-1", Addr: "passthrough:bufconn",
			DialOpts: []grpc.DialOption{
				grpc.WithContextDialer(a1.dialer),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		}},
		Out:              &buf,
		Streams:          []string{"intent"},
		EndpointResolver: resolver,
		ServiceResolver:  svc,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = agg.Run(ctx); close(done) }()

	time.Sleep(150 * time.Millisecond)
	feed(a1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && agg.Stats().Written < wantWritten {
		time.Sleep(20 * time.Millisecond)
	}
	// Small settle so a not-expected extra write would surface.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done
	return agg, buf.Bytes()
}

func TestXNodeExactCrossNodeEdge(t *testing.T) {
	// Advertise a listener on node-b, then a connect from node-a to it.
	agg, out := runXNode(t, nil, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-b", "web-0", false)
		a.svc.events <- netExchangeEnv("nx-1", "10.0.0.5:8080", "node-a", nil)
	}, 2) // NetworkExchange + ClusterPeerEdge (advertise is consumed, not written)

	if j := agg.Stats().XNodeJoins; j != 1 {
		t.Fatalf("XNodeJoins=%d, want 1 (stats=%+v)", j, agg.Stats())
	}
	edges := ndjsonEdges(t, out)
	if len(edges) != 1 {
		t.Fatalf("got %d edges, want 1\n%s", len(edges), out)
	}
	e := edges[0]
	if e["attribution"] != "cross-node" || e["via"] != "direct" ||
		e["peer_node"] != "node-b" || e["peer_pod"] != "web-0" || e["addr"] != "10.0.0.5:8080" {
		t.Errorf("edge attributes wrong: %+v", e)
	}
	if agg.Stats().ListenerRegistrySize != 1 {
		t.Errorf("registry size=%d, want 1", agg.Stats().ListenerRegistrySize)
	}
}

func TestXNodeSameNodeMarked(t *testing.T) {
	agg, out := runXNode(t, nil, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-a", "web-0", false)
		a.svc.events <- netExchangeEnv("nx-1", "10.0.0.5:8080", "node-a", nil)
	}, 2)

	if agg.Stats().XNodeJoins != 1 {
		t.Fatalf("XNodeJoins=%d, want 1", agg.Stats().XNodeJoins)
	}
	edges := ndjsonEdges(t, out)
	if len(edges) != 1 || edges[0]["attribution"] != "same-node" {
		t.Fatalf("expected one same-node edge, got %+v\n%s", edges, out)
	}
}

func TestXNodeMissNoEdge(t *testing.T) {
	agg, out := runXNode(t, nil, func(a *fakeAgent) {
		// No advertise for this peer → miss.
		a.svc.events <- netExchangeEnv("nx-1", "10.9.9.9:1234", "node-a", nil)
	}, 1) // only the NetworkExchange is written

	if m := agg.Stats().XNodeMisses; m != 1 {
		t.Fatalf("XNodeMisses=%d, want 1", m)
	}
	if agg.Stats().XNodeJoins != 0 {
		t.Errorf("XNodeJoins=%d, want 0", agg.Stats().XNodeJoins)
	}
	if edges := ndjsonEdges(t, out); len(edges) != 0 {
		t.Errorf("miss must not fabricate an edge: %+v", edges)
	}
}

func TestXNodeSkipsKernelAttributed(t *testing.T) {
	// peer_pid present → the kernel already attributed same-node; no join runs.
	agg, out := runXNode(t, nil, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-b", "web-0", false)
		a.svc.events <- netExchangeEnv("nx-1", "10.0.0.5:8080", "node-a",
			map[string]string{clusterpeers.AttrPeerPID: "777"})
	}, 1) // only the NetworkExchange is written; no edge, no miss

	if agg.Stats().XNodeJoins != 0 || agg.Stats().XNodeMisses != 0 {
		t.Fatalf("kernel-attributed exchange must not join or miss (stats=%+v)", agg.Stats())
	}
	if edges := ndjsonEdges(t, out); len(edges) != 0 {
		t.Errorf("no edge expected for kernel-attributed exchange: %+v", edges)
	}
}

// stubIdxResolver models the EndpointSlice DNAT fold for the aggregator test.
type stubIdxResolver struct{}

func (stubIdxResolver) ResolvePodAddr(addr string) (string, bool) {
	if addr == "10.0.0.5:80" {
		return "10.0.0.5:8080", true
	}
	return "", false
}
func (stubIdxResolver) Len() int { return 3 }

// stubSvcResolver models Service-VIP → backend listener resolution end-to-end.
type stubSvcResolver struct {
	backends []string
	ns, name string
}

func (s stubSvcResolver) ResolveVIP(addr string) ([]string, string, string, bool) {
	if addr == "10.96.0.10:80" {
		return s.backends, s.ns, s.name, true
	}
	return nil, "", "", false
}
func (s stubSvcResolver) NumServices() int { return 5 }

func TestXNodeServiceVIPSingleBackendEdge(t *testing.T) {
	svc := stubSvcResolver{backends: []string{"10.0.0.5:8080"}, ns: "prod", name: "web"}
	agg, out := runXNodeFull(t, nil, svc, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-b", "web-0", false)
		// Connect targets the Service ClusterIP:port (VIP), not a pod IP.
		a.svc.events <- netExchangeEnv("nx-1", "10.96.0.10:80", "node-a", nil)
	}, 2)

	edges := ndjsonEdges(t, out)
	if len(edges) != 1 {
		t.Fatalf("got %d edges, want 1\n%s", len(edges), out)
	}
	e := edges[0]
	if e["via"] != "service-vip" || e["attribution"] != "cross-node" ||
		e["peer_pod"] != "web-0" || e["peer_service"] != "prod/web" {
		t.Errorf("vip edge attributes wrong: %+v", e)
	}
	if _, ambiguous := e["peer_replica_ambiguous"]; ambiguous {
		t.Errorf("single backend must not be ambiguous: %+v", e)
	}
	if agg.Stats().VIPJoins != 1 || agg.Stats().XNodeJoins != 1 {
		t.Errorf("VIP metrics wrong: %+v", agg.Stats())
	}
	if agg.Stats().ServicesWatched != 5 {
		t.Errorf("ServicesWatched=%d, want 5", agg.Stats().ServicesWatched)
	}
}

func TestXNodeServiceVIPMultiBackendAmbiguousEdge(t *testing.T) {
	svc := stubSvcResolver{backends: []string{"10.0.0.5:8080", "10.0.0.6:8080"}, ns: "prod", name: "web"}
	agg, out := runXNodeFull(t, nil, svc, func(a *fakeAgent) {
		// Two replicas of one workload: same process "svc" (from advertiseEnv).
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-b", "web-0", false)
		a.svc.events <- advertiseEnv("10.0.0.6:8080", "8080", "node-c", "web-1", false)
		a.svc.events <- netExchangeEnv("nx-1", "10.96.0.10:80", "node-a", nil)
	}, 3) // 2 advertises consumed; NetworkExchange + edge written

	edges := ndjsonEdges(t, out)
	if len(edges) != 1 {
		t.Fatalf("got %d edges, want 1\n%s", len(edges), out)
	}
	e := edges[0]
	if e["via"] != "service-vip" || e["peer_process"] != "svc" || e["peer_service"] != "prod/web" {
		t.Errorf("multi-backend vip edge wrong: %+v", e)
	}
	if e["peer_replica_ambiguous"] != "true" || e["peer_replica_count"] != "2" {
		t.Errorf("replica ambiguity not marked N=2: %+v", e)
	}
	if agg.Stats().VIPJoins != 1 {
		t.Errorf("VIPJoins=%d, want 1", agg.Stats().VIPJoins)
	}
}

func TestXNodeServiceVIPSameNode(t *testing.T) {
	// The single backend runs on the connector's node → same-node attribution.
	svc := stubSvcResolver{backends: []string{"10.0.0.5:8080"}, ns: "prod", name: "web"}
	_, out := runXNodeFull(t, nil, svc, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-a", "web-0", false)
		a.svc.events <- netExchangeEnv("nx-1", "10.96.0.10:80", "node-a", nil)
	}, 2)

	edges := ndjsonEdges(t, out)
	if len(edges) != 1 || edges[0]["attribution"] != "same-node" || edges[0]["via"] != "service-vip" {
		t.Fatalf("expected one same-node service-vip edge, got %+v\n%s", edges, out)
	}
}

func TestXNodeServiceVIPUnknownMiss(t *testing.T) {
	svc := stubSvcResolver{backends: []string{"10.0.0.5:8080"}, ns: "prod", name: "web"}
	agg, out := runXNodeFull(t, nil, svc, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-b", "web-0", false)
		// A ClusterIP the Service watch never saw → miss, no fabricated edge.
		a.svc.events <- netExchangeEnv("nx-1", "10.96.9.9:80", "node-a", nil)
	}, 1)

	if agg.Stats().XNodeMisses != 1 || agg.Stats().VIPJoins != 0 {
		t.Fatalf("unknown VIP must miss, not join (stats=%+v)", agg.Stats())
	}
	if edges := ndjsonEdges(t, out); len(edges) != 0 {
		t.Errorf("unknown VIP must not fabricate an edge: %+v", edges)
	}
}

// stubChurnResolver implements EndpointResolver + PodLocator: it maps a connect
// pod IP to its owning pod so the aggregator can tell apart pods that all bind
// the same wildcard port.
type stubChurnResolver struct{ ips map[string][2]string }

func (s stubChurnResolver) ResolvePodAddr(string) (string, bool) { return "", false }
func (s stubChurnResolver) PodForIP(ip string) (string, string, bool) {
	v, ok := s.ips[ip]
	if !ok {
		return "", "", false
	}
	return v[0], v[1], true
}
func (s stubChurnResolver) Len() int { return len(s.ips) }

// TestXNodeWildcardChurnEdges is the end-to-end crux: two pods on one node both
// advertise a 0.0.0.0:6379 (wildcard) bind; connects to each pod's distinct IP
// must produce a ClusterPeerEdge attributing the CORRECT pod, with no collision.
func TestXNodeWildcardChurnEdges(t *testing.T) {
	res := stubChurnResolver{ips: map[string][2]string{
		"10.244.2.10":  {"prod", "server-a"},
		"10.244.2.111": {"prod", "server-b"},
	}}
	agg, out := runXNode(t, res, func(a *fakeAgent) {
		// Both server pods bind the wildcard address on the same node.
		a.svc.events <- advertiseEnv("0.0.0.0:6379", "6379", "node-x", "server-a", true)
		a.svc.events <- advertiseEnv("0.0.0.0:6379", "6379", "node-x", "server-b", true)
		// Connects target the concrete pod IPs directly.
		a.svc.events <- netExchangeEnv("nx-a", "10.244.2.10:6379", "node-a", nil)
		a.svc.events <- netExchangeEnv("nx-b", "10.244.2.111:6379", "node-a", nil)
	}, 4) // 2 NetworkExchange + 2 ClusterPeerEdge written (advertises consumed)

	edges := ndjsonEdges(t, out)
	if len(edges) != 2 {
		t.Fatalf("got %d edges, want 2\n%s", len(edges), out)
	}
	byAddr := map[string]map[string]string{}
	for _, e := range edges {
		byAddr[e["addr"]] = e
	}
	if e := byAddr["10.244.2.10:6379"]; e == nil || e["peer_pod"] != "server-a" {
		t.Errorf("connect to .10 attributed wrong: %+v", e)
	}
	if e := byAddr["10.244.2.111:6379"]; e == nil || e["peer_pod"] != "server-b" {
		t.Errorf("connect to .111 attributed wrong: %+v", e)
	}
	// Both are cross-node, direct pod-IP connects.
	for _, e := range edges {
		if e["attribution"] != "cross-node" || e["via"] != "direct" || e["peer_node"] != "node-x" {
			t.Errorf("edge attributes wrong: %+v", e)
		}
	}
	if agg.Stats().XNodeJoins != 2 {
		t.Errorf("XNodeJoins=%d, want 2", agg.Stats().XNodeJoins)
	}
}

// TestXNodeWildcardBaseSinglePodEdge is the end-to-end GATE for the regressed
// base case: a single pod binds 0.0.0.0:6379 and is in the pod index; a connect
// to its concrete pod IP must produce ONE cross-node ClusterPeerEdge attributing
// that pod (this is the case that produced xnode_joins_total=0 / all-miss when it
// regressed).
func TestXNodeWildcardBaseSinglePodEdge(t *testing.T) {
	res := stubChurnResolver{ips: map[string][2]string{
		"10.244.2.113": {"prod", "server-single"},
	}}
	agg, out := runXNode(t, res, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("0.0.0.0:6379", "6379", "node-x", "server-single", true)
		a.svc.events <- netExchangeEnv("nx-1", "10.244.2.113:6379", "node-a", nil)
	}, 2) // NetworkExchange + ClusterPeerEdge

	if agg.Stats().XNodeJoins != 1 {
		t.Fatalf("XNodeJoins=%d, want 1 (base wildcard join regressed) stats=%+v", agg.Stats().XNodeJoins, agg.Stats())
	}
	edges := ndjsonEdges(t, out)
	if len(edges) != 1 {
		t.Fatalf("got %d edges, want 1\n%s", len(edges), out)
	}
	e := edges[0]
	if e["attribution"] != "cross-node" || e["via"] != "direct" ||
		e["peer_node"] != "node-x" || e["peer_pod"] != "server-single" || e["addr"] != "10.244.2.113:6379" {
		t.Errorf("base wildcard edge attributes wrong: %+v", e)
	}
}

func TestXNodeEndpointsliceDNATEdge(t *testing.T) {
	agg, out := runXNode(t, stubIdxResolver{}, func(a *fakeAgent) {
		a.svc.events <- advertiseEnv("10.0.0.5:8080", "8080", "node-b", "web-0", false)
		// Connect targets the endpoint on the service port (80); resolver folds
		// it to the pod bind port (8080).
		a.svc.events <- netExchangeEnv("nx-1", "10.0.0.5:80", "node-a", nil)
	}, 2)

	edges := ndjsonEdges(t, out)
	if len(edges) != 1 || edges[0]["via"] != "endpointslice" {
		t.Fatalf("expected one endpointslice edge, got %+v\n%s", edges, out)
	}
	if agg.Stats().EndpointSliceEntries != 3 {
		t.Errorf("EndpointSliceEntries=%d, want 3", agg.Stats().EndpointSliceEntries)
	}
}
