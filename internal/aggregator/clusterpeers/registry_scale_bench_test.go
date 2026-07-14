// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Scaling microbenchmarks for the cross-node peer-attribution join (M2).
//
// The aggregator receives a MERGED stream from every node, so what scales the
// join is not the physical node count but (a) the size of the cluster listener
// registry (pods x listeners across all nodes) and (b) the cross-node flow rate.
// These benchmarks drive Registry.Join directly against synthetic registries of
// increasing size, isolating the join's per-edge cost from any network or
// physical-node effect. Per-op time gives per-edge join latency; its reciprocal
// is the single-aggregator sustained throughput ceiling (Join is mutex-serialized).
//
// Run:
//   go test -run '^$' -bench 'BenchmarkJoin' -benchmem \
//     ./internal/aggregator/clusterpeers/ | tee registry_scale.txt
package clusterpeers

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

// registrySizes is the cluster listener-registry sizes we sweep. 100k already
// models a large cluster (e.g. 100 nodes x 100 pods x ~10 listeners); 1M is an
// out-of-range point to expose any super-linear growth in the map-backed join.
var registrySizes = []int{1_000, 10_000, 100_000, 1_000_000}

// buildExactRegistry populates a registry with n unique exact "ip:port"
// listeners spread across a /8 pod CIDR, TTL long enough that nothing expires
// mid-run. Returns the registry and the list of live keys to join against.
func buildExactRegistry(n int) (*Registry, []string) {
	r := NewRegistry(time.Hour, nil, nil)
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ip := fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff)
		port := strconv.Itoa(6000 + i%2000) // spread ports so keys stay unique
		addr := ip + ":" + port
		r.Observe(Listener{
			Addr: addr, Port: port, PID: strconv.Itoa(1000 + i),
			Process: "/usr/local/bin/redis-server", NodeName: "camel",
			Namespace: "xnode", Pod: fmt.Sprintf("redis-%d", i),
		})
		keys = append(keys, addr)
	}
	return r, keys
}

// BenchmarkJoinDirect measures per-edge latency of the common case: an exact
// registry hit (HowDirect), the resolution path a same-cluster cross-node connect
// to a directly-dialed pod IP takes.
func BenchmarkJoinDirect(b *testing.B) {
	for _, n := range registrySizes {
		r, keys := buildExactRegistry(n)
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, how, ok := r.Join(keys[i%len(keys)], "boar")
				if !ok || how != HowDirect {
					b.Fatalf("expected direct hit at N=%d", n)
				}
			}
		})
	}
}

// BenchmarkJoinMiss measures the WORST case: a destination that matches nothing,
// so the join runs all four resolution steps (exact, endpointslice, service-vip,
// wildcard) before returning HowMiss. Bounds the cost of an unresolvable flow.
func BenchmarkJoinMiss(b *testing.B) {
	for _, n := range registrySizes {
		r, _ := buildExactRegistry(n)
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, how, ok := r.Join("10.250.250.250:9999", "boar"); ok || how != HowMiss {
					b.Fatal("expected miss")
				}
			}
		})
	}
}

// synthResolver is a synthetic EndpointResolver: it DNATs a fixed VIP-endpoint
// pattern to a backing pod addr, else declines. Models the EndpointSlice fold.
type synthResolver struct{ backing string }

func (s synthResolver) ResolvePodAddr(addr string) (string, bool) {
	if addr == "10.99.0.1:80" {
		return s.backing, true
	}
	return "", false
}

// synthSvc is a synthetic ServiceResolver: it resolves one Service ClusterIP:port
// to a set of backend listener addrs. Models kube-proxy DNAT via the Service +
// EndpointSlice index.
type synthSvc struct {
	vip      string
	backends []string
}

func (s synthSvc) ResolveVIP(addr string) ([]string, string, string, bool) {
	if addr == s.vip {
		return s.backends, "xnode", "ra-svc", true
	}
	return nil, "", "", false
}

// BenchmarkJoinServiceVIP measures the Service-VIP path (HowServiceVIP): an exact
// miss, then a Service ClusterIP:port resolved through the Service resolver to a
// backing listener. This is the most work a resolvable flow does.
func BenchmarkJoinServiceVIP(b *testing.B) {
	for _, n := range registrySizes {
		backing := "10.244.2.196:6379"
		svc := synthSvc{vip: "10.100.156.50:6379", backends: []string{backing}}
		r := NewRegistry(time.Hour, nil, svc)
		// Fill with n unrelated exact listeners, plus the one real backend.
		_, keys := buildExactRegistry(n)
		for _, k := range keys {
			r.Observe(Listener{Addr: k, Port: "6379", Process: "/x"})
		}
		r.Observe(Listener{
			Addr: backing, Port: "6379", Process: "/usr/local/bin/redis-server",
			NodeName: "camel", Namespace: "xnode", Pod: "ra",
		})
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, how, ok := r.Join(svc.vip, "boar"); !ok || how != HowServiceVIP {
					b.Fatalf("expected service-vip hit at N=%d", n)
				}
			}
		})
	}
}

// BenchmarkJoinParallel measures sustained aggregate throughput under concurrent
// callers at a fixed large registry, exposing the single-mutex serialization of
// Join (the honest throughput ceiling of one aggregator instance).
func BenchmarkJoinParallel(b *testing.B) {
	r, keys := buildExactRegistry(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.Join(keys[i%len(keys)], "boar")
			i++
		}
	})
}
