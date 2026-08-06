// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Fleet-scale measurements for the aggregator's cross-node tier: pod churn,
// EndpointSlice update rate, and cross-node flow rate at hundreds-of-nodes
// magnitude.
//
// The aggregator consumes one merged stream, so it scales with the fleet's
// aggregate pod population and event rates rather than with node count. All
// four drivers run through the production code path: advertise envelopes
// folded by foldAdvertise, EndpointSlice reconciles applied to the pod index,
// and joins run by joinPeer.
//
// Run:
//
//	go test -run 'TestFleet' -v ./internal/aggregator/ | tee fleet_scale.txt
//	go test -run '^$' -bench 'BenchmarkFleet' -benchmem ./internal/aggregator/
package aggregator

import (
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/discovery"
)

// fleetShape is one cluster size, expressed the way an operator would state it.
type fleetShape struct {
	nodes       int
	podsPerNode int
}

func (f fleetShape) pods() int      { return f.nodes * f.podsPerNode }
func (f fleetShape) String() string { return fmt.Sprintf("%dnodes", f.nodes) }

// fleetShapes spans a small production cluster to one an order of magnitude
// larger.
var fleetShapes = []fleetShape{{100, 30}, {300, 30}, {1000, 30}}

// podAddr is slot i's pod IP, spread across a /8 pod CIDR.
func podAddr(i int) string {
	return fmt.Sprintf("10.%d.%d.%d:8080", (i>>16)&0xff, (i>>8)&0xff, i&0xff)
}

// buildFleet returns an aggregator wired with the production pod index, holding
// one advertised listener per pod, plus the endpoint entries the same fleet would
// produce. It is the aggregator's steady state for a cluster of this shape.
func buildFleet(t testing.TB, f fleetShape) (*Aggregator, *discovery.PodEndpointIndex, []discovery.PodEndpointEntry) {
	t.Helper()
	idx := discovery.NewPodEndpointIndex()
	a, err := New(Config{
		Agents:           []AgentEndpoint{{Name: "seed", Addr: "unused"}},
		Out:              io.Discard,
		EndpointResolver: idx,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	entries := make([]discovery.PodEndpointEntry, 0, f.pods())
	for i := 0; i < f.pods(); i++ {
		addr := podAddr(i)
		ip := addr[:len(addr)-len(":8080")]
		pod := fmt.Sprintf("app-%d", i)
		entries = append(entries, discovery.PodEndpointEntry{
			IP: ip, Ports: []int32{8080}, Namespace: "prod", Pod: pod,
		})
		a.foldAdvertise(advertiseEnv(addr, "8080", fmt.Sprintf("node-%d", i%f.nodes), pod, false))
	}
	idx.Replace(entries)
	return a, idx, entries
}

// TestFleetResidency reports the aggregator's cross-node state footprint at fleet
// scale: the listener registry plus the EndpointSlice index, which are the two
// structures that grow with the cluster.
func TestFleetResidency(t *testing.T) {
	for _, f := range fleetShapes {
		t.Run(f.String(), func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			a, idx, _ := buildFleet(t, f)

			runtime.GC()
			runtime.ReadMemStats(&after)
			held := float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20)
			size := a.registry.Size()
			t.Logf("%d nodes x %d pods = %d pods: registry=%d entries, endpoint index=%d, "+
				"cross-node state=%.1f MiB (%.0f B/pod)",
				f.nodes, f.podsPerNode, f.pods(), size, idx.Len(),
				held, held*(1<<20)/float64(f.pods()))
			if size != f.pods() {
				t.Errorf("registry holds %d entries, want %d", size, f.pods())
			}
			runtime.KeepAlive(a)
			runtime.KeepAlive(idx)
		})
	}
}

// TestFleetChurnThroughput drives the three fleet-scale rates at once against one
// aggregator: advertise refreshes, whole-state EndpointSlice reconciles, and
// cross-node joins. It reports what one aggregator sustains and what fraction of a
// core that costs, which is the answer to "does this hold at hundreds of nodes".
func TestFleetChurnThroughput(t *testing.T) {
	for _, f := range fleetShapes {
		t.Run(f.String(), func(t *testing.T) {
			a, idx, entries := buildFleet(t, f)
			rng := rand.New(rand.NewSource(7))
			pods := f.pods()

			// One simulated second of a churning cluster: every pod re-advertises
			// (agents refresh on a 10s cycle, so a tenth of the fleet per second),
			// the EndpointSlice index is reconciled twice (a busy cluster's watch
			// event rate), and 20k cross-node flows are joined.
			const (
				reconcilesPerSec = 2
				flowsPerSec      = 20_000
			)
			refreshPerSec := pods / 10

			start := time.Now()
			for i := 0; i < refreshPerSec; i++ {
				j := rng.Intn(pods)
				a.foldAdvertise(advertiseEnv(podAddr(j), "8080",
					fmt.Sprintf("node-%d", j%f.nodes), fmt.Sprintf("app-%d", j), false))
			}
			advertiseDur := time.Since(start)

			start = time.Now()
			for i := 0; i < reconcilesPerSec; i++ {
				idx.Replace(entries)
			}
			reconcileDur := time.Since(start)

			start = time.Now()
			hits := 0
			for i := 0; i < flowsPerSec; i++ {
				j := rng.Intn(pods)
				if a.joinPeer(netExchangeEnv(fmt.Sprint(i), podAddr(j), "node-client", nil)) != nil {
					hits++
				}
			}
			joinDur := time.Since(start)

			busy := advertiseDur + reconcileDur + joinDur
			t.Logf("%d pods: %d advertises in %v, %d endpoint reconciles in %v (%.1f ms each), "+
				"%d joins in %v (%d resolved)",
				pods, refreshPerSec, advertiseDur.Round(time.Microsecond),
				reconcilesPerSec, reconcileDur.Round(time.Microsecond),
				float64(reconcileDur.Microseconds())/float64(reconcilesPerSec)/1000,
				flowsPerSec, joinDur.Round(time.Microsecond), hits)
			t.Logf("%d pods: one simulated second of fleet load costs %v of one core (%.2f%%)",
				pods, busy.Round(time.Microsecond), 100*busy.Seconds())

			if hits != flowsPerSec {
				t.Errorf("resolved %d of %d flows", hits, flowsPerSec)
			}
		})
	}
}

// BenchmarkFleetEndpointReconcile isolates the whole-state EndpointSlice reconcile,
// the "frequent EndpointSlice updates" cost: each watch event rebuilds the index
// over the fleet's entire endpoint population, so this is O(pods) per update.
func BenchmarkFleetEndpointReconcile(b *testing.B) {
	for _, f := range fleetShapes {
		_, idx, entries := buildFleet(b, f)
		b.Run(f.String(), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.Replace(entries)
			}
		})
	}
}

// BenchmarkFleetJoinNoChurn is the control for BenchmarkFleetJoinUnderChurn: the
// same joins over the same fleet with nothing mutating underneath. The gap between
// the two is what churn actually costs the attribution path.
func BenchmarkFleetJoinNoChurn(b *testing.B) {
	f := fleetShape{300, 30}
	a, _, _ := buildFleet(b, f)
	pods := f.pods()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			a.joinPeer(netExchangeEnv("x", podAddr(i%pods), "node-client", nil))
			i++
		}
	})
}

// BenchmarkFleetJoinUnderChurn measures join cost while the fleet churns
// underneath: advertises and endpoint reconciles take the exclusive locks that the
// joins' shared lock must interleave with. If churn ever starved the join path,
// this is where it would show.
func BenchmarkFleetJoinUnderChurn(b *testing.B) {
	f := fleetShape{300, 30}
	a, idx, entries := buildFleet(b, f)
	pods := f.pods()

	stop := make(chan struct{})
	defer close(stop)
	go func() { // churner: continuous advertises and endpoint reconciles
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			j := i % pods
			a.foldAdvertise(advertiseEnv(podAddr(j), "8080",
				fmt.Sprintf("node-%d", j%f.nodes), fmt.Sprintf("app-%d", j), false))
			if i%1000 == 0 {
				idx.Replace(entries)
			}
			i++
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			a.joinPeer(netExchangeEnv("x", podAddr(i%pods), "node-client", nil))
			i++
		}
	})
}
