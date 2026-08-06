// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Churn correctness for the cross-node join. A large cluster recycles pod IPs
// from its pod CIDR continuously, so an advertise can outlive the pod that sent
// it: the entry is still within its TTL while the IP already belongs to a
// different pod. Attributing that flow to the departed pod's process would be a
// silent misattribution, which is worse than a miss.
package clusterpeers

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestExactHitStalePodIdentity: an exact registry hit whose IP the pod index now
// places on a DIFFERENT pod must not be attributed to the pod that advertised it.
func TestExactHitStalePodIdentity(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{"10.0.0.5": {"prod", "redis-old"}}}
	r := newTestRegistry(t, clk, loc)
	r.Observe(Listener{Addr: "10.0.0.5:6379", Port: "6379", PID: "11",
		Process: "/usr/local/bin/redis-server", NodeName: "node-b",
		Namespace: "prod", Pod: "redis-old", Container: "redis"})

	// 10s into the 30s TTL the pod is gone and IPAM hands 10.0.0.5 to another pod.
	clk.advance(10 * time.Second)
	loc.ips["10.0.0.5"] = [2]string{"prod", "web-new"}

	l, how, ok := r.Join("10.0.0.5:6379", "node-a")
	if ok && l.Pod == "redis-old" {
		t.Fatalf("stale attribution: named %s/%s (%s) via %v after the IP moved",
			l.Namespace, l.Pod, l.Process, how)
	}
}

// TestExactHitUnknownIPStillResolves is the counterweight: the ownership check
// must reject only on a POSITIVE disagreement. A listener pod behind no Service
// appears in no EndpointSlice, so the index cannot place its IP at all, and that
// absence of evidence must NOT cost the attribution.
func TestExactHitUnknownIPStillResolves(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{"10.0.9.9": {"prod", "other"}}}
	r := newTestRegistry(t, clk, loc)
	r.Observe(lis("10.0.0.7:5432", "node-b")) // 10.0.0.7 is in no EndpointSlice

	l, how, ok := r.Join("10.0.0.7:5432", "node-a")
	if !ok || how != HowDirect {
		t.Fatalf("a pod outside any Service must still resolve: ok=%v how=%v", ok, how)
	}
	if l.Pod != "app-0" {
		t.Errorf("resolved the wrong pod: %s", l.Pod)
	}
}

// TestReusedIPResolvesToNewOwner: once the pod that now holds the IP advertises
// its own listener, the flow attributes to it, not to the departed pod.
func TestReusedIPResolvesToNewOwner(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	loc := &podLocResolver{ips: map[string][2]string{"10.0.0.5": {"prod", "redis-old"}}}
	r := newTestRegistry(t, clk, loc)
	r.Observe(Listener{Addr: "10.0.0.5:6379", Port: "6379", PID: "11",
		Process: "/usr/local/bin/redis-server", NodeName: "node-b",
		Namespace: "prod", Pod: "redis-old"})

	clk.advance(10 * time.Second)
	loc.ips["10.0.0.5"] = [2]string{"prod", "web-new"}
	r.Observe(Listener{Addr: "10.0.0.5:6379", Port: "6379", PID: "77",
		Process: "/usr/sbin/nginx", NodeName: "node-c",
		Namespace: "prod", Pod: "web-new"})

	l, _, ok := r.Join("10.0.0.5:6379", "node-a")
	if !ok {
		t.Fatal("the new owner's own advertise must resolve")
	}
	if l.Pod != "web-new" || l.Process != "/usr/sbin/nginx" {
		t.Errorf("resolved %s/%s, want prod/web-new /usr/sbin/nginx", l.Pod, l.Process)
	}
}

// naiveJoinExact is the join WITHOUT the ownership cross-check: a live exact hit
// is returned on address alone. It reproduces the pre-fix behaviour so the churn
// experiment can report both misattribution rates from one run, with no toggle in
// production code.
func (r *Registry) naiveJoinExact(addr string, now time.Time) (Listener, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l, ok := r.exact[addr]
	if !ok || !l.expires.After(now) {
		return Listener{}, false
	}
	return l, true
}

// TestFleetChurnMisattribution is the cluster-scale churn experiment (the
// aggregator sees one merged stream, so a fleet is reproduced as its aggregate
// pod population rather than as physical nodes). A 9,000-pod fleet recycles pod
// IPs continuously while flows are joined against it; departed pods' advertises
// stay within TTL, which is exactly when an address-only join names a dead
// process. The checked join must misattribute zero flows.
func TestFleetChurnMisattribution(t *testing.T) {
	const (
		nodes        = 300
		podsPerNode  = 30
		fleet        = nodes * podsPerNode // 9,000 pods
		ticks        = 60                  // simulated seconds
		churnPerTick = 30                  // pods replaced per second (IP recycled)
		joinsPerTick = 2000
		refreshEvery = 10 // advertises re-sent every 10s, TTL is 30s
	)

	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	rng := rand.New(rand.NewSource(42)) // fixed seed: the run is reproducible
	loc := &podLocResolver{ips: make(map[string][2]string, fleet)}
	r := newTestRegistry(t, clk, loc)

	// addrs[i] is pod slot i's "ip:port"; owner[i] is the pod name currently
	// holding that IP. A slot's IP is stable (the CIDR is finite and recycled);
	// the pod occupying it changes, which is the case under test.
	addrs := make([]string, fleet)
	owner := make([]string, fleet)
	generation := 0
	advertise := func(i int) {
		r.Observe(Listener{Addr: addrs[i], Port: "8080", PID: fmt.Sprint(1000 + i),
			Process: "/usr/local/bin/app", NodeName: fmt.Sprintf("node-%d", i%nodes),
			Namespace: "prod", Pod: owner[i]})
	}
	for i := 0; i < fleet; i++ {
		ip := fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff)
		addrs[i] = ip + ":8080"
		owner[i] = fmt.Sprintf("app-%d-g0", i)
		loc.ips[ip] = [2]string{"prod", owner[i]}
		advertise(i)
	}

	var joins, misses, staleChecked, staleNaive int
	next := 0
	for tick := 1; tick <= ticks; tick++ {
		clk.advance(time.Second)

		// Churn: the pod in each chosen slot is replaced, and IPAM hands its IP
		// straight to the successor. The index learns the new owner immediately
		// (an EndpointSlice update); the departed pod's advertise lingers until
		// its TTL lapses, which is the window this experiment probes.
		generation++
		for c := 0; c < churnPerTick; c++ {
			i := (next + c) % fleet
			owner[i] = fmt.Sprintf("app-%d-g%d", i, generation)
			ip := addrs[i][:len(addrs[i])-len(":8080")]
			loc.ips[ip] = [2]string{"prod", owner[i]}
		}
		next = (next + churnPerTick) % fleet

		// Flows are spread uniformly over the fleet, NOT aimed at the pods that
		// just churned: the rate below is then the rate a real cluster would see,
		// set by how much of the fleet sits in a stale window at any moment, not
		// by how hard the experiment aims at the window.
		for j := 0; j < joinsPerTick; j++ {
			i := rng.Intn(fleet)
			now := clk.now()

			if l, ok := r.naiveJoinExact(addrs[i], now); ok && l.Pod != owner[i] {
				staleNaive++
			}
			l, _, ok := r.Join(addrs[i], "node-client")
			if !ok {
				misses++
				continue
			}
			joins++
			if l.Pod != owner[i] {
				staleChecked++
			}
		}

		// Surviving pods refresh their advertises on schedule.
		if tick%refreshEvery == 0 {
			for i := 0; i < fleet; i++ {
				advertise(i)
			}
		}
	}

	total := joins + misses
	t.Logf("fleet=%d pods, %d ticks, churn=%d pods/tick, %d flows joined",
		fleet, ticks, churnPerTick, total)
	t.Logf("resolved=%d misses=%d (%.2f%%)", joins, misses, 100*float64(misses)/float64(total))
	t.Logf("MISATTRIBUTED to a departed pod: address-only join=%d (%.2f%% of flows), "+
		"ownership-checked join=%d", staleNaive, 100*float64(staleNaive)/float64(total), staleChecked)

	if staleNaive == 0 {
		t.Fatal("experiment is not exercising the reuse window: address-only join saw no stale hit")
	}
	if staleChecked != 0 {
		t.Errorf("checked join misattributed %d flows to departed pods, want 0", staleChecked)
	}
}
