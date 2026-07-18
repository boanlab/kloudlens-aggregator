// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package discovery

import (
	"net"
	"strconv"
	"sync"
)

// PodEndpointIndex maps EndpointSlice endpoint addresses to the backing pod
// "ip:port" a listener would have advertised, so a connect that hit a DNATed
// endpoint (kube-proxy rewrote the Service port to the pod's target port)
// resolves to the pod's real bind address.
//
// Honesty boundary (see CROSS_NODE_DESIGN.md): this index only knows endpoint
// (pod) IPs — the addresses EndpointSlice actually carries. It resolves:
//   - a connect already at a live endpoint "ip:port" (identity confirm), and
//   - a connect to a live endpoint IP on a non-target port, folded to the pod's
//     target port (the DNAT / service-port → target-port case).
//
// It does NOT know Service ClusterIPs (those live on the Service object, not in
// EndpointSlice), so a connect to a Service VIP is a MISS — never a fabricated
// peer. Full Service-VIP resolution would require also watching Service objects.
type PodEndpointIndex struct {
	mu sync.RWMutex
	// addrs is the set of live endpoint "ip:port" (identity-resolvable).
	addrs map[string]struct{}
	// ports maps an endpoint IP to its distinct target ports (for port fixup).
	ports map[string][]string
	// byIP maps a live endpoint (pod) IP to the pod's identity, so a connect to
	// a concrete pod IP can be attributed to the SPECIFIC advertising pod even
	// when that pod bound a wildcard (0.0.0.0) address. Only pods this index
	// actually sees are resolvable; an unknown IP is an honest miss.
	byIP map[string]podRef
}

// podRef is the identity of the pod backing an endpoint IP (from targetRef).
type podRef struct {
	namespace string
	pod       string
}

// NewPodEndpointIndex returns an empty, ready-to-use index.
func NewPodEndpointIndex() *PodEndpointIndex {
	return &PodEndpointIndex{
		addrs: make(map[string]struct{}),
		ports: make(map[string][]string),
		byIP:  make(map[string]podRef),
	}
}

// Replace atomically swaps the index contents for the endpoints in slices. It is
// the whole-state form used after a LIST/watch reconcile, so endpoints that
// disappeared are dropped. Each entry is (endpoint IP, its slice target ports).
func (idx *PodEndpointIndex) Replace(entries []PodEndpointEntry) {
	addrs := make(map[string]struct{})
	ports := make(map[string][]string)
	byIP := make(map[string]podRef)
	for _, e := range entries {
		for _, p := range e.Ports {
			ps := strconv.Itoa(int(p))
			addrs[e.IP+":"+ps] = struct{}{}
			if !contains(ports[e.IP], ps) {
				ports[e.IP] = append(ports[e.IP], ps)
			}
		}
		if e.Pod != "" {
			byIP[e.IP] = podRef{namespace: e.Namespace, pod: e.Pod}
		}
	}
	idx.mu.Lock()
	idx.addrs = addrs
	idx.ports = ports
	idx.byIP = byIP
	idx.mu.Unlock()
}

// PodEndpointEntry is one endpoint IP, the target ports it serves, and the
// identity of the pod backing it (from the EndpointSlice targetRef; empty when
// the slice carries no pod reference).
type PodEndpointEntry struct {
	IP        string
	Ports     []int32
	Namespace string
	Pod       string
}

// ResolvePodAddr maps a connect "ip:port" to the backing pod "ip:port".
// Resolution paths (all honest — an unknown IP is a miss, never a guess):
//  1. connect addr is itself a live endpoint "ip:port" → returned as-is.
//  2. connect IP is a live endpoint on a single target port → folded to that
//     port (the DNAT / service-port → target-port case).
//  3. otherwise (unknown IP, or an ambiguous multi-port IP with a non-matching
//     port) → ok=false.
func (idx *PodEndpointIndex) ResolvePodAddr(connectAddr string) (string, bool) {
	host, port, err := net.SplitHostPort(connectAddr)
	if err != nil {
		return "", false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if _, ok := idx.addrs[connectAddr]; ok {
		return connectAddr, true
	}
	ports := idx.ports[host]
	if len(ports) == 0 {
		return "", false
	}
	if contains(ports, port) {
		return connectAddr, true
	}
	if len(ports) == 1 {
		return host + ":" + ports[0], true
	}
	return "", false
}

// PodForIP maps a live pod IP to the identity (namespace, pod) of the pod that
// owns it. It lets the cluster listener registry attribute a connect to a
// concrete pod IP to the SPECIFIC advertising pod, even when that pod bound a
// wildcard (0.0.0.0) address that would otherwise collide with every other pod
// on the same port. An IP this index has not seen (or a slice with no pod
// targetRef) returns ok=false: an honest miss, never a guessed pod.
func (idx *PodEndpointIndex) PodForIP(ip string) (namespace, pod string, ok bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	r, found := idx.byIP[ip]
	if !found {
		return "", "", false
	}
	return r.namespace, r.pod, true
}

// Len reports the number of live endpoint "ip:port" entries. Sampled by the
// kloudlens_agg_endpointslice_entries gauge.
func (idx *PodEndpointIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.addrs)
}

func contains(s []string, v string) bool {
	for _, e := range s {
		if e == v {
			return true
		}
	}
	return false
}

