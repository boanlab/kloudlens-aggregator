// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Package clusterpeers maintains the aggregator's cluster-wide listener
// registry and performs cross-node peer attribution joins.
//
// The kloudLens agent attributes same-node peers in-kernel (kl_ipc_listener).
// The aggregator sees every node's listeners and every node's connects, so it
// can promote attribution to cluster-wide cross-node process-to-process.
// Agents periodically emit ListenerAdvertise IntentEvents; the aggregator folds
// them into a TTL registry (default 30s). On a NetworkExchange envelope whose
// peer was NOT already same-node-attributed, the aggregator resolves the connect
// destination against the registry (with optional EndpointSlice DNAT fixup) and,
// on a hit, emits a ClusterPeerEdge IntentEvent.
package clusterpeers

import (
	"context"
	"net"
	"sync"
	"time"
)

// DefaultTTL is the lifetime of a registry entry before it must be refreshed by
// a fresh ListenerAdvertise. Matches the wire contract's 30s advertise TTL.
const DefaultTTL = 30 * time.Second

// Listener is one cluster-wide bound listener, folded from a ListenerAdvertise.
// The identity fields (Node..Image) come from the advertise's ContainerMeta;
// Addr/Port/PID/Process/Wildcard come from its attributes.
type Listener struct {
	Addr        string // bind "ip:port", verbatim from the advertise
	Port        string // port only
	PID         string
	Process     string
	Wildcard    bool // bound to 0.0.0.0/:: (indexed per pod under its port)
	NodeName    string
	Namespace   string
	Pod         string
	Container   string
	ContainerID string
	Image       string

	// The following fields are populated ONLY on a HowServiceVIP resolution;
	// they are zero for a listener folded from an advertise. Service names the
	// Kubernetes Service whose ClusterIP the connect targeted. When the Service
	// has more than one live backend replica, ReplicaAmbiguous is set and
	// ReplicaCount reports how many: the backends share one process+image (they
	// are replicas of one workload), so Process/Image/Namespace are the correct
	// remote identity, but the specific replica Pod/NodeName here is the
	// representative backend, not necessarily the one kube-proxy DNATed the flow
	// to. This is the honesty boundary: the process is resolved, the pod is not.
	Service          ServiceRef
	ReplicaAmbiguous bool
	ReplicaCount     int

	expires time.Time
}

// ServiceRef identifies the Kubernetes Service a Service-VIP connect resolved
// through. Its String form ("namespace/name") is carried on the ClusterPeerEdge
// as peer_service.
type ServiceRef struct {
	Namespace string
	Name      string
}

// String renders the ref as "namespace/name" (empty when unset).
func (s ServiceRef) String() string {
	if s.Namespace == "" && s.Name == "" {
		return ""
	}
	return s.Namespace + "/" + s.Name
}

// How records the path by which Join resolved (or failed to resolve) a connect
// destination to a Listener.
type How int

const (
	// HowMiss means no live listener backed the connect destination.
	HowMiss How = iota
	// HowDirect means an exact "ip:port" registry hit.
	HowDirect
	// HowEndpointslice means the destination was resolved to a backing pod
	// "ip:port" through the EndpointSlice index, then matched exactly.
	HowEndpointslice
	// HowWildcard means the destination matched a wildcard (0.0.0.0) listener:
	// the connect's pod IP was resolved to its owning pod and matched that pod's
	// wildcard entry on the port, or (absent an IP→pod resolution) a single
	// unambiguous wildcard listener held the port.
	HowWildcard
	// HowServiceVIP means the destination was a Kubernetes Service ClusterIP:port
	// resolved through the Service + EndpointSlice maps to its backend
	// listener(s). kube-proxy DNATs a ClusterIP:svcPort to a backend
	// podIP:targetPort, and the backends are replicas of one workload, so the
	// resolved process/image is correct even when the specific replica pod is
	// ambiguous (see Listener.ReplicaAmbiguous).
	HowServiceVIP
)

func (h How) String() string {
	switch h {
	case HowDirect:
		return "direct"
	case HowEndpointslice:
		return "endpointslice"
	case HowWildcard:
		return "wildcard"
	case HowServiceVIP:
		return "service-vip"
	default:
		return "miss"
	}
}

// Via maps the resolution path onto the ClusterPeerEdge `via` attribute, one of
// {"direct","endpointslice","service-vip"}. A wildcard match is still a direct
// registry lookup (no EndpointSlice was consulted), so it reports "direct".
func (h How) Via() string {
	switch h {
	case HowEndpointslice:
		return "endpointslice"
	case HowServiceVIP:
		return "service-vip"
	default:
		return "direct"
	}
}

// EndpointResolver maps a connect destination ("ip:port": a Service VIP or a
// DNATed endpoint) to the backing pod "ip:port" a listener would have
// advertised. Implemented by discovery.PodEndpointIndex. An implementation MUST
// return ok=false rather than fabricate a mapping it cannot justify.
type EndpointResolver interface {
	ResolvePodAddr(addr string) (podAddr string, ok bool)
}

// PodLocator maps a concrete pod IP to the identity (namespace, pod) of the pod
// that owns it. It is the key to attributing a wildcard (0.0.0.0/::) bind to the
// SPECIFIC advertising pod: every server pod that binds 0.0.0.0:port shares one
// port-only key, so a connect to a concrete pod IP must be resolved to its pod
// (via this locator) and matched against that pod's wildcard entry, never a
// shared per-port key that collides across pods. An IP the locator has not seen
// returns ok=false: an honest miss, never a guessed (possibly stale) pod.
// Implemented by discovery.PodEndpointIndex.
type PodLocator interface {
	PodForIP(ip string) (namespace, pod string, ok bool)
}

// ServiceResolver maps a Kubernetes Service ClusterIP:port (a VIP) to the set of
// backend listener addresses ("podIP:targetPort") and the owning Service's
// identity. It joins watched Service objects (ClusterIP + port→targetPort) with
// the backing EndpointSlices (ready pod IPs), so it resolves ONLY a VIP the
// cluster data actually supports: an unknown ClusterIP, a Service with no ready
// backend, or a port the Service does not expose returns ok=false — never a
// fabricated backend. Implemented by discovery.ServiceIndex.
type ServiceResolver interface {
	ResolveVIP(addr string) (backends []string, namespace, name string, ok bool)
}

// Registry is the TTL-scoped cluster listener registry. It is safe for
// concurrent use. Populate it with Observe (from ListenerAdvertise) and query
// it with Join (from NetworkExchange).
type Registry struct {
	mu    sync.RWMutex
	exact map[string]Listener // key: "ip:port"
	// wild indexes 0.0.0.0/:: binds, keyed by ":port" then by the advertising
	// pod's "namespace/pod" so same-port pods keep distinct entries.
	wild map[string]map[string]Listener
	ttl  time.Duration

	// resolver is consulted on an exact miss to fold Service/endpoint IPs to a
	// backing pod addr. Optional; nil disables the EndpointSlice path.
	resolver EndpointResolver

	// svc is consulted on an exact miss to resolve a Service ClusterIP:port to
	// its backend listener set. Optional; nil disables the Service-VIP path.
	svc ServiceResolver

	// podLoc maps a connect's pod IP to its owning pod, so a wildcard bind is
	// attributed to the specific advertising pod rather than a shared per-port
	// key. Derived from resolver when it also implements PodLocator (the
	// production discovery.PodEndpointIndex does). nil disables the pod-scoped
	// wildcard path, leaving only the single-unambiguous-pod fallback.
	podLoc PodLocator

	// now is overridable in tests to drive TTL expiry deterministically.
	now func() time.Time
}

// NewRegistry returns a registry with the given entry TTL (0 → DefaultTTL), an
// optional EndpointSlice resolver (nil disables the endpointslice join path),
// and an optional Service-VIP resolver (nil disables the service-vip path).
func NewRegistry(ttl time.Duration, resolver EndpointResolver, svc ServiceResolver) *Registry {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	r := &Registry{
		exact:    make(map[string]Listener),
		wild:     make(map[string]map[string]Listener),
		ttl:      ttl,
		resolver: resolver,
		svc:      svc,
		now:      time.Now,
	}
	// The pod-endpoint resolver doubles as the pod locator when it can map a pod
	// IP to its identity, which is what makes wildcard binds resolvable per pod.
	if pl, ok := resolver.(PodLocator); ok {
		r.podLoc = pl
	}
	return r
}

// Observe folds one advertised listener into the registry, refreshing its TTL.
// A listener with an empty Addr is ignored. A wildcard (0.0.0.0/::) listener is
// indexed per advertising pod under its port, so pods that all bind the same
// wildcard port keep distinct entries and never collide; a concrete-addr
// (non-wildcard) listener is indexed exactly.
func (r *Registry) Observe(l Listener) {
	if l.Addr == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	l.expires = r.now().Add(r.ttl)
	if l.Wildcard {
		port := portOf(l.Port, l.Addr)
		if port == "" {
			return // a wildcard bind with no port is not resolvable
		}
		key := ":" + port
		inner := r.wild[key]
		if inner == nil {
			inner = make(map[string]Listener)
			r.wild[key] = inner
		}
		inner[podIdent(l.Namespace, l.Pod)] = l
		return
	}
	r.exact[l.Addr] = l
}

// Remove retires a listener the moment its binder exits, the counterpart of
// Observe. It deletes the exact-addr or per-pod wildcard entry the advertise
// created, so a withdrawn listener stops resolving connects immediately rather
// than lingering until its TTL expires. A withdraw that names no live entry
// (already expired, or never observed) is a no-op.
func (r *Registry) Remove(l Listener) {
	if l.Addr == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if l.Wildcard {
		port := portOf(l.Port, l.Addr)
		if port == "" {
			return
		}
		key := ":" + port
		inner := r.wild[key]
		if inner == nil {
			return
		}
		delete(inner, podIdent(l.Namespace, l.Pod))
		if len(inner) == 0 {
			delete(r.wild, key)
		}
		return
	}
	delete(r.exact, l.Addr)
}

// Join resolves connectAddr ("ip:port") to a live cluster listener. It returns
// the matched Listener, the resolution path, and ok=false on a miss. connectorNode
// is the connecting side's node; callers use it to set the ClusterPeerEdge
// attribution ("cross-node" vs "same-node"). Resolution order:
//  1. exact "ip:port" registry hit                → HowDirect
//  2. EndpointSlice fold to pod "ip:port", exact  → HowEndpointslice
//  3. Service ClusterIP:port → backend listener(s) → HowServiceVIP
//  4. wildcard bind, resolved per pod (last resort) → HowWildcard
//
// VIP resolution precedes the wildcard fallback so a ClusterIP:port reaches its
// backend rather than an unrelated bind on the same port.
// Join is READ-ONLY and takes the shared lock, so joins from every agent stream
// proceed concurrently; only Observe and Sweep take the exclusive lock. An
// expired entry is therefore a miss, never a delete: reclamation belongs to
// Sweep, since deleting here would serialize every join behind the write lock.
func (r *Registry) Join(connectAddr, _ string) (Listener, How, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()

	// 1. Exact hit.
	if l, ok := r.liveExact(connectAddr, now); ok {
		return l, HowDirect, true
	}

	// 2. EndpointSlice fold: resolve to the backing pod addr, then match exactly.
	if r.resolver != nil {
		if podAddr, ok := r.resolver.ResolvePodAddr(connectAddr); ok {
			if l, ok := r.liveExact(podAddr, now); ok {
				return l, HowEndpointslice, true
			}
			// The resolved pod may itself be a wildcard bind; honour it but keep
			// the endpointslice classification (an EndpointSlice was consulted).
			if l, ok := r.liveWild(podAddr, now); ok {
				return l, HowEndpointslice, true
			}
		}
	}

	// 3. Service VIP: resolve a ClusterIP:port to its backend listener set.
	if r.svc != nil {
		if backends, ns, name, ok := r.svc.ResolveVIP(connectAddr); ok {
			if l, ok := r.resolveVIP(backends, ServiceRef{Namespace: ns, Name: name}, now); ok {
				return l, HowServiceVIP, true
			}
		}
	}

	// 4. Wildcard (0.0.0.0) listener on the destination port.
	if l, ok := r.liveWild(connectAddr, now); ok {
		return l, HowWildcard, true
	}

	return Listener{}, HowMiss, false
}

// resolveVIP maps a Service's backend listener addresses to a single resolved
// peer identity. It gathers the live listeners among backends (each is a
// "podIP:targetPort" a replica advertised) and:
//   - 0 live backends            → miss (no fabricated peer).
//   - 1 live backend             → resolve to it exactly (Service.String set).
//   - N live, same process+image → resolve to their common process/image and
//     mark the specific replica ambiguous (ReplicaAmbiguous, ReplicaCount=N).
//   - N live, disagreeing        → miss (a real Service backs one workload; a
//     disagreement means the data is not something we can honestly attribute).
//
// Backends are already deterministically ordered by the resolver, so the
// representative (live[0]) is stable across calls. Caller holds r.mu.
func (r *Registry) resolveVIP(backends []string, svc ServiceRef, now time.Time) (Listener, bool) {
	var live []Listener
	for _, b := range backends {
		if l, ok := r.liveExact(b, now); ok {
			live = append(live, l)
			continue
		}
		// A backend may have advertised a wildcard bind on its target port.
		if l, ok := r.liveWild(b, now); ok {
			live = append(live, l)
		}
	}
	if len(live) == 0 {
		return Listener{}, false
	}
	rep := live[0]
	rep.Service = svc
	if len(live) == 1 {
		return rep, true
	}
	for _, l := range live[1:] {
		if l.Process != rep.Process || l.Image != rep.Image {
			// Defensive: should not happen for a real Service (its backends are
			// replicas of one workload). Do not guess across distinct workloads.
			return Listener{}, false
		}
	}
	rep.ReplicaAmbiguous = true
	rep.ReplicaCount = len(live)
	return rep, true
}

// liveExact returns the exact-addr entry if present, unexpired, and still owned
// by the pod that advertised it. An expired entry is a miss left for Sweep to
// reclaim, so this stays read-only. Caller holds r.mu.
func (r *Registry) liveExact(addr string, now time.Time) (Listener, bool) {
	l, ok := r.exact[addr]
	if !ok {
		return Listener{}, false
	}
	if !l.expires.After(now) {
		return Listener{}, false
	}
	if !r.stillOwns(l, addr) {
		return Listener{}, false
	}
	return l, true
}

// stillOwns reports whether the cluster still places addr's IP on the pod that
// advertised l. A cluster recycles pod IPs continuously, so an advertise can
// outlive its pod: the entry sits within its TTL while the IP already belongs to
// a successor, and returning it would name a departed process as the peer.
//
// The test is one-sided. Only a POSITIVE disagreement rejects, when the pod index
// places the IP on some other pod. An IP the index has never seen is not evidence
// against l, since a listener pod behind no Service appears in no EndpointSlice
// at all. Caller holds r.mu.
func (r *Registry) stillOwns(l Listener, addr string) bool {
	if r.podLoc == nil || l.Pod == "" {
		return true
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return true
	}
	ns, pod, ok := r.podLoc.PodForIP(host)
	if !ok {
		return true
	}
	return ns == l.Namespace && pod == l.Pod
}

// liveWild resolves addr against the wildcard index without colliding across
// pods sharing a wildcard port. With a PodLocator wired, the connect IP is
// mapped to its owning pod and matched against that pod's entry, so two pods
// on 0.0.0.0:port are told apart by IP. Outcomes:
//
//   - live pod with its own entry -> resolve to it;
//   - live pod, no entry under that identity -> the port's sole live listener
//     if it names the same pod, covering an advertise/EndpointSlice namespace
//     disagreement; two or more live listeners are ambiguous, so a miss;
//   - unplaceable IP (unknown or since-deleted) -> miss, never a same-port
//     fall-through, which is what keeps eviction honest.
//
// Without a PodLocator, placement is skipped and the port's sole live listener
// answers directly. Expired entries are swept as seen. Caller holds r.mu.
func (r *Registry) liveWild(addr string, now time.Time) (Listener, bool) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return Listener{}, false
	}
	key := ":" + port
	if len(r.wild[key]) == 0 {
		return Listener{}, false
	}

	if r.podLoc != nil {
		ns, pod, ok := r.podLoc.PodForIP(host)
		if !ok {
			// Unplaceable IP: a miss, never a same-port fall-through.
			return Listener{}, false
		}
		if l, ok := r.liveWildPod(key, podIdent(ns, pod), now); ok {
			return l, true
		}
		// Same-pod fallback only, tolerating an advertise/index namespace
		// disagreement. Any-sole-entry would answer with an unrelated workload,
		// since every wildcard bind on a port shares one key.
		return r.soleLiveWildForPod(key, pod, now)
	}

	// Degraded mode (no pod index): honour the port's wildcard only when a
	// single live pod advertised it (unambiguous).
	return r.soleLiveWild(key, now)
}

// soleLiveWild returns the single live wildcard listener under portKey when
// EXACTLY ONE remains. Two or more live listeners are ambiguous (miss); zero is
// a miss. Expired entries are skipped, not deleted, so this stays read-only.
// Caller holds r.mu.
func (r *Registry) soleLiveWild(portKey string, now time.Time) (Listener, bool) {
	var found Listener
	live := 0
	for _, l := range r.wild[portKey] {
		if !l.expires.After(now) {
			continue
		}
		found = l
		live++
	}
	if live == 1 {
		return found, true
	}
	return Listener{}, false
}

// soleLiveWildForPod: the sole live wildcard listener under portKey when it
// names pod. Pod-name match tolerates a namespace disagreement without
// answering for another workload. Caller holds r.mu.
func (r *Registry) soleLiveWildForPod(portKey, pod string, now time.Time) (Listener, bool) {
	l, ok := r.soleLiveWild(portKey, now)
	if !ok || l.Pod != pod {
		return Listener{}, false
	}
	return l, true
}

// liveWildPod returns the wildcard entry a specific pod advertised under portKey.
// A pod that advertised no wildcard on this port is a miss: the caller must not
// attribute the connect to any other pod. Read-only. Caller holds r.mu.
func (r *Registry) liveWildPod(portKey, pod string, now time.Time) (Listener, bool) {
	l, ok := r.wild[portKey][pod]
	if !ok || !l.expires.After(now) {
		return Listener{}, false
	}
	return l, true
}

// podIdent is the wildcard index's inner key: the advertising pod's identity.
func podIdent(namespace, pod string) string {
	return namespace + "/" + pod
}

// Sweep reclaims expired entries. Reclamation is this path's job, not the read
// path's, which is what lets Join run under the shared lock.
func (r *Registry) Sweep() { _, _ = r.Stats() }

// SweepEvery reclaims expired entries every interval until ctx is done. A zero
// or negative interval falls back to the entry TTL, bounding the residency of an
// expired entry to about one TTL beyond its expiry.
func (r *Registry) SweepEvery(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = r.ttl
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.Sweep()
		}
	}
}

// Stats reports the number of live (unexpired) exact and wildcard entries,
// sweeping expired entries as a side effect so the registry does not grow
// unboundedly when advertises stop arriving.
func (r *Registry) Stats() (exact, wild int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for k, l := range r.exact {
		if !l.expires.After(now) {
			delete(r.exact, k)
			continue
		}
		exact++
	}
	for key, inner := range r.wild {
		for k, l := range inner {
			if !l.expires.After(now) {
				delete(inner, k)
				continue
			}
			wild++
		}
		if len(inner) == 0 {
			delete(r.wild, key)
		}
	}
	return exact, wild
}

// Size returns the total number of live entries (exact + wildcard). Sampled by
// the kloudlens_agg_listener_registry_size gauge.
func (r *Registry) Size() int {
	e, w := r.Stats()
	return e + w
}

// portOf returns the port component. It prefers an explicit port string, else
// parses it out of "ip:port" (tolerating bare "ip" with no port).
func portOf(explicit, addr string) string {
	if explicit != "" {
		return explicit
	}
	if _, p, err := net.SplitHostPort(addr); err == nil {
		return p
	}
	return ""
}
