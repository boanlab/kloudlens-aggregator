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
	mu    sync.Mutex
	exact map[string]Listener // key: "ip:port"
	// wild indexes 0.0.0.0/:: binds. It is a two-level map, keyed first by
	// ":port" and then by the advertising pod's "namespace/pod", so pods that
	// all bind the same wildcard port each keep a DISTINCT entry instead of
	// colliding on one shared key (the correctness bug this structure fixes).
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

// Join resolves connectAddr ("ip:port") to a live cluster listener. It returns
// the matched Listener, the resolution path, and ok=false on a miss. connectorNode
// is the connecting side's node; callers use it to set the ClusterPeerEdge
// attribution ("cross-node" vs "same-node"). Resolution order mirrors the spec:
//  1. exact "ip:port" registry hit                → HowDirect
//  2. EndpointSlice fold to pod "ip:port", exact  → HowEndpointslice
//  3. Service ClusterIP:port → backend listener(s) → HowServiceVIP
//  4. wildcard bind, resolved per pod (last resort) → HowWildcard
//
// Service-VIP resolution runs before the wildcard fallback so a ClusterIP:port
// is attributed to its real backend workload rather than colliding with an
// unrelated wildcard bind that happens to share the destination port. The
// wildcard step itself never collides across pods: it resolves the connect's
// pod IP to its owning pod (via the PodLocator) and matches that pod's wildcard
// entry, so two pods that both bind 0.0.0.0:port are told apart by their IPs.
func (r *Registry) Join(connectAddr, _ string) (Listener, How, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// liveExact returns the exact-addr entry if present and unexpired, deleting it
// (and any wildcard mirror) on expiry. Caller holds r.mu.
func (r *Registry) liveExact(addr string, now time.Time) (Listener, bool) {
	l, ok := r.exact[addr]
	if !ok {
		return Listener{}, false
	}
	if !l.expires.After(now) {
		delete(r.exact, addr)
		return Listener{}, false
	}
	return l, true
}

// liveWild resolves addr against the wildcard index WITHOUT colliding across
// pods that all bound the same wildcard port.
//
// When a PodLocator is wired, resolution is pod-scoped: the connect's IP is
// mapped to its owning pod and matched against that pod's wildcard entry. Two
// pods that both bind 0.0.0.0:port are told apart by their IPs.
//
// The locator's answer is the honesty pivot:
//   - The IP maps to a live pod that filed a wildcard entry under that identity →
//     resolve to it (the collision-free churn path).
//   - The IP maps to a CONFIRMED-LIVE pod but no entry sits under that exact
//     identity → fall back to the port's SOLE unambiguous live wildcard listener.
//     This restores the base cross-node join when the advertising agent's pod
//     identity and the EndpointSlice targetRef disagree on the namespace/pod
//     strings for the same physical pod (two independent data sources). It is
//     safe: the IP is a confirmed-live pod (not a stale/deleted one), and a
//     single live listener on the port is unambiguous. With two or more live
//     listeners the port is genuinely ambiguous, so we miss rather than guess.
//   - The IP cannot be placed at all (unknown, or a since-deleted pod the
//     EndpointSlice has dropped) → MISS. It must never fall through to a same-port
//     pod, which would attribute the connect to a possibly stale listener. This is
//     what makes eviction honest: a deleted pod disappears from the locator
//     immediately, before its advertise TTL, so its IP no longer resolves.
//
// Only when NO PodLocator is wired at all (a degraded mode without a pod index)
// does it skip placement and honour the port's sole live wildcard listener
// directly. Expired entries are swept as seen. Caller holds r.mu.
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
			// The connect IP cannot be placed to any pod (unknown or since-deleted):
			// an honest miss, never a fall-through to a same-port pod.
			r.sweepWild(key, now)
			return Listener{}, false
		}
		// The IP is a confirmed-live pod. Prefer its own wildcard entry (this is
		// what distinguishes two pods sharing the port); otherwise fall back to the
		// port's sole live listener to survive an advertise/index identity mismatch.
		if l, ok := r.liveWildPod(key, podIdent(ns, pod), now); ok {
			return l, true
		}
		return r.soleLiveWild(key, now)
	}

	// Degraded mode (no pod index): honour the port's wildcard only when a
	// single live pod advertised it (unambiguous).
	return r.soleLiveWild(key, now)
}

// soleLiveWild returns the single live wildcard listener under portKey when
// EXACTLY ONE remains, sweeping expired entries (and the empty bucket) as it
// goes. Two or more live listeners are ambiguous (miss); zero is a miss. Caller
// holds r.mu.
func (r *Registry) soleLiveWild(portKey string, now time.Time) (Listener, bool) {
	inner := r.wild[portKey]
	var found Listener
	live := 0
	for k, l := range inner {
		if !l.expires.After(now) {
			delete(inner, k)
			continue
		}
		found = l
		live++
	}
	if len(inner) == 0 {
		delete(r.wild, portKey)
	}
	if live == 1 {
		return found, true
	}
	return Listener{}, false
}

// sweepWild drops expired entries under portKey (and the empty port bucket).
// Caller holds r.mu.
func (r *Registry) sweepWild(portKey string, now time.Time) {
	inner := r.wild[portKey]
	for k, l := range inner {
		if !l.expires.After(now) {
			delete(inner, k)
		}
	}
	if len(inner) == 0 {
		delete(r.wild, portKey)
	}
}

// liveWildPod returns the wildcard entry a specific pod advertised under portKey,
// deleting it on expiry. A pod that advertised no wildcard on this port is a
// miss (the caller must not attribute the connect to any other pod). Caller
// holds r.mu.
func (r *Registry) liveWildPod(portKey, pod string, now time.Time) (Listener, bool) {
	inner := r.wild[portKey]
	l, ok := inner[pod]
	if !ok {
		return Listener{}, false
	}
	if !l.expires.After(now) {
		delete(inner, pod)
		if len(inner) == 0 {
			delete(r.wild, portKey)
		}
		return Listener{}, false
	}
	return l, true
}

// podIdent is the wildcard index's inner key: the advertising pod's identity.
func podIdent(namespace, pod string) string {
	return namespace + "/" + pod
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
