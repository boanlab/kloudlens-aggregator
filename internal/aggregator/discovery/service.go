// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package discovery

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ServiceIndex resolves a Kubernetes Service ClusterIP:port (a VIP) to the set
// of backend listener addresses ("podIP:targetPort") and the owning Service.
//
// It joins two live cluster views:
//   - Service objects give ClusterIP(s) and the svcPort→(named) target-port map;
//   - EndpointSlice objects give, per Service, the numeric target port (resolved
//     from the port name) and the set of ready backend pod IPs.
//
// A connect to ClusterIP:svcPort is DNATed by kube-proxy to podIP:targetPort of
// one backend replica. Because a Service's backends are replicas of one
// workload, resolving the VIP to that backend set yields the correct remote
// process/image; only the specific replica pod is ambiguous when there is more
// than one backend (the caller in clusterpeers handles that).
//
// ResolveVIP returns ok=false for an unknown ClusterIP, a port the Service does
// not expose, or a Service with no ready backends, rather than fabricating a
// backend the cluster data does not support.
type ServiceIndex struct {
	mu       sync.RWMutex
	services map[string]serviceEntry // key: ClusterIP
	slices   map[string]sliceGroup   // key: "namespace/serviceName"
}

// serviceEntry is one Service's identity and its published ports.
type serviceEntry struct {
	namespace string
	name      string
	ports     []svcPortRef
}

// svcPortRef is a single Service port: what a client targets on the VIP (port)
// and the name used to join it to the EndpointSlice's numeric target port.
type svcPortRef struct {
	port int32
	name string
}

// sliceGroup aggregates all EndpointSlices of one Service: the port-name→numeric
// target-port map and the sorted set of ready backend pod IPs.
type sliceGroup struct {
	ports  map[string]int32
	podIPs []string
}

// NewServiceIndex returns an empty, ready-to-use index.
func NewServiceIndex() *ServiceIndex {
	return &ServiceIndex{
		services: make(map[string]serviceEntry),
		slices:   make(map[string]sliceGroup),
	}
}

// SetServices atomically replaces the Service half of the index.
func (idx *ServiceIndex) SetServices(m map[string]serviceEntry) {
	idx.mu.Lock()
	idx.services = m
	idx.mu.Unlock()
}

// SetSlices atomically replaces the EndpointSlice half of the index.
func (idx *ServiceIndex) SetSlices(m map[string]sliceGroup) {
	idx.mu.Lock()
	idx.slices = m
	idx.mu.Unlock()
}

// ResolveVIP maps a connect "ClusterIP:port" to its backend listener addresses
// ("podIP:targetPort") and the owning Service (namespace, name). ok=false on any
// honesty miss: malformed addr, unknown ClusterIP, a port the Service does not
// expose, an unresolved target port, or no ready backend. Backends are returned
// in a deterministic order so the caller's representative replica is stable.
func (idx *ServiceIndex) ResolveVIP(addr string) (backends []string, namespace, name string, ok bool) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, "", "", false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, "", "", false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	svc, ok := idx.services[host]
	if !ok {
		return nil, "", "", false
	}
	portName, found := "", false
	for _, p := range svc.ports {
		// Widen the int32 Service port to int for the compare rather than
		// narrowing the parsed connect port to int32: a port outside the int32
		// range (an out-of-spec addr) then simply fails to match instead of
		// wrapping into a spurious hit.
		if int(p.port) == port {
			portName, found = p.name, true
			break
		}
	}
	if !found {
		return nil, "", "", false
	}
	grp, ok := idx.slices[svc.namespace+"/"+svc.name]
	if !ok {
		return nil, "", "", false
	}
	tp, ok := grp.ports[portName]
	if !ok || tp == 0 {
		return nil, "", "", false
	}
	if len(grp.podIPs) == 0 {
		return nil, "", "", false
	}
	tps := strconv.Itoa(int(tp))
	out := make([]string, 0, len(grp.podIPs))
	for _, ip := range grp.podIPs {
		out = append(out, ip+":"+tps)
	}
	return out, svc.namespace, svc.name, true
}

// NumServices reports the number of live Services indexed (counted by distinct
// ClusterIP entry). Sampled by the kloudlens_agg_services_watched gauge.
func (idx *ServiceIndex) NumServices() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.services)
}

// ServiceWatcher keeps a ServiceIndex live by watching Service and EndpointSlice
// objects cluster-wide. Intentionally dep-light: direct REST + watch against the
// kube apiserver, mirroring EndpointSliceWatcher.
type ServiceWatcher struct {
	// APIServer, when empty, defaults to https://kubernetes.default.svc.
	APIServer string
	// CAFile + TokenFile default to the in-cluster ServiceAccount mounts.
	CAFile    string
	TokenFile string
	// Index is refreshed on every Service/EndpointSlice change. Required.
	Index *ServiceIndex
	// PodIndex, when non-nil, is refreshed from the same cluster-wide
	// EndpointSlice watch so a connect to a concrete pod IP resolves to its
	// backing pod. Optional: nil leaves pod-IP resolution to whatever else
	// feeds the index. This is what makes the direct (pod-IP) cross-node path
	// work — the agent-discovery watch only sees the agent Service's endpoints.
	// Must come from a cluster-wide watch: a per-Service watch places no
	// workload pod.
	PodIndex *PodEndpointIndex
	// HTTPClient overrides the default client (used by tests). nil in production.
	HTTPClient *http.Client
	// ReconnectBackoff is the delay between watch reconnects. 0 → 2s.
	ReconnectBackoff time.Duration
}

// Run starts the Service and EndpointSlice watch loops and blocks until ctx is
// cancelled. Both resources are watched cluster-wide, so the ServiceAccount must
// carry cluster-scoped get/list/watch on services and endpointslices.
func (w *ServiceWatcher) Run(ctx context.Context) error {
	if w.Index == nil {
		return fmt.Errorf("discovery: ServiceWatcher requires an Index")
	}
	if w.APIServer == "" {
		w.APIServer = defaultAPIServer
	}
	if w.CAFile == "" {
		w.CAFile = defaultCAFile
	}
	if w.TokenFile == "" {
		w.TokenFile = defaultTokenFile
	}
	if w.ReconnectBackoff == 0 {
		w.ReconnectBackoff = 2 * time.Second
	}
	if w.HTTPClient == nil {
		cl, err := buildClient(w.CAFile)
		if err != nil {
			return err
		}
		w.HTTPClient = cl
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		w.watchLoop(ctx, "/api/v1/services", w.applyServices)
	}()
	go func() {
		defer wg.Done()
		w.watchLoop(ctx, "/apis/discovery.k8s.io/v1/endpointslices", w.applySlices)
	}()
	// Pods are watched only to name a destination pod IP, and only when an
	// index exists to name it into. Without it, a connect to a Service-less pod
	// resolves to no pod and its wildcard bind is indistinguishable from any
	// other listener on that port.
	if w.PodIndex != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.watchLoop(ctx, "/api/v1/pods", w.applyPods)
		}()
	}
	wg.Wait()
	return nil
}

// podObject is the sliver of a Pod this index needs: who it is and which IP
// answers for it. Pods sharing the node's network namespace are skipped: a
// hostNetwork pod's IP is the node's, so indexing it would attribute every
// host-network flow on that node to whichever such pod was seen last.
type podObject struct {
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		HostNetwork bool `json:"hostNetwork"`
	} `json:"spec"`
	Status struct {
		PodIP  string `json:"podIP"`
		PodIPs []struct {
			IP string `json:"ip"`
		} `json:"podIPs"`
		Phase string `json:"phase"`
	} `json:"status"`
}

// applyPods refreshes the pod-IP identities from a whole-state pod list.
func (w *ServiceWatcher) applyPods(state map[string]json.RawMessage) {
	if w.PodIndex == nil {
		return
	}
	out := make([]PodIdentity, 0, len(state))
	for _, raw := range state {
		var po podObject
		if err := json.Unmarshal(raw, &po); err != nil {
			continue
		}
		if po.Metadata.Name == "" || po.Spec.HostNetwork || po.Status.Phase != "Running" {
			continue
		}
		// podIPs carries every assigned family and repeats podIP as its first
		// entry. Index all of them so a dual-stack pod is nameable on either
		// address rather than only on the one the status happened to summarize.
		ips := []string{}
		if po.Status.PodIP != "" {
			ips = append(ips, po.Status.PodIP)
		}
		for _, e := range po.Status.PodIPs {
			if e.IP != "" && e.IP != po.Status.PodIP {
				ips = append(ips, e.IP)
			}
		}
		for _, ip := range ips {
			out = append(out, PodIdentity{IP: ip, Namespace: po.Metadata.Namespace, Pod: po.Metadata.Name})
		}
	}
	w.PodIndex.ReplacePodIdentities(out)
}

// watchLoop runs list→watch iterations for one resource path until ctx is
// cancelled, keying object state by "namespace/name" and invoking apply after
// every LIST replace and every watch event.
func (w *ServiceWatcher) watchLoop(ctx context.Context, path string, apply func(map[string]json.RawMessage)) {
	state := make(map[string]json.RawMessage)
	for {
		if ctx.Err() != nil {
			return
		}
		rv, err := w.list(ctx, path, state)
		if err != nil {
			w.sleepBackoff(ctx)
			continue
		}
		apply(state)
		if err := w.watch(ctx, path, rv, state, apply); err != nil {
			w.sleepBackoff(ctx)
			continue
		}
	}
}

func (w *ServiceWatcher) sleepBackoff(ctx context.Context) {
	t := time.NewTimer(w.ReconnectBackoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (w *ServiceWatcher) get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	auth, err := bearerAuth(w.TokenFile)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", auth)
	return w.HTTPClient.Do(req)
}

// list performs the initial LIST and replaces state wholesale (so objects
// deleted while disconnected do not linger). Returns the resourceVersion.
func (w *ServiceWatcher) list(ctx context.Context, path string, state map[string]json.RawMessage) (string, error) {
	resp, err := w.get(ctx, w.APIServer+path)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("discovery: list %s %d: %s", path, resp.StatusCode, string(b))
	}
	var lr rawList
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return "", fmt.Errorf("discovery: decode list %s: %w", path, err)
	}
	for k := range state {
		delete(state, k)
	}
	for _, item := range lr.Items {
		if key, ok := keyOf(item); ok {
			state[key] = item
		}
	}
	return lr.Metadata.ResourceVersion, nil
}

// watch streams watch events into state, invoking apply on every change.
// Returns nil iff ctx was cancelled.
func (w *ServiceWatcher) watch(ctx context.Context, path, rv string, state map[string]json.RawMessage, apply func(map[string]json.RawMessage)) error {
	url := fmt.Sprintf("%s%s?resourceVersion=%s&watch=true&allowWatchBookmarks=true", w.APIServer, path, rv)
	resp, err := w.get(ctx, url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("discovery: watch %s %d: %s", path, resp.StatusCode, string(b))
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 1<<16), 4<<20)
	for sc.Scan() {
		if ctx.Err() != nil {
			return nil
		}
		var ev rawEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			return fmt.Errorf("discovery: parse watch event %s: %w", path, err)
		}
		switch ev.Type {
		case "ADDED", "MODIFIED":
			if key, ok := keyOf(ev.Object); ok {
				state[key] = ev.Object
			}
		case "DELETED":
			if key, ok := keyOf(ev.Object); ok {
				delete(state, key)
			}
		case "BOOKMARK":
			continue
		case "ERROR":
			return fmt.Errorf("discovery: watch ERROR %s: %s", path, string(sc.Bytes()))
		default:
			continue
		}
		apply(state)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}

// applyServices rebuilds the ClusterIP→serviceEntry map from Service objects.
func (w *ServiceWatcher) applyServices(state map[string]json.RawMessage) {
	out := make(map[string]serviceEntry)
	for _, raw := range state {
		var so serviceObject
		if err := json.Unmarshal(raw, &so); err != nil {
			continue
		}
		if so.Metadata.Name == "" {
			continue
		}
		var ports []svcPortRef
		for _, p := range so.Spec.Ports {
			if p.Port != 0 {
				ports = append(ports, svcPortRef{port: p.Port, name: p.Name})
			}
		}
		if len(ports) == 0 {
			continue
		}
		entry := serviceEntry{namespace: so.Metadata.Namespace, name: so.Metadata.Name, ports: ports}
		for _, ip := range clusterIPsOf(so) {
			out[ip] = entry
		}
	}
	w.Index.SetServices(out)
}

// applySlices rebuilds the "namespace/serviceName"→sliceGroup map from
// EndpointSlice objects, keeping only ready backend endpoints.
func (w *ServiceWatcher) applySlices(state map[string]json.RawMessage) {
	type acc struct {
		ports map[string]int32
		ips   map[string]struct{}
	}
	groups := make(map[string]*acc)
	for _, raw := range state {
		var so svcSliceObject
		if err := json.Unmarshal(raw, &so); err != nil {
			continue
		}
		svcName := so.Metadata.Labels["kubernetes.io/service-name"]
		if svcName == "" {
			continue
		}
		key := so.Metadata.Namespace + "/" + svcName
		g := groups[key]
		if g == nil {
			g = &acc{ports: make(map[string]int32), ips: make(map[string]struct{})}
			groups[key] = g
		}
		for _, p := range so.Ports {
			if p.Port != 0 {
				g.ports[p.Name] = p.Port
			}
		}
		for _, ep := range so.Endpoints {
			if !sliceEndpointReady(ep.Conditions.Ready, ep.Conditions.Terminating) {
				continue
			}
			for _, ip := range ep.Addresses {
				g.ips[ip] = struct{}{}
			}
		}
	}
	// Replace is whole-state, so a pod whose EndpointSlice vanished (deleted /
	// not-ready) drops out of the index — the connect to its now-stale IP then
	// misses honestly instead of resolving to a same-port survivor.
	if w.PodIndex != nil {
		w.PodIndex.Replace(podEntriesFromSlices(state))
	}
	out := make(map[string]sliceGroup, len(groups))
	for key, g := range groups {
		ips := make([]string, 0, len(g.ips))
		for ip := range g.ips {
			ips = append(ips, ip)
		}
		sort.Strings(ips)
		out[key] = sliceGroup{ports: g.ports, podIPs: ips}
	}
	w.Index.SetSlices(out)
}

// podEntriesFromSlices folds cluster-wide EndpointSlice state into the pod-IP
// index. Ready endpoints with a Pod targetRef only; others are skipped, leaving
// an unknown IP as a miss.
func podEntriesFromSlices(state map[string]json.RawMessage) []PodEndpointEntry {
	byIP := make(map[string]*PodEndpointEntry)
	for _, raw := range state {
		var so svcSliceObject
		if err := json.Unmarshal(raw, &so); err != nil {
			continue
		}
		var ports []int32
		for _, p := range so.Ports {
			if p.Port != 0 {
				ports = append(ports, p.Port)
			}
		}
		for _, ep := range so.Endpoints {
			if !sliceEndpointReady(ep.Conditions.Ready, ep.Conditions.Terminating) {
				continue
			}
			if ep.TargetRef == nil || ep.TargetRef.Kind != "Pod" || ep.TargetRef.Name == "" {
				continue
			}
			ns := ep.TargetRef.Namespace
			if ns == "" {
				ns = so.Metadata.Namespace
			}
			for _, ip := range ep.Addresses {
				e := byIP[ip]
				if e == nil {
					e = &PodEndpointEntry{IP: ip, Namespace: ns, Pod: ep.TargetRef.Name}
					byIP[ip] = e
				}
				for _, pt := range ports {
					if !containsPort(e.Ports, pt) {
						e.Ports = append(e.Ports, pt)
					}
				}
			}
		}
	}
	out := make([]PodEndpointEntry, 0, len(byIP))
	for _, e := range byIP {
		sort.Slice(e.Ports, func(i, j int) bool { return e.Ports[i] < e.Ports[j] })
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IP < out[j].IP })
	return out
}

func containsPort(s []int32, v int32) bool {
	for _, e := range s {
		if e == v {
			return true
		}
	}
	return false
}

// clusterIPsOf returns the routable ClusterIP(s) of a Service, skipping the
// "None" sentinel of a headless Service (no VIP to resolve).
func clusterIPsOf(so serviceObject) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(ip string) {
		if ip == "" || ip == "None" {
			return
		}
		if _, ok := seen[ip]; ok {
			return
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	add(so.Spec.ClusterIP)
	for _, ip := range so.Spec.ClusterIPs {
		add(ip)
	}
	return out
}

// sliceEndpointReady mirrors the k8s convention: a nil Ready means "unknown,
// assume ready unless explicitly terminating".
func sliceEndpointReady(ready, terminating *bool) bool {
	if ready == nil {
		return terminating == nil || !*terminating
	}
	return *ready
}

// keyOf extracts an object's "namespace/name" state key from its raw JSON.
func keyOf(raw json.RawMessage) (string, bool) {
	var m objMeta
	if json.Unmarshal(raw, &m) != nil || m.Metadata.Name == "" {
		return "", false
	}
	return m.Metadata.Namespace + "/" + m.Metadata.Name, true
}

// --- decode shapes (only the fields we need) ---

type rawList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []json.RawMessage `json:"items"`
}

type rawEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

type objMeta struct {
	Metadata struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	} `json:"metadata"`
}

type serviceObject struct {
	Metadata struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		ClusterIP  string   `json:"clusterIP"`
		ClusterIPs []string `json:"clusterIPs"`
		Ports      []struct {
			Name string `json:"name"`
			Port int32  `json:"port"`
		} `json:"ports"`
	} `json:"spec"`
}

type svcSliceObject struct {
	Metadata struct {
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	Ports []struct {
		Name string `json:"name"`
		Port int32  `json:"port"`
	} `json:"ports"`
	Endpoints []struct {
		Addresses  []string `json:"addresses"`
		Conditions struct {
			Ready       *bool `json:"ready"`
			Terminating *bool `json:"terminating"`
		} `json:"conditions"`
		// TargetRef identifies the pod backing this endpoint. Its namespace/name
		// feed the PodEndpointIndex so a connect to a concrete pod IP can be
		// attributed to the specific pod even when it bound a wildcard address.
		// A pointer because the field is optional: an endpoint with no targetRef
		// (or one referring to something other than a Pod) names no pod, and the
		// fold has to skip it rather than record a blank identity.
		TargetRef *struct {
			Kind      string `json:"kind"`
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		} `json:"targetRef"`
	} `json:"endpoints"`
}
