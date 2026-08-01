// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Package discovery watches Kubernetes EndpointSlice objects to produce
// the live set of agent endpoints the aggregator subscribes to. Using
// EndpointSlice — rather than resolving the headless Service DNS once at
// startup — lets operators roll nodes in/out of the DaemonSet without
// restarting the aggregator.
//
// Intentionally dep-light: direct REST + watch against the kube apiserver
// instead of pulling k8s.io/client-go (~30 transitive deps for one watch).
package discovery

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Endpoint is a discovered agent: one addressable pod backing the Service.
type Endpoint struct {
	Name string // slice_name/addr — stable enough for cursor keying
	Addr string // host:port
}

// EndpointSliceWatcher watches EndpointSlice objects for a Service and emits
// the current endpoint set on every change. Safe for one Run caller.
type EndpointSliceWatcher struct {
	// APIServer, when empty, defaults to https://kubernetes.default.svc.
	APIServer string
	// CAFile + TokenFile default to the in-cluster ServiceAccount mounts.
	CAFile    string
	TokenFile string
	// Namespace + ServiceName select the EndpointSlices to follow.
	Namespace   string
	ServiceName string
	// TargetPort picks the port on each endpoint. 0 → first port in the
	// slice's Ports array (typical when the Service has one port).
	TargetPort int32
	// HTTPClient overrides the default client (used by tests). Production
	// callers leave this nil.
	HTTPClient *http.Client
	// ReconnectBackoff is the delay between watch reconnects. 0 → 2s.
	ReconnectBackoff time.Duration
}

const (
	defaultAPIServer = "https://kubernetes.default.svc"
	defaultCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	defaultTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token" // #nosec G101 -- well-known kubelet SA path, not a hardcoded secret
)

// Run blocks and emits the current endpoint set on every change through the
// returned channel. The channel closes when ctx is cancelled. Internally a
// list+watch loop reconnects on disconnect; cursor state is preserved by
// the aggregator (per-(agent,stream) cursor store) so reconnects are
// lossless.
func (w *EndpointSliceWatcher) Run(ctx context.Context) (<-chan []Endpoint, error) {
	if w.Namespace == "" || w.ServiceName == "" {
		return nil, errors.New("discovery: Namespace and ServiceName are required")
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
			return nil, err
		}
		w.HTTPClient = cl
	}

	out := make(chan []Endpoint, 4)
	go w.loop(ctx, out)
	return out, nil
}

func buildClient(caFile string) (*http.Client, error) {
	caBytes, err := os.ReadFile(caFile) // #nosec G304 -- caFile is a CLI/config-supplied TLS trust root
	if err != nil {
		return nil, fmt.Errorf("discovery: read CA %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("discovery: CA file %s has no PEM certs", caFile)
	}
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		ResponseHeaderTimeout: 30 * time.Second,
	}
	// Note: no overall client Timeout — the watch stream is long-lived.
	return &http.Client{Transport: tr}, nil
}

func (w *EndpointSliceWatcher) authHeader() (string, error) {
	return bearerAuth(w.TokenFile)
}

// bearerAuth reads a ServiceAccount token file and returns the Authorization
// header value. Shared by the EndpointSlice and Service watchers.
func bearerAuth(tokenFile string) (string, error) {
	token, err := os.ReadFile(tokenFile) // #nosec G304 -- tokenFile is the in-cluster ServiceAccount token path (defaultTokenFile or operator-set), not user input
	if err != nil {
		return "", fmt.Errorf("discovery: read token %s: %w", tokenFile, err)
	}
	return "Bearer " + strings.TrimSpace(string(token)), nil
}

// loop runs list→watch iterations forever until ctx is cancelled.
func (w *EndpointSliceWatcher) loop(ctx context.Context, out chan<- []Endpoint) {
	defer close(out)
	state := make(map[string]sliceObject) // key: slice name
	for {
		if ctx.Err() != nil {
			return
		}
		rv, err := w.list(ctx, state)
		if err != nil {
			w.sleepBackoff(ctx)
			continue
		}
		emit(ctx, out, state, w.TargetPort)
		if err := w.watchUntilErr(ctx, rv, state, out); err != nil {
			w.sleepBackoff(ctx)
			continue
		}
	}
}

func (w *EndpointSliceWatcher) sleepBackoff(ctx context.Context) {
	t := time.NewTimer(w.ReconnectBackoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// list performs the initial LIST and replaces state. Returns the
// resourceVersion to start the watch from.
func (w *EndpointSliceWatcher) list(ctx context.Context, state map[string]sliceObject) (string, error) {
	u := fmt.Sprintf("%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?labelSelector=%s",
		w.APIServer, url.PathEscape(w.Namespace),
		url.QueryEscape("kubernetes.io/service-name="+w.ServiceName))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	auth, err := w.authHeader()
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", auth)
	resp, err := w.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("discovery: list %d: %s", resp.StatusCode, string(b))
	}
	var listResp sliceList
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return "", fmt.Errorf("discovery: decode list: %w", err)
	}
	// Replace state wholesale — ensures objects deleted while we were
	// disconnected are not lingering.
	for k := range state {
		delete(state, k)
	}
	for _, item := range listResp.Items {
		state[item.Metadata.Name] = item
	}
	return listResp.Metadata.ResourceVersion, nil
}

// watchUntilErr streams watch events into state, emitting on every change.
// Returns nil iff ctx was cancelled; any other return is an error.
func (w *EndpointSliceWatcher) watchUntilErr(ctx context.Context, rv string, state map[string]sliceObject, out chan<- []Endpoint) error {
	u := fmt.Sprintf("%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?labelSelector=%s&resourceVersion=%s&watch=true&allowWatchBookmarks=true",
		w.APIServer, url.PathEscape(w.Namespace),
		url.QueryEscape("kubernetes.io/service-name="+w.ServiceName),
		url.QueryEscape(rv))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	auth, err := w.authHeader()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)
	resp, err := w.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("discovery: watch %d: %s", resp.StatusCode, string(b))
	}
	// Watch events arrive newline-delimited. Use Scanner with a generous
	// buffer — some slices carry hundreds of endpoints and exceed the
	// default 64KiB token size.
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 1<<16), 4<<20)
	for sc.Scan() {
		if ctx.Err() != nil {
			return nil
		}
		var ev watchEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			return fmt.Errorf("discovery: parse watch event: %w", err)
		}
		switch ev.Type {
		case "ADDED", "MODIFIED":
			state[ev.Object.Metadata.Name] = ev.Object
		case "DELETED":
			delete(state, ev.Object.Metadata.Name)
		case "BOOKMARK":
			// Pure resume hint — nothing to do.
			continue
		case "ERROR":
			return fmt.Errorf("discovery: watch ERROR: %s", string(sc.Bytes()))
		default:
			continue
		}
		emit(ctx, out, state, w.TargetPort)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}

// sliceList is the shape of the LIST response; only the fields we need are
// decoded — the rest is ignored. Same for watchEvent + sliceObject.
type sliceList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []sliceObject `json:"items"`
}

type watchEvent struct {
	Type   string      `json:"type"`
	Object sliceObject `json:"object"`
}

type sliceObject struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	AddressType string          `json:"addressType"`
	Ports       []slicePort     `json:"ports"`
	Endpoints   []sliceEndpoint `json:"endpoints"`
}

type slicePort struct {
	Port int32  `json:"port"`
	Name string `json:"name"`
}

type sliceEndpoint struct {
	Addresses  []string `json:"addresses"`
	Conditions struct {
		Ready       *bool `json:"ready"`
		Serving     *bool `json:"serving"`
		Terminating *bool `json:"terminating"`
	} `json:"conditions"`
	NodeName string `json:"nodeName"`
}

// emit snapshots state into an Endpoint slice and sends it on out. Callers
// hand in targetPort; if 0, the first port in the slice is used.
func emit(ctx context.Context, out chan<- []Endpoint, state map[string]sliceObject, targetPort int32) {
	endpoints := flatten(state, targetPort)
	select {
	case <-ctx.Done():
	case out <- endpoints:
	}
}

// flatten produces a deterministic endpoint list (sorted by addr) so
// aggregator diffs don't thrash goroutine bookkeeping on every emit.
func flatten(state map[string]sliceObject, targetPort int32) []Endpoint {
	var endpoints []Endpoint
	for _, slice := range state {
		port := pickPort(slice, targetPort)
		if port == 0 {
			continue
		}
		for _, ep := range slice.Endpoints {
			if !endpointReady(ep) {
				continue
			}
			for _, addr := range ep.Addresses {
				endpoints = append(endpoints, Endpoint{
					Name: slice.Metadata.Name + "/" + addr,
					Addr: addr + ":" + strconv.Itoa(int(port)),
				})
			}
		}
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Addr < endpoints[j].Addr })
	return endpoints
}

func pickPort(s sliceObject, want int32) int32 {
	if want == 0 && len(s.Ports) > 0 {
		return s.Ports[0].Port
	}
	for _, p := range s.Ports {
		if p.Port == want {
			return p.Port
		}
	}
	return 0
}

func endpointReady(ep sliceEndpoint) bool {
	// Missing Ready means "unknown, assume ready" per the k8s convention
	// (sliceEndpoint.conditions is optional).
	if ep.Conditions.Ready == nil {
		return ep.Conditions.Terminating == nil || !*ep.Conditions.Terminating
	}
	return *ep.Conditions.Ready
}
