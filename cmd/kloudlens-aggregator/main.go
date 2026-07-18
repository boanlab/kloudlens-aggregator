// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Command kloudlens-aggregator is the optional cluster fan-in. It
// subscribes to every listed agent's EventService and merges their
// envelopes into a single NDJSON sink. Shipped as an optional Deployment
// under `deployments/manifests/` (disabled by default; enable by applying
// the aggregator yaml alongside the agent DaemonSet).
//
// Features:
//   - static --agents=host:port,host:port plus EndpointSlice discovery
//     for dynamic agent rosters;
//   - per-(agent, stream) cursor persistence and a parallel envelope WAL;
//   - NDJSON sink to stdout/file/stderr and optional re-export gRPC;
//   - /healthz + /readyz; optional /metrics on --metrics-addr.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator"
	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/clusterpeers"
	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/discovery"
	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/envwal"
	pb "github.com/boanlab/kloudlens/protobuf"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// version is set at build time via -ldflags "-X main.version=<tag>".
var version = "dev"

type cliFlags struct {
	agents       string
	output       string
	streams      string
	consumerID   string
	backoff      time.Duration
	queueDepth   int
	metricsAddr  string
	cursorFile   string
	walDir       string
	walMaxBytes  int64
	walSegBytes  int64
	walTTL       time.Duration
	walGCEvery   time.Duration
	reexportAddr string
	aggregatorID string
	k8sService   string
	k8sAPIServer string
	k8sCAFile    string
	k8sTokenFile string
	agentPort    int
	resolveVIP   bool
	showVersion  bool
}

func parseFlags() cliFlags {
	var f cliFlags
	flag.StringVar(&f.agents, "agents", "",
		"comma-separated agent addresses (host:port[,host:port...]); required")
	flag.StringVar(&f.output, "output", "-",
		"NDJSON sink — '-' for stdout, 'stderr' for stderr, otherwise a file path")
	flag.StringVar(&f.streams, "streams", "intent",
		"comma-separated stream names to subscribe to (intent|deviation|graph_edge|lifecycle|audit)")
	flag.StringVar(&f.consumerID, "consumer-id", "kloudlens-aggregator",
		"consumer_id tag sent on every Subscribe; per-agent cursor is keyed on this value")
	flag.DurationVar(&f.backoff, "backoff", 2*time.Second,
		"retry delay between Subscribe reconnects on a single agent")
	flag.IntVar(&f.queueDepth, "queue-depth", 1024,
		"internal fan-in channel size; envelopes dropped when full")
	flag.StringVar(&f.metricsAddr, "metrics-addr", "",
		"host:port to serve /healthz, /readyz, /metrics, /stats (disabled when empty)")
	flag.StringVar(&f.cursorFile, "cursor-file", "",
		"path to the per-(agent,stream) cursor JSON (empty = no persistence, restart resumes from live tail)")
	flag.StringVar(&f.walDir, "wal-dir", "",
		"aggregator WAL directory for merged envelopes (empty = no WAL; re-export gRPC still serves live subscribers only)")
	flag.Int64Var(&f.walMaxBytes, "wal-max-bytes", 2<<30,
		"soft cap on WAL retention bytes; oldest segments trimmed on overflow")
	flag.Int64Var(&f.walSegBytes, "wal-segment-bytes", 32<<20,
		"WAL segment rotation threshold")
	flag.DurationVar(&f.walTTL, "wal-ttl", 2*time.Hour,
		"WAL segment TTL; segments older than this are removed on each GC tick")
	flag.DurationVar(&f.walGCEvery, "wal-gc-interval", time.Minute,
		"WAL janitor tick interval")
	flag.StringVar(&f.reexportAddr, "reexport-addr", "",
		"host:port for the re-export EventService gRPC (empty = disabled)")
	flag.StringVar(&f.aggregatorID, "aggregator-id", "kloudlens-aggregator",
		"NodeId stamped on outgoing re-export cursors; lets downstream federate multiple clusters")
	flag.StringVar(&f.k8sService, "k8s-service", "",
		"namespace/name of the headless Service backing the agent DaemonSet; when set, --agents is ignored and EndpointSlice is watched live")
	flag.StringVar(&f.k8sAPIServer, "k8s-apiserver", "",
		"Kubernetes API server URL (default: https://kubernetes.default.svc)")
	flag.StringVar(&f.k8sCAFile, "k8s-ca-file", "",
		"Kubernetes CA bundle path (default: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt)")
	flag.StringVar(&f.k8sTokenFile, "k8s-token-file", "",
		"Kubernetes ServiceAccount token path (default: /var/run/secrets/kubernetes.io/serviceaccount/token)")
	flag.IntVar(&f.agentPort, "agent-port", 0,
		"TCP port to use for each discovered agent pod; 0 = first port in the EndpointSlice")
	flag.BoolVar(&f.resolveVIP, "resolve-service-vip", true,
		"with --k8s-service, watch Services + EndpointSlices cluster-wide to resolve connects to a Service ClusterIP (VIP) to the backing remote workload (needs cluster-scoped get/list/watch on services and endpointslices)")
	flag.BoolVar(&f.showVersion, "version", false,
		"print the build version and exit")
	flag.Parse()
	return f
}

func main() {
	f := parseFlags()
	if f.showVersion {
		fmt.Println(version)
		return
	}
	if err := run(f); err != nil {
		log.SetFlags(0)
		log.Fatalf("kloudlens-aggregator: %v", err)
	}
}

func run(f cliFlags) error {
	if f.agents == "" && f.k8sService == "" {
		return fmt.Errorf("one of --agents or --k8s-service is required")
	}
	var agents []aggregator.AgentEndpoint
	if f.agents != "" {
		agents = parseAgents(f.agents)
		if len(agents) == 0 && f.k8sService == "" {
			return fmt.Errorf("--agents parsed to zero endpoints: %q", f.agents)
		}
	}

	out, closer, err := openOutput(f.output)
	if err != nil {
		return err
	}
	defer closer()

	var cursors aggregator.CursorStore = aggregator.NullCursorStore()
	if f.cursorFile != "" {
		c, err := aggregator.NewFileCursorStore(f.cursorFile, 0)
		if err != nil {
			return err
		}
		cursors = c
		defer func() { _ = c.Close() }()
	}

	var wal *envwal.WAL
	if f.walDir != "" {
		w, err := envwal.Open(envwal.Options{
			Dir:         f.walDir,
			MaxBytes:    f.walMaxBytes,
			SegmentSize: f.walSegBytes,
			TTL:         f.walTTL,
		})
		if err != nil {
			return fmt.Errorf("open wal: %w", err)
		}
		wal = w
		defer func() { _ = w.Close() }()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var discoveryCh <-chan []aggregator.AgentEndpoint
	var podIndex *discovery.PodEndpointIndex
	var svcIndex *discovery.ServiceIndex
	if f.k8sService != "" {
		// (1) Agent-discovery watch — scoped to the agent Service, used only to
		// discover agent pods to subscribe to. Does not feed the pod-index.
		ch, err := startK8sDiscovery(ctx, f)
		if err != nil {
			return err
		}
		discoveryCh = ch

		// (2) Cluster-wide Service + EndpointSlice watch — the source for
		// cross-node peer resolution. It folds every workload's pod IP into the
		// PodEndpointIndex (needed for the direct pod-IP path) and, when
		// --resolve-service-vip is on, resolves Service ClusterIPs (VIPs) too.
		// svcIndex is always built so the watcher has its required Index; VIP
		// resolution is only exposed to the aggregator when the flag is on.
		podIndex = discovery.NewPodEndpointIndex()
		svcIndex = discovery.NewServiceIndex()
		startClusterEndpointWatch(ctx, f, svcIndex, podIndex)
	}

	agg, err := aggregator.New(aggregator.Config{
		Agents:           agents,
		Out:              out,
		Streams:          strings.Split(f.streams, ","),
		ConsumerID:       f.consumerID,
		Backoff:          f.backoff,
		QueueDepth:       f.queueDepth,
		Cursors:          cursors,
		WAL:              wal,
		Discovery:        discoveryCh,
		EndpointResolver: resolverOrNil(podIndex),
		// svcIndex is always built (to feed the watcher), but VIP resolution is
		// only wired into joins when the operator opts in via --resolve-service-vip.
		ServiceResolver: svcResolverWhen(f.resolveVIP, svcIndex),
	})
	if err != nil {
		return err
	}

	if wal != nil && f.walGCEvery > 0 {
		go runWALGC(ctx, wal, f.walGCEvery)
	}

	if f.reexportAddr != "" {
		stopReexport, err := serveReExport(f.reexportAddr, f.aggregatorID, agg)
		if err != nil {
			return err
		}
		defer stopReexport()
	}

	if f.metricsAddr != "" {
		go serveMetrics(f.metricsAddr, agg)
	}

	fmt.Fprintf(os.Stderr, "kloudlens-aggregator: agents=%d streams=%s output=%s\n",
		len(agents), f.streams, f.output)
	return agg.Run(ctx)
}

// resolverOrNil returns idx as a clusterpeers.EndpointResolver, or an untyped
// nil interface when idx is nil (avoids a typed-nil interface that the
// aggregator would treat as a live-but-panicking resolver).
func resolverOrNil(idx *discovery.PodEndpointIndex) clusterpeers.EndpointResolver {
	if idx == nil {
		return nil
	}
	return idx
}

// svcResolverWhen returns idx as a clusterpeers.ServiceResolver when enabled,
// or an untyped nil interface otherwise (same typed-nil guard as resolverOrNil).
// The Service watch always runs to feed the pod-index; this only controls
// whether Service-VIP resolution is consulted during joins.
func svcResolverWhen(enabled bool, idx *discovery.ServiceIndex) clusterpeers.ServiceResolver {
	if !enabled || idx == nil {
		return nil
	}
	return idx
}

// startClusterEndpointWatch launches the cluster-wide Service + EndpointSlice
// watch. It feeds podIndex (every workload's pod IP → its pod identity, for the
// direct cross-node path) always, and svcIndex (Service ClusterIP → backend,
// for the VIP path) which the caller exposes to joins only under
// --resolve-service-vip. Runs until ctx is cancelled; watch errors are retried
// internally, so a transient RBAC/API hiccup degrades joins to misses rather
// than crashing the aggregator.
func startClusterEndpointWatch(ctx context.Context, f cliFlags, svcIdx *discovery.ServiceIndex, podIdx *discovery.PodEndpointIndex) {
	w := &discovery.ServiceWatcher{
		APIServer: f.k8sAPIServer,
		CAFile:    f.k8sCAFile,
		TokenFile: f.k8sTokenFile,
		Index:     svcIdx,
		PodIndex:  podIdx,
	}
	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("cluster endpoint watch: %v", err)
		}
	}()
	fmt.Fprintln(os.Stderr, "kloudlens-aggregator: cluster endpoint watch (pod-index + service-vip) watching services + endpointslices cluster-wide")
}

func parseAgents(csv string) []aggregator.AgentEndpoint {
	var out []aggregator.AgentEndpoint
	for _, a := range strings.Split(csv, ",") {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		out = append(out, aggregator.AgentEndpoint{Name: a, Addr: a})
	}
	return out
}

func openOutput(p string) (io.Writer, func(), error) {
	switch p {
	case "-":
		return os.Stdout, func() {}, nil
	case "stderr":
		return os.Stderr, func() {}, nil
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- p is operator-supplied output path
	if err != nil {
		return nil, nil, fmt.Errorf("open output %s: %w", p, err)
	}
	return f, func() { _ = f.Close() }, nil
}

// buildMetricsMux returns the http.Handler that serves the four
// operator-facing endpoints (/healthz, /readyz, /metrics, /stats). It is
// kept separate from serveMetrics so tests can assert the /stats plaintext
// shape against the same handler the binary actually uses, without
// binding a real listener.
func buildMetricsMux(agg *aggregator.Aggregator) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(aggregator.NewPromCollector(agg))

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		s := agg.Stats()
		fmt.Fprintf(w, "received=%d written=%d dropped=%d errors=%d wal_appends=%d wal_errors=%d wal_last_seq=%d subscriber_dropped=%d subscribers=%d\n",
			s.Received, s.Written, s.Dropped, s.Errors, s.WALAppends, s.WALErrors, s.WALLastSeq, s.SubscriberDropped, s.Subscribers)
	})
	return mux
}

func serveMetrics(addr string, agg *aggregator.Aggregator) {
	srv := &http.Server{Addr: addr, Handler: buildMetricsMux(agg), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("metrics: %v", err)
	}
}

// serveReExport starts a grpc.Server on addr that publishes the aggregator's
// merged stream to downstream consumers. Returns a stop func that blocks
// until the server has drained active calls.
func serveReExport(addr, aggregatorID string, agg *aggregator.Aggregator) (func(), error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("reexport listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()
	rx := aggregator.NewReExportServer(agg)
	if aggregatorID != "" {
		rx.AggregatorID = aggregatorID
	}
	pb.RegisterEventServiceServer(srv, rx)
	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("reexport serve: %v", err)
		}
	}()
	fmt.Fprintf(os.Stderr, "kloudlens-aggregator: re-export listening on %s (id=%s)\n", addr, aggregatorID)
	return func() { srv.GracefulStop() }, nil
}

// startK8sDiscovery translates the CLI k8s flags into an
// EndpointSliceWatcher and bridges its Endpoint emissions into the
// aggregator.AgentEndpoint shape the aggregator consumes. Runs until ctx is
// cancelled; the returned channel closes when the watcher goroutine exits.
func startK8sDiscovery(ctx context.Context, f cliFlags) (<-chan []aggregator.AgentEndpoint, error) {
	ns, svc, ok := strings.Cut(f.k8sService, "/")
	if !ok || ns == "" || svc == "" {
		return nil, fmt.Errorf("--k8s-service must be namespace/name, got %q", f.k8sService)
	}
	// This watch is scoped to the agent Service (namespace + service-name
	// selector) and is used only to discover agent pods to subscribe to. It
	// deliberately does NOT feed the pod-index: that index needs every
	// workload's pod IP cluster-wide, which the cluster-wide Service +
	// EndpointSlice watch (startClusterEndpointWatch) supplies instead.
	w := &discovery.EndpointSliceWatcher{
		APIServer:   f.k8sAPIServer,
		CAFile:      f.k8sCAFile,
		TokenFile:   f.k8sTokenFile,
		Namespace:   ns,
		ServiceName: svc,
		TargetPort:  int32(f.agentPort), // #nosec G115 -- CLI flag (int) narrowing to int32 port
	}
	src, err := w.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("k8s discovery: %w", err)
	}
	out := make(chan []aggregator.AgentEndpoint, 4)
	go func() {
		defer close(out)
		// Aggregator opens plaintext gRPC connections to discovered pod
		// IPs — all subscribe/admin surfaces run insecure, so only
		// expose the agent Service on trusted cluster networks.
		dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		for eps := range src {
			conv := make([]aggregator.AgentEndpoint, 0, len(eps))
			for _, e := range eps {
				conv = append(conv, aggregator.AgentEndpoint{
					Name:     e.Name,
					Addr:     e.Addr,
					DialOpts: dialOpts,
				})
			}
			select {
			case <-ctx.Done():
				return
			case out <- conv:
			}
		}
	}()
	fmt.Fprintf(os.Stderr, "kloudlens-aggregator: k8s discovery watching %s/%s\n", ns, svc)
	return out, nil
}

func runWALGC(ctx context.Context, w *envwal.WAL, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.GC(); err != nil {
				log.Printf("wal gc: %v", err)
			}
		}
	}
}
