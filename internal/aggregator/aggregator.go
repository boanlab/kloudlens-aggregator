// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Package aggregator is the cluster fan-in component shipped as the
// kloudlens-aggregator Deployment. N per-agent gRPC Subscribe goroutines
// push EventEnvelopes to a shared channel; a single writer goroutine
// emits NDJSON to an io.Writer. A parallel envelope WAL, per-(agent,
// stream) cursor persistence, an optional re-export gRPC surface, and
// EndpointSlice-based discovery are available via Config fields.
package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/clusterpeers"
	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/envwal"
	pb "github.com/boanlab/kloudlens/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

// AgentEndpoint is one agent the aggregator subscribes to. DialOpts is
// optional — when empty, the aggregator applies insecure credentials. Tests
// pass a bufconn contextDialer in here so they can exercise the full fan-in
// loop without a real TCP listener.
type AgentEndpoint struct {
	Name     string
	Addr     string
	DialOpts []grpc.DialOption
}

// Config configures a single Aggregator run. All fields except Agents/Out
// have sensible defaults; callers typically only populate those two.
type Config struct {
	Agents     []AgentEndpoint
	Out        io.Writer
	Streams    []string      // streams to subscribe to per agent; empty defaults to ["intent"]
	ConsumerID string        // reused across agents; cursor is per (agent, consumer, stream) anyway
	Backoff    time.Duration // retry delay on stream error; 0 → 2s
	QueueDepth int           // internal channel capacity; 0 → 1024

	// Cursors persists per-(agent, stream) resume points. When nil the
	// aggregator restarts from nil on every reconnect; when set, every
	// Subscribe carries the last-seen Cursor so the agent's WAL replay
	// resumes precisely and restarts are lossless. Typical wiring is
	// NewFileCursorStore(stateDir/cursors.json, 0).
	Cursors CursorStore

	// WAL, when non-nil, durably persists every merged envelope with a
	// cluster-assigned seq before (or alongside) the NDJSON sink. The
	// re-export gRPC surface serves downstream subscribers by replaying
	// this log. Caller owns Open/Close/GC.
	WAL *envwal.WAL

	// Discovery, when non-nil, replaces the static Agents list with a live
	// fleet feed — the aggregator spawns/cancels per-(agent, stream)
	// goroutines as endpoints appear and disappear. The channel must emit
	// an initial snapshot on startup and then one slice per change. When
	// set, Config.Agents is treated as the seed (may be empty).
	Discovery <-chan []AgentEndpoint

	// NewClient is overridable for tests that want to inject bufconn dialers.
	// Production callers leave it nil (the default uses grpc.NewClient).
	NewClient func(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

	// AdvertiseTTL is the lifetime of a cluster listener registry entry before
	// a fresh ListenerAdvertise must refresh it. 0 → clusterpeers.DefaultTTL (30s).
	AdvertiseTTL time.Duration

	// EndpointResolver, when set, folds Service/endpoint IPs to backing pod
	// addresses during a cross-node join (the EndpointSlice DNAT case).
	// discovery.PodEndpointIndex satisfies this. nil disables the endpointslice
	// join path (exact + wildcard still resolve). When it also exposes Len()
	// int, that count feeds the endpointslice-entries gauge.
	EndpointResolver clusterpeers.EndpointResolver

	// ServiceResolver, when set, resolves a Service ClusterIP:port (a VIP) to
	// its backend listener set during a cross-node join (the kube-proxy DNAT
	// case that carries most real cross-node traffic). discovery.ServiceIndex
	// satisfies this. nil disables the service-vip join path. When it also
	// exposes NumServices() int, that count feeds the services-watched gauge.
	ServiceResolver clusterpeers.ServiceResolver
}

// Aggregator is the run-time state built from Config. Create with New() and
// invoke Run(ctx) — when ctx is cancelled, Run waits for in-flight writes
// before returning. Stats() is safe to call while Run is active.
type Aggregator struct {
	cfg Config
	ch  chan envelopeWithAgent

	received    atomic.Uint64 // envelopes pulled off agent streams
	written     atomic.Uint64 // envelopes successfully NDJSON-encoded
	dropped     atomic.Uint64 // envelopes dropped because ch was full
	errors      atomic.Uint64 // per-stream recv errors observed
	walAppends  atomic.Uint64 // envelopes appended to the aggregator WAL
	walErrors   atomic.Uint64 // WAL append failures (NDJSON emit still attempted)
	subsDropped atomic.Uint64 // broadcast drops summed across already-unregistered subscribers
	xnodeJoins  atomic.Uint64 // NetworkExchange peers resolved to a cluster listener
	xnodeMisses atomic.Uint64 // NetworkExchange peers with no live listener (no edge emitted)
	vipJoins    atomic.Uint64 // subset of xnodeJoins resolved through a Service VIP

	// liveSeq is the in-memory counter the aggregator uses to tag live
	// broadcasts when no WAL is configured. When a WAL is set, broadcasts
	// reuse the WAL-assigned seq so re-export clients see a consistent
	// monotonic sequence across WAL replay → live tail.
	liveSeq atomic.Uint64

	subsMu sync.Mutex
	subs   map[*liveSubscriber]struct{}

	// registry folds ListenerAdvertise events into a TTL map and resolves
	// NetworkExchange peers into cross-node ClusterPeerEdge emits. Always
	// non-nil after New; the EndpointSlice and Service-VIP resolvers are optional.
	registry    *clusterpeers.Registry
	resolver    clusterpeers.EndpointResolver
	svcResolver clusterpeers.ServiceResolver
}

// liveSubscriber is one downstream re-export client. Each Subscribe call
// allocates one; writeLoop pushes every merged envelope to ch. Buffer is
// sized for brief replay-catch-up; on overflow the drop counter advances
// and re-export closes the stream so the client reconnects with a cursor.
type liveSubscriber struct {
	ch    chan liveEnvelope
	drops atomic.Uint64
}

type liveEnvelope struct {
	Seq      uint64
	Agent    string
	Envelope *pb.EventEnvelope
}

// envelopeWithAgent tags each envelope with the originating agent name so the
// merged NDJSON output is useful when multiple agents share a cluster.
type envelopeWithAgent struct {
	Agent    string
	Envelope *pb.EventEnvelope
}

// Stats is a snapshot of counters; exposed for the admin /metrics surface on
// the aggregator binary.
type Stats struct {
	Received uint64
	Written  uint64
	Dropped  uint64
	Errors   uint64
	// Subscribers is the live count of downstream re-export clients the
	// aggregator is fanning out to right now. Exported as a gauge so
	// operators can correlate subscriber-dropped spikes with fleet churn.
	Subscribers       int
	WALAppends        uint64
	WALErrors         uint64
	WALLastSeq        uint64
	SubscriberDropped uint64
	// Cross-node peer attribution.
	XNodeJoins           uint64 // NetworkExchange peers resolved to a cluster listener
	XNodeMisses          uint64 // NetworkExchange peers with no live listener
	VIPJoins             uint64 // subset of XNodeJoins resolved through a Service VIP
	ListenerRegistrySize int    // live entries in the cluster listener registry
	EndpointSliceEntries int    // live pod endpoints in the EndpointSlice index
	ServicesWatched      int    // live Services in the Service-VIP index
}

var errNoAgents = errors.New("aggregator: no agents configured")

// New validates cfg and returns an Aggregator ready for Run().
func New(cfg Config) (*Aggregator, error) {
	if len(cfg.Agents) == 0 && cfg.Discovery == nil {
		return nil, errNoAgents
	}
	if cfg.Out == nil {
		return nil, errors.New("aggregator: Out writer required")
	}
	if cfg.Backoff == 0 {
		cfg.Backoff = 2 * time.Second
	}
	if cfg.QueueDepth == 0 {
		cfg.QueueDepth = 1024
	}
	if len(cfg.Streams) == 0 {
		cfg.Streams = []string{"intent"}
	}
	if cfg.ConsumerID == "" {
		cfg.ConsumerID = "kloudlens-aggregator"
	}
	if cfg.NewClient == nil {
		cfg.NewClient = grpc.NewClient
	}
	if cfg.Cursors == nil {
		cfg.Cursors = NullCursorStore()
	}
	return &Aggregator{
		cfg:         cfg,
		ch:          make(chan envelopeWithAgent, cfg.QueueDepth),
		subs:        make(map[*liveSubscriber]struct{}),
		registry:    clusterpeers.NewRegistry(cfg.AdvertiseTTL, cfg.EndpointResolver, cfg.ServiceResolver),
		resolver:    cfg.EndpointResolver,
		svcResolver: cfg.ServiceResolver,
	}, nil
}

// registerSubscriber adds a live subscriber and returns it. Caller must call
// unregisterSubscriber when done. Buffer size is generous — re-export
// clients block on grpc.Send latency, not on back-pressure within the
// aggregator process — but a drop counter is exposed for observability.
func (a *Aggregator) registerSubscriber(buf int) *liveSubscriber {
	if buf <= 0 {
		buf = 1024
	}
	s := &liveSubscriber{ch: make(chan liveEnvelope, buf)}
	a.subsMu.Lock()
	a.subs[s] = struct{}{}
	a.subsMu.Unlock()
	return s
}

func (a *Aggregator) unregisterSubscriber(s *liveSubscriber) {
	a.subsMu.Lock()
	delete(a.subs, s)
	a.subsMu.Unlock()
	// Fold this subscriber's drops into the aggregator-wide accumulator
	// before releasing the pointer — otherwise the drop count vanishes on
	// disconnect and /metrics loses monotonicity across reconnects.
	a.subsDropped.Add(s.drops.Load())
	close(s.ch)
}

func (a *Aggregator) broadcast(ev liveEnvelope) {
	a.subsMu.Lock()
	defer a.subsMu.Unlock()
	for s := range a.subs {
		select {
		case s.ch <- ev:
		default:
			s.drops.Add(1)
		}
	}
}

// Run blocks until ctx is cancelled. It spawns one goroutine per
// (agent, stream) pair plus a single writer goroutine that drains the merge
// channel to cfg.Out. Returns nil on clean shutdown.
//
// Per-stream subscription (vs. one Subscribe carrying all streams) keeps the
// cursor model coherent: pb.Cursor is a single (NodeId, Stream, Seq) scalar,
// so a client that multiplexes streams through one Subscribe has no way to
// express distinct resume points. The per-stream loop pays for one extra
// gRPC stream per pair and buys lossless resume.
//
// When Config.Discovery is set the agent set is rebuilt on every emission
// from the channel — goroutines are started for new endpoints and cancelled
// for endpoints that disappear from the slice. Cursor state is preserved
// across endpoint churn (the per-(agent,stream) cursor file is keyed on the
// endpoint Name).
func (a *Aggregator) Run(ctx context.Context) error {
	// Two wait groups so we can close the merge channel exactly once all
	// agent goroutines have returned (otherwise they'd send on a closed
	// channel). The writer goroutine is on its own wg so Run only returns
	// after the last NDJSON line has been encoded.
	var agentWg, writerWg sync.WaitGroup

	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		a.writeLoop()
	}()

	// Seed with the static Agents list (discovery adds to it; if there's no
	// discovery, this is the whole fleet).
	active := make(map[string]*endpointRun)
	for _, ep := range a.cfg.Agents {
		a.startEndpoint(ctx, ep, active, &agentWg)
	}

	if a.cfg.Discovery != nil {
		a.reconcileLoop(ctx, active, &agentWg)
	} else {
		<-ctx.Done()
	}

	agentWg.Wait()
	close(a.ch)
	writerWg.Wait()
	_ = a.cfg.Cursors.Flush()
	return nil
}

// endpointRun tracks one active endpoint's per-stream cancels so reconcile
// can stop its streams when the endpoint leaves the fleet.
type endpointRun struct {
	cancel context.CancelFunc
}

// reconcileLoop drains the discovery channel. Each emission replaces the
// active set: new endpoints start, departed endpoints cancel. Returns when
// ctx is done or the channel closes.
func (a *Aggregator) reconcileLoop(ctx context.Context, active map[string]*endpointRun, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			return
		case eps, ok := <-a.cfg.Discovery:
			if !ok {
				// Discovery closed — no more fleet changes, but existing
				// streams should keep running until ctx is done.
				<-ctx.Done()
				return
			}
			a.reconcileFleet(ctx, eps, active, wg)
		}
	}
}

func (a *Aggregator) reconcileFleet(ctx context.Context, eps []AgentEndpoint, active map[string]*endpointRun, wg *sync.WaitGroup) {
	want := make(map[string]AgentEndpoint, len(eps))
	for _, ep := range eps {
		want[ep.Name] = ep
	}
	// Cancel departed endpoints.
	for name, run := range active {
		if _, keep := want[name]; !keep {
			run.cancel()
			delete(active, name)
		}
	}
	// Start newly-discovered endpoints.
	for name, ep := range want {
		if _, already := active[name]; already {
			continue
		}
		a.startEndpoint(ctx, ep, active, wg)
	}
}

func (a *Aggregator) startEndpoint(ctx context.Context, ep AgentEndpoint, active map[string]*endpointRun, wg *sync.WaitGroup) {
	epCtx, cancel := context.WithCancel(ctx)
	active[ep.Name] = &endpointRun{cancel: cancel}
	for _, stream := range a.cfg.Streams {
		wg.Add(1)
		go func(stream string) {
			defer wg.Done()
			a.streamLoop(epCtx, ep, stream)
		}(stream)
	}
}

// Stats returns the current counter snapshot. Safe for concurrent callers.
func (a *Aggregator) Stats() Stats {
	s := Stats{
		Received:    a.received.Load(),
		Written:     a.written.Load(),
		Dropped:     a.dropped.Load(),
		Errors:      a.errors.Load(),
		WALAppends:  a.walAppends.Load(),
		WALErrors:   a.walErrors.Load(),
		XNodeJoins:  a.xnodeJoins.Load(),
		XNodeMisses: a.xnodeMisses.Load(),
		VIPJoins:    a.vipJoins.Load(),
	}
	if a.registry != nil {
		s.ListenerRegistrySize = a.registry.Size()
	}
	if l, ok := a.resolver.(interface{ Len() int }); ok {
		s.EndpointSliceEntries = l.Len()
	}
	if sv, ok := a.svcResolver.(interface{ NumServices() int }); ok {
		s.ServicesWatched = sv.NumServices()
	}
	if a.cfg.WAL != nil {
		s.WALLastSeq = a.cfg.WAL.LastSeq()
	}
	// Sum live subscriber drops on top of already-folded accumulator so
	// operators see a monotonic total even as re-export clients churn.
	total := a.subsDropped.Load()
	a.subsMu.Lock()
	for sub := range a.subs {
		total += sub.drops.Load()
	}
	s.Subscribers = len(a.subs)
	a.subsMu.Unlock()
	s.SubscriberDropped = total
	return s
}

func (a *Aggregator) writeLoop() {
	// encoding/json can't serialize proto oneof payloads cleanly (it would
	// emit Go field names like `{"Payload":{"Intent":{...}}}` rather than the
	// proto JSON shape `{"intent":{...}}`), so we hand-assemble each NDJSON
	// line: `{"_agent":"<name>","envelope":<protojson>}\n`.
	marshal := protojson.MarshalOptions{UseProtoNames: false}
	for ev := range a.ch {
		// Cross-node hook. ListenerAdvertise events feed the registry and are
		// NOT re-emitted downstream (control-plane chatter; one per listener
		// every ~10s). A NetworkExchange that resolves to a cluster listener
		// yields a ClusterPeerEdge, emitted right after its source below so it
		// inherits the next cluster seq — the edge is contiguous with its
		// trigger in both the WAL and the re-export stream, and no extra
		// synchronisation is needed because writeLoop is the sole seq assigner.
		if a.foldAdvertise(ev.Envelope) {
			continue
		}
		a.emitEnvelope(marshal, ev)
		if edge := a.joinPeer(ev.Envelope); edge != nil {
			a.emitEnvelope(marshal, envelopeWithAgent{
				Agent:    ev.Agent,
				Envelope: &pb.EventEnvelope{Payload: &pb.EventEnvelope_Intent{Intent: edge}},
			})
		}
	}
}

// emitEnvelope assigns the next cluster seq (WAL or in-memory), broadcasts to
// re-export subscribers, and writes the NDJSON line. Extracted so a synthesised
// ClusterPeerEdge rides the exact same path as an upstream envelope.
func (a *Aggregator) emitEnvelope(marshal protojson.MarshalOptions, ev envelopeWithAgent) {
	var seq uint64
	if a.cfg.WAL != nil {
		s, err := a.cfg.WAL.Append(ev.Agent, ev.Envelope)
		if err != nil {
			a.walErrors.Add(1)
			// NDJSON emit is still worth attempting — a transient WAL
			// write failure shouldn't silence the live stream. We fall
			// back to the in-memory liveSeq so broadcast ordering holds.
			seq = a.liveSeq.Add(1)
		} else {
			a.walAppends.Add(1)
			seq = s
			a.liveSeq.Store(s)
		}
	} else {
		seq = a.liveSeq.Add(1)
	}
	a.broadcast(liveEnvelope{Seq: seq, Agent: ev.Agent, Envelope: ev.Envelope})
	envBytes, err := marshal.Marshal(ev.Envelope)
	if err != nil {
		a.errors.Add(1)
		return
	}
	agentBytes, err := marshalJSONString(ev.Agent)
	if err != nil {
		a.errors.Add(1)
		return
	}
	line := make([]byte, 0, len(envBytes)+len(agentBytes)+24)
	line = append(line, `{"_agent":`...)
	line = append(line, agentBytes...)
	line = append(line, `,"envelope":`...)
	line = append(line, envBytes...)
	line = append(line, '}', '\n')
	if _, err := a.cfg.Out.Write(line); err != nil {
		a.errors.Add(1)
		return
	}
	a.written.Add(1)
}

// foldAdvertise folds a ListenerAdvertise envelope into the cluster registry.
// It returns true when the envelope was an advertise (and should be consumed,
// not re-emitted downstream), false otherwise.
func (a *Aggregator) foldAdvertise(env *pb.EventEnvelope) bool {
	intent := env.GetIntent()
	if intent == nil || intent.GetKind() != clusterpeers.KindListenerAdvertise {
		return false
	}
	if l, ok := clusterpeers.ListenerFromAdvertise(intent); ok {
		a.registry.Observe(l)
	}
	return true
}

// joinPeer runs a cross-node attribution join for a NetworkExchange envelope
// whose peer was not already same-node-attributed by the kernel. On a hit it
// returns a ClusterPeerEdge IntentEvent; otherwise nil (and, on a genuine
// lookup, advances the miss counter). Envelopes that are not eligible
// NetworkExchange events return nil without touching either counter.
func (a *Aggregator) joinPeer(env *pb.EventEnvelope) *pb.IntentEvent {
	intent := env.GetIntent()
	if intent == nil || intent.GetKind() != clusterpeers.KindNetworkExchange {
		return nil
	}
	attrs := intent.GetAttributes()
	peer := attrs[clusterpeers.AttrPeer]
	if peer == "" {
		return nil
	}
	// The kernel already attributed a same-node peer; do not re-run the join.
	if _, already := attrs[clusterpeers.AttrPeerPID]; already {
		return nil
	}
	connectorNode := intent.GetMeta().GetNodeName()
	l, how, ok := a.registry.Join(peer, connectorNode)
	if !ok {
		a.xnodeMisses.Add(1)
		return nil
	}
	a.xnodeJoins.Add(1)
	if how == clusterpeers.HowServiceVIP {
		a.vipJoins.Add(1)
	}
	return clusterpeers.PeerEdge(intent, l, how, connectorNode)
}

// marshalJSONString returns the JSON-quoted form of s (with escapes). Kept
// separate so callers above can compose a full NDJSON line without building
// an intermediate map/struct.
func marshalJSONString(s string) ([]byte, error) {
	// Single-quoted fast path: an ASCII name with no escapable chars is the
	// common case (agent names are normally DNS labels).
	safe := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c == '"' || c == '\\' || c >= 0x7f {
			safe = false
			break
		}
	}
	if safe {
		out := make([]byte, 0, len(s)+2)
		out = append(out, '"')
		out = append(out, s...)
		out = append(out, '"')
		return out, nil
	}
	return json.Marshal(s)
}

// streamLoop keeps one Subscribe stream open per (agent, stream) and forwards
// envelopes to the merge channel. On error it sleeps cfg.Backoff and
// reconnects, re-supplying the last-saved cursor so the agent's WAL replay
// resumes precisely. Cursor writes go through cfg.Cursors.Save which
// dedupes/debounces fsync storms.
func (a *Aggregator) streamLoop(ctx context.Context, ep AgentEndpoint, stream string) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := a.subscribeOnce(ctx, ep, stream); err != nil && ctx.Err() == nil {
			a.errors.Add(1)
			select {
			case <-ctx.Done():
				return
			case <-time.After(a.cfg.Backoff):
			}
		}
	}
}

func (a *Aggregator) subscribeOnce(ctx context.Context, ep AgentEndpoint, streamName string) error {
	opts := ep.DialOpts
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	cc, err := a.cfg.NewClient(ep.Addr, opts...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", ep.Addr, err)
	}
	defer cc.Close()
	cl := pb.NewEventServiceClient(cc)
	req := &pb.SubscribeRequest{
		ConsumerId: a.cfg.ConsumerID,
		Streams:    []string{streamName},
		Cursor:     a.cfg.Cursors.Load(ep.Name, streamName),
	}
	stream, err := cl.Subscribe(ctx, req)
	if err != nil {
		return fmt.Errorf("subscribe %s/%s: %w", ep.Addr, streamName, err)
	}
	for {
		env, err := stream.Recv()
		if err != nil {
			return err
		}
		a.received.Add(1)
		if c := env.GetCursor(); c != nil {
			a.cfg.Cursors.Save(ep.Name, streamName, c)
		}
		select {
		case a.ch <- envelopeWithAgent{Agent: ep.Name, Envelope: env}:
		default:
			a.dropped.Add(1)
		}
	}
}
