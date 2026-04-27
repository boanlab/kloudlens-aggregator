// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/envwal"
	pb "github.com/boanlab/kloudlens/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// stubEventService is a minimal pb.EventServiceServer. Tests push envelopes
// onto `events` and the server forwards them to any live Subscribe call. We
// deliberately avoid the real SubscribeServer + WAL path here so the fan-in
// test isn't coupled to WAL internals (a pre-existing race between
// WAL.Append and WAL.ReadFrom would otherwise trip the race detector).
type stubEventService struct {
	pb.UnimplementedEventServiceServer
	events chan *pb.EventEnvelope
}

func newStubEventService() *stubEventService {
	return &stubEventService{events: make(chan *pb.EventEnvelope, 64)}
}

func (s *stubEventService) Subscribe(_ *pb.SubscribeRequest, stream pb.EventService_SubscribeServer) error {
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case env, ok := <-s.events:
			if !ok {
				return nil
			}
			if err := stream.Send(env); err != nil {
				return err
			}
		}
	}
}

// fakeAgent wraps a stubEventService on a bufconn listener. Handing the
// bufconn contextDialer back through DialOpts lets the aggregator exercise
// its full client path (gRPC dial + Subscribe + envelope receive) without
// opening a TCP socket.
type fakeAgent struct {
	name   string
	svc    *stubEventService
	stop   func()
	dialer func(context.Context, string) (net.Conn, error)
}

func newFakeAgent(t *testing.T, name string) *fakeAgent {
	t.Helper()
	svc := newStubEventService()
	lis := bufconn.Listen(1 << 16)
	grpcSrv := grpc.NewServer()
	pb.RegisterEventServiceServer(grpcSrv, svc)
	go func() { _ = grpcSrv.Serve(lis) }()
	return &fakeAgent{
		name: name,
		svc:  svc,
		stop: grpcSrv.Stop,
		dialer: func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		},
	}
}

func (f *fakeAgent) pushIntent(id, kind string) {
	f.svc.events <- &pb.EventEnvelope{
		Payload: &pb.EventEnvelope_Intent{
			Intent: &pb.IntentEvent{IntentId: id, Kind: kind},
		},
	}
}

// TestAggregatorFanInFromTwoAgents asserts that envelopes submitted through
// two different agents all land in the aggregator's NDJSON output, tagged
// with the agent of origin.
func TestAggregatorFanInFromTwoAgents(t *testing.T) {
	a1 := newFakeAgent(t, "agent-1")
	a2 := newFakeAgent(t, "agent-2")
	t.Cleanup(func() {
		a1.stop()
		a2.stop()
	})

	var buf safeBuf
	agg, err := New(Config{
		Agents: []AgentEndpoint{
			{
				Name: "agent-1", Addr: "passthrough:bufconn",
				DialOpts: []grpc.DialOption{
					grpc.WithContextDialer(a1.dialer),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				},
			},
			{
				Name: "agent-2", Addr: "passthrough:bufconn",
				DialOpts: []grpc.DialOption{
					grpc.WithContextDialer(a2.dialer),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				},
			},
		},
		Out:     &buf,
		Streams: []string{"intent"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = agg.Run(ctx)
		close(done)
	}()

	// Give each agent goroutine time to dial and have its Subscribe call
	// land on the server before we push envelopes.
	time.Sleep(150 * time.Millisecond)

	a1.pushIntent("a1-e1", "FileRead")
	a2.pushIntent("a2-e1", "FileWrite")
	a1.pushIntent("a1-e2", "NetworkExchange")

	// Wait for 3 encoded lines. 2s is generous — bufconn delivery is sub-ms.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && agg.Stats().Written < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if got := agg.Stats().Written; got != 3 {
		t.Fatalf("written=%d, want 3 (stats=%+v)\n--- out ---\n%s", got, agg.Stats(), buf.String())
	}

	// Parse NDJSON and check each envelope carries the correct _agent tag.
	type line struct {
		Agent    string `json:"_agent"`
		Envelope struct {
			Intent struct {
				IntentID string `json:"intentId"`
			} `json:"intent"`
		} `json:"envelope"`
	}
	var lines []line
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for dec.More() {
		var l line
		if err := dec.Decode(&l); err != nil {
			t.Fatalf("decode ndjson: %v\n%s", err, buf.String())
		}
		lines = append(lines, l)
	}
	if len(lines) != 3 {
		t.Fatalf("got %d decoded lines, want 3: %s", len(lines), buf.String())
	}
	seen := map[string]string{}
	for _, l := range lines {
		seen[l.Envelope.Intent.IntentID] = l.Agent
	}
	if seen["a1-e1"] != "agent-1" || seen["a2-e1"] != "agent-2" || seen["a1-e2"] != "agent-1" {
		t.Errorf("agent tagging wrong: %+v\nraw=%s", seen, buf.String())
	}
}

// TestAggregatorAppendsToWAL asserts that when Config.WAL is set, every
// envelope pulled off an agent lands in the aggregator WAL in order, with
// the correct agent tag and a freshly-assigned cluster seq.
func TestAggregatorAppendsToWAL(t *testing.T) {
	a1 := newFakeAgent(t, "agent-1")
	t.Cleanup(a1.stop)

	dir := t.TempDir()
	w, err := envwal.Open(envwal.Options{Dir: filepath.Join(dir, "wal")})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	var buf safeBuf
	agg, err := New(Config{
		Agents: []AgentEndpoint{{
			Name: "agent-1", Addr: "passthrough:bufconn",
			DialOpts: []grpc.DialOption{
				grpc.WithContextDialer(a1.dialer),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		}},
		Out:     &buf,
		Streams: []string{"intent"},
		WAL:     w,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = agg.Run(ctx); close(done) }()

	time.Sleep(150 * time.Millisecond)
	a1.pushIntent("w1", "FileRead")
	a1.pushIntent("w2", "FileWrite")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && agg.Stats().WALAppends < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	s := agg.Stats()
	if s.WALAppends != 2 {
		t.Fatalf("WALAppends=%d, want 2 (stats=%+v)", s.WALAppends, s)
	}
	if s.WALErrors != 0 {
		t.Errorf("WALErrors=%d, want 0", s.WALErrors)
	}
	if s.WALLastSeq != 2 {
		t.Errorf("WALLastSeq=%d, want 2", s.WALLastSeq)
	}

	var gotIDs []string
	var gotAgents []string
	if err := w.ReadFrom(0, func(e envwal.Entry) error {
		gotIDs = append(gotIDs, e.Envelope.GetIntent().IntentId)
		gotAgents = append(gotAgents, e.Agent)
		return nil
	}); err != nil {
		t.Fatalf("readfrom: %v", err)
	}
	if len(gotIDs) != 2 || gotIDs[0] != "w1" || gotIDs[1] != "w2" {
		t.Errorf("WAL content wrong: %+v", gotIDs)
	}
	if gotAgents[0] != "agent-1" || gotAgents[1] != "agent-1" {
		t.Errorf("agent tags wrong: %+v", gotAgents)
	}
}

func TestAggregatorRequiresAgents(t *testing.T) {
	_, err := New(Config{Out: &bytes.Buffer{}})
	if err == nil {
		t.Fatal("expected error when no agents configured")
	}
}

func TestAggregatorRequiresOutput(t *testing.T) {
	_, err := New(Config{Agents: []AgentEndpoint{{Name: "x", Addr: "localhost:9999"}}})
	if err == nil {
		t.Fatal("expected error when no output writer")
	}
}

// safeBuf serializes access to a bytes.Buffer — the aggregator's writer
// goroutine writes concurrently with the test reading the buffer, so a
// bare bytes.Buffer would race.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuf) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, b.buf.Len())
	copy(out, b.buf.Bytes())
	return out
}

func (b *safeBuf) String() string {
	return string(b.Bytes())
}
