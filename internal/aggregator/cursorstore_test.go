// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package aggregator

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/boanlab/kloudlens/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestFileCursorStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cursors.json")
	s, err := NewFileCursorStore(path, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileCursorStore: %v", err)
	}

	s.Save("agent-a", "intent", &pb.Cursor{NodeId: "node-a", Stream: "intent", Seq: 42})
	s.Save("agent-a", "deviation", &pb.Cursor{NodeId: "node-a", Stream: "deviation", Seq: 7})
	s.Save("agent-b", "intent", &pb.Cursor{NodeId: "node-b", Stream: "intent", Seq: 100})
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify persistence.
	s2, err := NewFileCursorStore(path, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if c := s2.Load("agent-a", "intent"); c == nil || c.Seq != 42 || c.NodeId != "node-a" {
		t.Errorf("load agent-a/intent: %+v", c)
	}
	if c := s2.Load("agent-a", "deviation"); c == nil || c.Seq != 7 {
		t.Errorf("load agent-a/deviation: %+v", c)
	}
	if c := s2.Load("agent-b", "intent"); c == nil || c.Seq != 100 {
		t.Errorf("load agent-b/intent: %+v", c)
	}
	if c := s2.Load("agent-b", "deviation"); c != nil {
		t.Errorf("load missing pair should be nil, got %+v", c)
	}
}

func TestFileCursorStoreMonotonic(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileCursorStore(filepath.Join(dir, "cursors.json"), 20*time.Millisecond)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	s.Save("a", "s", &pb.Cursor{NodeId: "n", Stream: "s", Seq: 50})
	// A rewind to a lower seq from the same node should be rejected — it
	// would cause us to re-request envelopes we've already fanned out.
	s.Save("a", "s", &pb.Cursor{NodeId: "n", Stream: "s", Seq: 10})
	if c := s.Load("a", "s"); c == nil || c.Seq != 50 {
		t.Errorf("monotonic guard breached: %+v", c)
	}
	// But a node-id change IS accepted — it means the agent was reinstalled
	// and WAL seqs restart from 1.
	s.Save("a", "s", &pb.Cursor{NodeId: "n2", Stream: "s", Seq: 1})
	if c := s.Load("a", "s"); c == nil || c.NodeId != "n2" || c.Seq != 1 {
		t.Errorf("node-id swap not accepted: %+v", c)
	}
}

// TestAggregatorReplaysCursorAcrossReconnect asserts that after the stream
// drops, the aggregator reconnects with SubscribeRequest.Cursor set to the
// last envelope it saw. Without this, reconnecting with cursor=nil would
// replay the agent's full buffer and duplicate every replayed event.
func TestAggregatorReplaysCursorAcrossReconnect(t *testing.T) {
	svc := newRecordingAgent()
	lis := bufconn.Listen(1 << 16)
	grpcSrv := grpc.NewServer()
	pb.RegisterEventServiceServer(grpcSrv, svc)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	dir := t.TempDir()
	store, err := NewFileCursorStore(filepath.Join(dir, "cursors.json"), 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var buf safeBuf
	agg, err := New(Config{
		Agents: []AgentEndpoint{{
			Name: "a1", Addr: "passthrough:bufconn",
			DialOpts: []grpc.DialOption{
				grpc.WithContextDialer(dialer),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		}},
		Out:     &buf,
		Streams: []string{"intent"},
		Cursors: store,
		Backoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = agg.Run(ctx); close(done) }()

	// Wait for the first Subscribe, then push two envelopes and kill the stream.
	waitForRequests(t, svc, 1)
	svc.push(&pb.EventEnvelope{
		Cursor:  &pb.Cursor{NodeId: "n1", Stream: "intent", Seq: 1},
		Payload: &pb.EventEnvelope_Intent{Intent: &pb.IntentEvent{IntentId: "e1", Kind: "FileRead"}},
	})
	svc.push(&pb.EventEnvelope{
		Cursor:  &pb.Cursor{NodeId: "n1", Stream: "intent", Seq: 2},
		Payload: &pb.EventEnvelope_Intent{Intent: &pb.IntentEvent{IntentId: "e2", Kind: "FileRead"}},
	})
	// Wait for the writer to consume both.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && agg.Stats().Written < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	svc.breakStream()
	waitForRequests(t, svc, 2)
	cancel()
	<-done

	reqs := svc.requests()
	if len(reqs) < 2 {
		t.Fatalf("expected ≥2 Subscribe requests after reconnect, got %d", len(reqs))
	}
	second := reqs[1]
	if c := second.GetCursor(); c == nil || c.Seq != 2 {
		t.Errorf("reconnect cursor: want Seq=2, got %+v", c)
	}
}

// recordingAgent is a pb.EventServiceServer that records every Subscribe
// request and can drop the active stream on demand. Used by the cursor
// replay test above.
type recordingAgent struct {
	pb.UnimplementedEventServiceServer
	mu      sync.Mutex
	reqs    []*pb.SubscribeRequest
	events  chan *pb.EventEnvelope
	breakCh atomic.Pointer[chan struct{}]
}

func newRecordingAgent() *recordingAgent {
	a := &recordingAgent{events: make(chan *pb.EventEnvelope, 64)}
	ch := make(chan struct{})
	a.breakCh.Store(&ch)
	return a
}

func (a *recordingAgent) push(e *pb.EventEnvelope) { a.events <- e }

func (a *recordingAgent) breakStream() {
	oldPtr := a.breakCh.Load()
	newCh := make(chan struct{})
	a.breakCh.Store(&newCh)
	close(*oldPtr)
}

func (a *recordingAgent) requests() []*pb.SubscribeRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]*pb.SubscribeRequest, len(a.reqs))
	copy(out, a.reqs)
	return out
}

func (a *recordingAgent) Subscribe(req *pb.SubscribeRequest, stream pb.EventService_SubscribeServer) error {
	a.mu.Lock()
	a.reqs = append(a.reqs, req)
	a.mu.Unlock()
	breakCh := *a.breakCh.Load()
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-breakCh:
			return nil // clean EOF → client treats as retryable
		case env := <-a.events:
			if err := stream.Send(env); err != nil {
				return err
			}
		}
	}
}

func waitForRequests(t *testing.T, a *recordingAgent, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(a.requests()) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d Subscribe requests, saw %d", n, len(a.requests()))
}
