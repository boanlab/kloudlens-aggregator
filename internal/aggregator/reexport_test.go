// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package aggregator

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/envwal"
	pb "github.com/boanlab/kloudlens/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// startReExportGRPC spins up a grpc.Server over bufconn that serves the
// aggregator's ReExportServer. Returns a dialer + cleanup.
func startReExportGRPC(t *testing.T, agg *Aggregator) (func(context.Context, string) (net.Conn, error), func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 16)
	srv := grpc.NewServer()
	pb.RegisterEventServiceServer(srv, NewReExportServer(agg))
	go func() { _ = srv.Serve(lis) }()
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	return dialer, srv.Stop
}

// TestReExportLiveTail asserts that a downstream subscriber with no cursor
// sees envelopes as they arrive on upstream agents, with the cluster cursor
// stamped on each.
func TestReExportLiveTail(t *testing.T) {
	a1 := newFakeAgent(t, "agent-1")
	t.Cleanup(a1.stop)

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
	})
	if err != nil {
		t.Fatal(err)
	}

	aggCtx, aggCancel := context.WithCancel(context.Background())
	aggDone := make(chan struct{})
	go func() { _ = agg.Run(aggCtx); close(aggDone) }()
	t.Cleanup(func() { aggCancel(); <-aggDone })

	reDialer, reStop := startReExportGRPC(t, agg)
	t.Cleanup(reStop)

	// Downstream connects to re-export and subscribes from live tail.
	cc, err := grpc.NewClient("passthrough:bufconn",
		grpc.WithContextDialer(reDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	cl := pb.NewEventServiceClient(cc)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	stream, err := cl.Subscribe(subCtx, &pb.SubscribeRequest{
		ConsumerId: "downstream-1",
		Streams:    []string{"intent"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Give the upstream goroutine time to dial its fakeAgent + our live
	// subscriber to register.
	time.Sleep(200 * time.Millisecond)

	a1.pushIntent("live-1", "FileRead")
	a1.pushIntent("live-2", "FileWrite")

	recv := make(chan *pb.EventEnvelope, 4)
	go func() {
		for {
			env, err := stream.Recv()
			if err != nil {
				return
			}
			recv <- env
		}
	}()

	var got []*pb.EventEnvelope
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case env := <-recv:
			got = append(got, env)
		case <-deadline:
			t.Fatalf("timed out; got %d envelopes, want 2", len(got))
		}
	}

	if got[0].GetIntent().GetIntentId() != "live-1" || got[1].GetIntent().GetIntentId() != "live-2" {
		t.Errorf("order wrong: %q %q", got[0].GetIntent().GetIntentId(), got[1].GetIntent().GetIntentId())
	}
	if c := got[0].GetCursor(); c == nil || c.NodeId != "kloudlens-aggregator" || c.Stream != "intent" || c.Seq != 1 {
		t.Errorf("cluster cursor stamp wrong on #1: %+v", c)
	}
	if c := got[1].GetCursor(); c == nil || c.Seq != 2 {
		t.Errorf("cluster cursor seq not monotonic: %+v", c)
	}
}

// TestReExportResumesFromWAL asserts that a downstream client that reconnects
// with an old cluster cursor sees the missed envelopes replayed from the
// aggregator WAL.
func TestReExportResumesFromWAL(t *testing.T) {
	a1 := newFakeAgent(t, "agent-1")
	t.Cleanup(a1.stop)

	wal, err := envwal.Open(envwal.Options{Dir: filepath.Join(t.TempDir(), "wal")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })

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
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}

	aggCtx, aggCancel := context.WithCancel(context.Background())
	aggDone := make(chan struct{})
	go func() { _ = agg.Run(aggCtx); close(aggDone) }()
	t.Cleanup(func() { aggCancel(); <-aggDone })

	time.Sleep(150 * time.Millisecond)
	a1.pushIntent("pre-1", "FileRead")
	a1.pushIntent("pre-2", "FileRead")
	// Wait for WAL to record both.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && agg.Stats().WALAppends < 2 {
		time.Sleep(20 * time.Millisecond)
	}

	reDialer, reStop := startReExportGRPC(t, agg)
	t.Cleanup(reStop)

	cc, err := grpc.NewClient("passthrough:bufconn",
		grpc.WithContextDialer(reDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	cl := pb.NewEventServiceClient(cc)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	// Subscribe from cursor=0 → expect both WAL-resident envelopes replayed.
	stream, err := cl.Subscribe(subCtx, &pb.SubscribeRequest{
		Streams: []string{"intent"},
		Cursor:  &pb.Cursor{Seq: 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []*pb.EventEnvelope
	recv := make(chan *pb.EventEnvelope, 8)
	go func() {
		for {
			env, err := stream.Recv()
			if err != nil {
				return
			}
			recv <- env
		}
	}()

	dl := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case env := <-recv:
			got = append(got, env)
		case <-dl:
			t.Fatalf("timed out; got %d replayed envelopes", len(got))
		}
	}

	if got[0].GetIntent().GetIntentId() != "pre-1" || got[1].GetIntent().GetIntentId() != "pre-2" {
		t.Errorf("WAL replay order broken: %q %q",
			got[0].GetIntent().GetIntentId(), got[1].GetIntent().GetIntentId())
	}

	// Push a live envelope; the subscriber should see it without a gap.
	a1.pushIntent("live-3", "FileRead")
	select {
	case env := <-recv:
		if env.GetIntent().GetIntentId() != "live-3" {
			t.Errorf("live tail wrong envelope: %q", env.GetIntent().GetIntentId())
		}
		if c := env.GetCursor(); c == nil || c.Seq != 3 {
			t.Errorf("live tail seq: got %+v, want Seq=3", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live tail after WAL replay")
	}
}

// TestReExportCursorExpired asserts that a stale cursor produces
// FailedPrecondition so clients know to restart without a cursor.
func TestReExportCursorExpired(t *testing.T) {
	a1 := newFakeAgent(t, "agent-1")
	t.Cleanup(a1.stop)

	wal, err := envwal.Open(envwal.Options{Dir: filepath.Join(t.TempDir(), "wal")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })

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
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}

	aggCtx, aggCancel := context.WithCancel(context.Background())
	aggDone := make(chan struct{})
	go func() { _ = agg.Run(aggCtx); close(aggDone) }()
	t.Cleanup(func() { aggCancel(); <-aggDone })

	time.Sleep(150 * time.Millisecond)
	a1.pushIntent("x1", "FileRead")
	for agg.Stats().WALAppends < 1 {
		time.Sleep(10 * time.Millisecond)
	}

	// Simulate retention trim: mark the only segment as starting at 100 so
	// seq=1 is considered expired.
	wal.TrimForTest(100)

	reDialer, reStop := startReExportGRPC(t, agg)
	t.Cleanup(reStop)

	cc, err := grpc.NewClient("passthrough:bufconn",
		grpc.WithContextDialer(reDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	cl := pb.NewEventServiceClient(cc)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	stream, err := cl.Subscribe(subCtx, &pb.SubscribeRequest{
		Streams: []string{"intent"},
		Cursor:  &pb.Cursor{Seq: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("want error from Recv on expired cursor, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Errorf("want FailedPrecondition, got %v", err)
	}
}
