// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package aggregator

import (
	"errors"

	"github.com/boanlab/kloudlens-aggregator/internal/aggregator/envwal"
	pb "github.com/boanlab/kloudlens/protobuf"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReExportServer implements pb.EventServiceServer on the aggregator, letting
// downstream consumers (SIEM, tier-2 sinks) subscribe to the merged stream
// of envelopes from every agent.
//
// Resume semantics: clients pass pb.SubscribeRequest.Cursor.Seq = the
// aggregator-assigned cluster seq they last observed. The server replays
// any envelopes still in the aggregator WAL above that seq, then transitions
// to the live broadcast channel. If the requested seq is older than the
// WAL retention window, FailedPrecondition is returned and the client
// should restart from the live tail (cursor=nil) or from whatever durable
// copy it already has. This is the same policy as the on-node agent WAL.
//
// Cursor rewrite: the outgoing EventEnvelope.Cursor is set to
// {NodeId: AggregatorID, Stream: <as-subscribed>, Seq: clusterSeq} so
// downstream clients have a single resume key. The original agent-side
// cursor is not preserved on the wire.
type ReExportServer struct {
	pb.UnimplementedEventServiceServer

	agg *Aggregator
	// AggregatorID is stamped into every re-exported envelope's Cursor.NodeId
	// so downstream consumers can distinguish cluster instances in a
	// federation. Defaults to "kloudlens-aggregator" when empty.
	AggregatorID string

	// SubscriberBuffer is the per-subscriber channel size. 0 → 1024.
	SubscriberBuffer int
}

// NewReExportServer wires an Aggregator into the pb.EventServiceServer
// interface. The returned server is safe to Register on a grpc.Server.
func NewReExportServer(agg *Aggregator) *ReExportServer {
	return &ReExportServer{
		agg:              agg,
		AggregatorID:     "kloudlens-aggregator",
		SubscriberBuffer: 1024,
	}
}

// Subscribe serves one downstream consumer. Replays WAL from the requested
// cursor and then streams live broadcasts until the client disconnects or
// the aggregator shuts down.
func (r *ReExportServer) Subscribe(req *pb.SubscribeRequest, stream pb.EventService_SubscribeServer) error {
	if r.agg == nil {
		return status.Error(codes.Internal, "reexport: aggregator not wired")
	}

	// Register the live subscriber BEFORE touching the WAL. That way, any
	// envelopes appended after we snapshot WAL.LastSeq() land in both the
	// WAL (picked up by ReadFrom) and the live channel — the consumer loop
	// below de-duplicates by comparing against lastSentSeq.
	sub := r.agg.registerSubscriber(r.SubscriberBuffer)
	defer r.agg.unregisterSubscriber(sub)

	streamName := ""
	if len(req.GetStreams()) > 0 {
		// Re-export supports only a single stream per Subscribe (matches the
		// on-node EventService semantics). Additional streams require
		// separate Subscribe calls — cursor keys are per-stream anyway.
		streamName = req.GetStreams()[0]
	}

	var fromSeq uint64
	if c := req.GetCursor(); c != nil {
		fromSeq = c.GetSeq()
	}

	var lastSentSeq uint64
	if r.agg.cfg.WAL != nil {
		if err := r.replayWAL(stream, r.agg.cfg.WAL, fromSeq, streamName, &lastSentSeq); err != nil {
			return err
		}
	}

	// Live tail. Skip any seq already delivered by replayWAL (possible when
	// an envelope is appended between "register sub" and "ReadFrom snapshot").
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-sub.ch:
			if !ok {
				// Aggregator shutting down.
				return nil
			}
			if ev.Seq <= lastSentSeq {
				continue
			}
			if !streamMatches(streamName, ev.Envelope) {
				continue
			}
			out := r.stampCursor(ev.Envelope, ev.Seq, streamName)
			if err := stream.Send(out); err != nil {
				return err
			}
			lastSentSeq = ev.Seq
		}
	}
}

func (r *ReExportServer) replayWAL(stream pb.EventService_SubscribeServer, wal *envwal.WAL, fromSeq uint64, streamName string, lastSent *uint64) error {
	err := wal.ReadFrom(fromSeq, func(e envwal.Entry) error {
		if !streamMatches(streamName, e.Envelope) {
			return nil
		}
		out := r.stampCursor(e.Envelope, e.Seq, streamName)
		if err := stream.Send(out); err != nil {
			return err
		}
		*lastSent = e.Seq
		return nil
	})
	if errors.Is(err, envwal.ErrCursorExpired) {
		return status.Errorf(codes.FailedPrecondition,
			"reexport: cursor seq=%d older than WAL retention; client should restart from live tail", fromSeq)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "reexport: wal replay: %v", err)
	}
	return nil
}

// stampCursor overwrites the envelope's Cursor with the cluster cursor so
// downstream clients have a single resume key. Uses proto.Clone because
// shallow-copying pb.EventEnvelope would clone its internal sync.Mutex and
// because concurrent subscribers must not stomp on each other's Cursor.
func (r *ReExportServer) stampCursor(env *pb.EventEnvelope, seq uint64, streamName string) *pb.EventEnvelope {
	out := proto.Clone(env).(*pb.EventEnvelope)
	out.Cursor = &pb.Cursor{
		NodeId: r.AggregatorID,
		Stream: streamName,
		Seq:    seq,
	}
	return out
}

func streamMatches(want string, env *pb.EventEnvelope) bool {
	if want == "" {
		return true
	}
	switch env.GetPayload().(type) {
	case *pb.EventEnvelope_Intent:
		return want == "intent"
	case *pb.EventEnvelope_Deviation:
		return want == "deviation"
	case *pb.EventEnvelope_GraphEdge:
		return want == "graph_edge"
	case *pb.EventEnvelope_Lifecycle:
		return want == "lifecycle"
	case *pb.EventEnvelope_Audit:
		return want == "audit"
	}
	return false
}

var _ pb.EventServiceServer = (*ReExportServer)(nil)
