// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package clusterpeers

import (
	"strconv"

	pb "github.com/boanlab/kloudlens/protobuf"
)

// Wire contract constants (LOCKED — see CROSS_NODE_DESIGN.md).
const (
	KindListenerAdvertise = "ListenerAdvertise"
	KindListenerWithdraw  = "ListenerWithdraw"
	KindNetworkExchange   = "NetworkExchange"
	KindClusterPeerEdge   = "ClusterPeerEdge"

	// AttrPeer is the connect destination "ip:port" on a NetworkExchange.
	AttrPeer = "peer"
	// AttrPeerPID, when already present on a NetworkExchange, marks it as
	// same-node-attributed by the kernel; the aggregator then skips the join.
	AttrPeerPID = "peer_pid"
)

// ListenerFromAdvertise builds a Listener from a ListenerAdvertise IntentEvent.
// It returns ok=false when the event is not a well-formed advertise (wrong kind
// or empty addr), so a caller can safely feed every intent through it.
func ListenerFromAdvertise(ev *pb.IntentEvent) (Listener, bool) {
	if ev == nil || ev.GetKind() != KindListenerAdvertise {
		return Listener{}, false
	}
	attrs := ev.GetAttributes()
	addr := attrs["addr"]
	if addr == "" {
		return Listener{}, false
	}
	m := ev.GetMeta()
	return Listener{
		Addr:        addr,
		Port:        attrs["port"],
		PID:         attrs["pid"],
		Process:     attrs["process"],
		Wildcard:    attrs["wildcard"] == "true",
		NodeName:    m.GetNodeName(),
		Namespace:   m.GetNamespace(),
		Pod:         m.GetPod(),
		Container:   m.GetContainer(),
		ContainerID: m.GetContainerId(),
		Image:       m.GetImage(),
	}, true
}

// ListenerFromWithdraw builds the identity of a listener to retire from a
// ListenerWithdraw IntentEvent. Only the fields Registry.Remove keys on are
// populated — Addr for an exact entry, and (Port, Namespace, Pod) for a
// wildcard entry indexed per pod. Returns ok=false for a malformed event.
func ListenerFromWithdraw(ev *pb.IntentEvent) (Listener, bool) {
	if ev == nil || ev.GetKind() != KindListenerWithdraw {
		return Listener{}, false
	}
	attrs := ev.GetAttributes()
	addr := attrs["addr"]
	if addr == "" {
		return Listener{}, false
	}
	m := ev.GetMeta()
	return Listener{
		Addr:      addr,
		Port:      attrs["port"],
		Wildcard:  attrs["wildcard"] == "true",
		Namespace: m.GetNamespace(),
		Pod:       m.GetPod(),
	}, true
}

// PeerEdge builds a ClusterPeerEdge IntentEvent from a resolved cross-node (or
// same-node redundant-confirm) join. src is the connecting side's NetworkExchange
// intent: its Meta and ContributingEventIds are carried onto the edge. connectorNode
// is the connecting side's node; when it equals the listener's node the edge is
// marked "same-node" (the kernel already attributed it), else "cross-node".
func PeerEdge(src *pb.IntentEvent, l Listener, how How, connectorNode string) *pb.IntentEvent {
	attribution := "cross-node"
	if connectorNode != "" && l.NodeName == connectorNode {
		attribution = "same-node"
	}
	attrs := map[string]string{
		"addr":           src.GetAttributes()[AttrPeer],
		"peer_pid":       l.PID,
		"peer_process":   l.Process,
		"peer_pod":       l.Pod,
		"peer_namespace": l.Namespace,
		"peer_container": l.Container,
		"peer_image":     l.Image,
		"peer_node":      l.NodeName,
		"attribution":    attribution,
		"via":            how.Via(),
	}
	// Service-VIP resolution carries the backing Service and, when the flow could
	// have been DNATed to any of several replicas, marks the specific replica
	// (peer_pod/peer_node) ambiguous. peer_process/peer_image stay correct: the
	// replicas run one workload, which is exactly what the paper attributes.
	if svc := l.Service.String(); svc != "" {
		attrs["peer_service"] = svc
	}
	if l.ReplicaAmbiguous {
		attrs["peer_replica_ambiguous"] = "true"
		attrs["peer_replica_count"] = strconv.Itoa(l.ReplicaCount)
	}
	return &pb.IntentEvent{
		Kind:                 KindClusterPeerEdge,
		Attributes:           attrs,
		Meta:                 src.GetMeta(),
		ContributingEventIds: src.GetContributingEventIds(),
		Severity:             src.GetSeverity(),
		Confidence:           1.0,
	}
}
