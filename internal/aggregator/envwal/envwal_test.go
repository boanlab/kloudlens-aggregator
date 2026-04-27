// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

package envwal

import (
	"path/filepath"
	"testing"
	"time"

	pb "github.com/boanlab/kloudlens/protobuf"
)

func mkIntent(id string) *pb.EventEnvelope {
	return &pb.EventEnvelope{
		Payload: &pb.EventEnvelope_Intent{
			Intent: &pb.IntentEvent{IntentId: id, Kind: "FileRead"},
		},
	}
}

func TestWALAppendReadFrom(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	for i, id := range []string{"e1", "e2", "e3"} {
		seq, err := w.Append("agent-a", mkIntent(id))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Errorf("append seq=%d, want %d", seq, i+1)
		}
	}

	var got []Entry
	if err := w.ReadFrom(0, func(e Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("readfrom: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(got)=%d, want 3", len(got))
	}
	if got[0].Envelope.GetIntent().IntentId != "e1" || got[2].Envelope.GetIntent().IntentId != "e3" {
		t.Errorf("envelope roundtrip broken: %+v", got)
	}
	if got[1].Agent != "agent-a" {
		t.Errorf("agent tag lost: %q", got[1].Agent)
	}

	// Cursor > 0: skip already-delivered entries.
	got = got[:0]
	if err := w.ReadFrom(2, func(e Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("readfrom(2): %v", err)
	}
	if len(got) != 1 || got[0].Seq != 3 {
		t.Fatalf("readfrom(2) got %+v, want one entry with seq=3", got)
	}
}

func TestWALResumesAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append("a1", mkIntent("x1")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append("a1", mkIntent("x2")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if w2.LastSeq() != 2 {
		t.Fatalf("LastSeq after reopen = %d, want 2", w2.LastSeq())
	}
	seq, err := w2.Append("a1", mkIntent("x3"))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 3 {
		t.Errorf("next seq after reopen = %d, want 3", seq)
	}

	var ids []string
	if err := w2.ReadFrom(0, func(e Entry) error {
		ids = append(ids, e.Envelope.GetIntent().IntentId)
		return nil
	}); err != nil {
		t.Fatalf("readfrom: %v", err)
	}
	if len(ids) != 3 || ids[0] != "x1" || ids[2] != "x3" {
		t.Errorf("resume order broken: %+v", ids)
	}
}

func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// A very small segment size forces rotation after the first write.
	w, err := Open(Options{Dir: dir, SegmentSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 5; i++ {
		if _, err := w.Append("a", mkIntent("e")); err != nil {
			t.Fatal(err)
		}
	}
	matches, err := filepath.Glob(filepath.Join(dir, "aggwal-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 2 {
		t.Errorf("expected segment rotation; got %d segments", len(matches))
	}
}

func TestWALGCByTTL(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentSize: 64, TTL: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 5; i++ {
		if _, err := w.Append("a", mkIntent("e")); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(5 * time.Millisecond)
	if err := w.GC(); err != nil {
		t.Fatalf("gc: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "aggwal-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	// TTL was nanoseconds — all non-current segments should be gone, leaving
	// at most the current (still-open) segment.
	if len(matches) > 1 {
		t.Errorf("ttl-gc left %d segments, want ≤1", len(matches))
	}
}

func TestWALCursorExpired(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Append("a", mkIntent("e1")); err != nil {
		t.Fatal(err)
	}
	w.TrimForTest(50)
	err = w.ReadFrom(1, func(Entry) error { return nil })
	if err != ErrCursorExpired {
		t.Fatalf("want ErrCursorExpired, got %v", err)
	}
}
