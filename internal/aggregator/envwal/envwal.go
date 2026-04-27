// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Package envwal is the cluster-aggregator's write-ahead log of merged
// pb.EventEnvelope records. It mirrors the on-node agent WAL design —
// JSONL segments, size+TTL retention, ReadFrom(seq) resume — but stores
// merged envelopes tagged with the originating agent rather than the
// node-local IntentEvent shape.
//
// The aggregator's re-export gRPC surface serves downstream subscribers
// by replaying this log from a cursor so SIEM / tier-2 consumers survive
// aggregator restarts without losing events.
package envwal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	pb "github.com/boanlab/kloudlens/protobuf"

	"google.golang.org/protobuf/encoding/protojson"
)

// Entry is one record in the aggregator WAL. Seq is cluster-assigned (a
// dense counter local to this WAL instance) and is distinct from the
// per-agent per-stream Seq inside pb.EventEnvelope.Cursor.
type Entry struct {
	Seq      uint64
	Agent    string
	Envelope *pb.EventEnvelope
	TS       int64
}

// Options configures the WAL. Defaults are chosen to match the on-node
// WAL so operator muscle memory transfers.
type Options struct {
	Dir         string
	MaxBytes    int64         // soft cap; oldest segments trimmed on overflow
	SegmentSize int64         // rotate current segment at this size
	TTL         time.Duration // remove segments older than this
}

// WAL is an append-only log of merged envelopes. Safe for one Append caller
// and many ReadFrom callers concurrently.
type WAL struct {
	opts Options

	mu       sync.Mutex
	seq      uint64
	segments []*segment
	cur      *segment

	overflowTrims uint64
}

type segment struct {
	path      string
	startSeq  uint64
	endSeq    uint64
	bytes     int64
	createdAt int64
	f         *os.File
}

// diskEntry is the on-disk JSONL shape. Envelope is kept as raw protojson so
// readers can re-emit it to gRPC without a lossy round-trip through an ad-hoc
// Go struct.
type diskEntry struct {
	Seq      uint64          `json:"seq"`
	Agent    string          `json:"agent"`
	Envelope json.RawMessage `json:"envelope"`
	TS       int64           `json:"ts_ns"`
}

// ErrCursorExpired means the caller's resume seq is older than the oldest
// segment retained. Re-export serves this as gRPC FailedPrecondition in 1-E/3.
var ErrCursorExpired = errors.New("envwal: cursor expired")

// Open creates or resumes a WAL in opts.Dir.
func Open(opts Options) (*WAL, error) {
	if opts.Dir == "" {
		return nil, errors.New("envwal: empty dir")
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 32 << 20
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 2 << 30
	}
	if opts.TTL <= 0 {
		opts.TTL = 2 * time.Hour
	}
	if err := os.MkdirAll(opts.Dir, 0o750); err != nil {
		return nil, err
	}
	w := &WAL{opts: opts}
	if err := w.reload(); err != nil {
		return nil, err
	}
	if len(w.segments) > 0 {
		w.seq = w.segments[len(w.segments)-1].endSeq
	}
	if err := w.openSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

// Append writes an envelope with a freshly-assigned cluster seq and returns
// that seq. Rotates the active segment when it crosses SegmentSize.
func (w *WAL) Append(agent string, env *pb.EventEnvelope) (uint64, error) {
	if env == nil {
		return 0, errors.New("envwal: nil envelope")
	}
	envBytes, err := protojson.Marshal(env)
	if err != nil {
		return 0, fmt.Errorf("envwal: marshal envelope: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	line, err := json.Marshal(diskEntry{
		Seq:      w.seq,
		Agent:    agent,
		Envelope: envBytes,
		TS:       time.Now().UnixNano(),
	})
	if err != nil {
		return 0, err
	}
	if _, err := w.cur.f.Write(line); err != nil {
		return 0, err
	}
	if _, err := w.cur.f.Write([]byte("\n")); err != nil {
		return 0, err
	}
	w.cur.bytes += int64(len(line)) + 1
	w.cur.endSeq = w.seq
	if w.cur.bytes >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	return w.seq, nil
}

// LastSeq returns the most recently assigned cluster seq (0 if empty).
func (w *WAL) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// OverflowCount returns the cumulative segment trims induced by MaxBytes.
// Surfaced on /stats as kloudlens_aggwal_overflow_total.
func (w *WAL) OverflowCount() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overflowTrims
}

// ReadFrom streams entries with Seq > fromSeq through cb. If fromSeq is
// older than the retained window ErrCursorExpired is returned.
func (w *WAL) ReadFrom(fromSeq uint64, cb func(Entry) error) error {
	type segView struct {
		path   string
		endSeq uint64
	}
	w.mu.Lock()
	if len(w.segments) == 0 {
		w.mu.Unlock()
		return nil
	}
	oldest := w.segments[0].startSeq
	views := make([]segView, len(w.segments))
	for i, s := range w.segments {
		views[i] = segView{path: s.path, endSeq: s.endSeq}
	}
	w.mu.Unlock()
	if fromSeq > 0 && fromSeq+1 < oldest {
		return ErrCursorExpired
	}
	for _, s := range views {
		if s.endSeq <= fromSeq {
			continue
		}
		f, err := os.Open(s.path)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(f)
		for dec.More() {
			var d diskEntry
			if err := dec.Decode(&d); err != nil {
				_ = f.Close()
				return err
			}
			if d.Seq <= fromSeq {
				continue
			}
			env := &pb.EventEnvelope{}
			if err := protojson.Unmarshal(d.Envelope, env); err != nil {
				_ = f.Close()
				return fmt.Errorf("envwal: unmarshal seq=%d: %w", d.Seq, err)
			}
			if err := cb(Entry{Seq: d.Seq, Agent: d.Agent, Envelope: env, TS: d.TS}); err != nil {
				_ = f.Close()
				return err
			}
		}
		_ = f.Close()
	}
	return nil
}

// GC trims segments older than TTL or beyond MaxBytes. The current segment
// is never trimmed. Callers should invoke this from a janitor goroutine.
func (w *WAL) GC() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now().UnixNano()
	ttl := int64(w.opts.TTL)

	keep := w.segments[:0]
	for _, s := range w.segments {
		if s == w.cur {
			keep = append(keep, s)
			continue
		}
		if now-s.createdAt > ttl {
			_ = os.Remove(s.path)
			continue
		}
		keep = append(keep, s)
	}
	w.segments = keep

	var total int64
	for _, s := range w.segments {
		total += s.bytes
	}
	for total > w.opts.MaxBytes && len(w.segments) > 1 {
		victim := w.segments[0]
		if victim == w.cur {
			break
		}
		_ = os.Remove(victim.path)
		total -= victim.bytes
		w.segments = w.segments[1:]
		w.overflowTrims++
	}
	return nil
}

// Close flushes and closes the active segment file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur != nil && w.cur.f != nil {
		err := w.cur.f.Close()
		w.cur.f = nil
		return err
	}
	return nil
}

func (w *WAL) rotate() error {
	if w.cur != nil && w.cur.f != nil {
		_ = w.cur.f.Close()
		w.cur.f = nil
	}
	return w.openSegment()
}

func (w *WAL) openSegment() error {
	name := fmt.Sprintf("aggwal-%020d.jsonl", w.seq+1)
	p := filepath.Join(w.opts.Dir, name)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- p is derived from opts.Dir (operator-configured) + a segment name we just generated
	if err != nil {
		return err
	}
	w.cur = &segment{
		path:      p,
		startSeq:  w.seq + 1,
		endSeq:    w.seq,
		createdAt: time.Now().UnixNano(),
		f:         f,
	}
	w.segments = append(w.segments, w.cur)
	return nil
}

// TrimForTest bumps the oldest segment's startSeq so ReadFrom(fromSeq<x)
// returns ErrCursorExpired without actually deleting files. Test-only.
func (w *WAL) TrimForTest(startSeq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.segments) > 0 {
		w.segments[0].startSeq = startSeq
	}
}

// reload scans opts.Dir for existing aggwal-*.jsonl segments and parses their
// seq range so subsequent Appends continue the sequence.
func (w *WAL) reload() error {
	entries, err := os.ReadDir(w.opts.Dir)
	if err != nil {
		return err
	}
	var segs []*segment
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) < 8 || name[:7] != "aggwal-" {
			continue
		}
		full := filepath.Join(w.opts.Dir, name)
		info, err := os.Stat(full)
		if err != nil {
			continue
		}
		s := &segment{
			path:      full,
			bytes:     info.Size(),
			createdAt: info.ModTime().UnixNano(),
		}
		f, err := os.Open(full) // #nosec G304 -- full is derived from opts.Dir (operator-configured) + a glob-matched segment name we just generated
		if err != nil {
			continue
		}
		dec := json.NewDecoder(f)
		first := true
		for dec.More() {
			var d diskEntry
			if err := dec.Decode(&d); err != nil {
				break
			}
			if first {
				s.startSeq = d.Seq
				first = false
			}
			s.endSeq = d.Seq
		}
		_ = f.Close()
		if !first {
			segs = append(segs, s)
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].startSeq < segs[j].startSeq })
	w.segments = segs
	return nil
}
