// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 BoanLab @ DKU

// Per (agent, stream) cursor persistence. The aggregator subscribes to
// each agent with pb.SubscribeRequest.Cursor set to the last envelope it
// saw; the agent's WAL replay then resumes from there instead of
// re-sending envelopes already fanned into the aggregator's own WAL.
// Without this, an aggregator restart replays the agent's full in-memory
// buffer and either duplicates events downstream or silently loses
// events emitted during the restart window.
//
// Scale: O(agents × streams) entries, each ~50 bytes. A flat JSON file
// is plenty — bbolt would be overkill. Writes are debounced so a 50k
// evt/s stream doesn't trigger 50k fsyncs.

package aggregator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/boanlab/kloudlens/protobuf"
)

// CursorStore tracks last-seen cursors keyed by (agent, stream). Callers
// write through Save; the store persists on its own cadence (timer + Close).
type CursorStore interface {
	Load(agent, stream string) *pb.Cursor
	Save(agent, stream string, c *pb.Cursor)
	Flush() error
	Close() error
}

// nullStore is the default when no cursor file is configured: every
// reconnect restarts from nil.
type nullStore struct{}

func (nullStore) Load(string, string) *pb.Cursor  { return nil }
func (nullStore) Save(string, string, *pb.Cursor) {}
func (nullStore) Flush() error                    { return nil }
func (nullStore) Close() error                    { return nil }

// NullCursorStore returns a CursorStore that never persists. Used by tests
// and by production when the operator opts out of cursor persistence.
func NullCursorStore() CursorStore { return nullStore{} }

// persistedCursor is the on-disk shape. We store node_id/stream/seq
// explicitly rather than reusing protojson — this file will be read by
// humans during incidents and the field names should be stable/obvious.
type persistedCursor struct {
	NodeID string `json:"node_id"`
	Stream string `json:"stream"`
	Seq    uint64 `json:"seq"`
}

type fileCursorStore struct {
	path      string
	flushIval time.Duration

	mu      sync.Mutex
	data    map[string]persistedCursor // key: agent + "\x00" + stream
	dirty   bool
	closing bool

	wakeCh chan struct{}
	doneCh chan struct{}
}

// NewFileCursorStore opens (or initializes) a JSON-backed cursor file and
// starts a background flusher. flushIval is the debounce window; 0 defaults
// to 1s. A Close() call flushes synchronously and stops the goroutine.
func NewFileCursorStore(path string, flushIval time.Duration) (CursorStore, error) {
	if flushIval <= 0 {
		flushIval = time.Second
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("cursor store: mkdir %s: %w", filepath.Dir(path), err)
	}
	s := &fileCursorStore{
		path:      path,
		flushIval: flushIval,
		data:      make(map[string]persistedCursor),
		wakeCh:    make(chan struct{}, 1),
		doneCh:    make(chan struct{}),
	}
	if err := s.loadFromDisk(); err != nil {
		return nil, err
	}
	go s.loop()
	return s, nil
}

func keyOf(agent, stream string) string { return agent + "\x00" + stream }

func (s *fileCursorStore) loadFromDisk() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("cursor store: read %s: %w", s.path, err)
	}
	if len(b) == 0 {
		return nil
	}
	var on disk // typed below
	if err := json.Unmarshal(b, &on); err != nil {
		return fmt.Errorf("cursor store: parse %s: %w", s.path, err)
	}
	for _, e := range on.Entries {
		s.data[keyOf(e.Agent, e.Cursor.Stream)] = e.Cursor
	}
	return nil
}

// disk is the file format: a typed envelope keeps it forward-compatible
// (a future Version field slot is available) and lets a human reader see
// the agent tag alongside each cursor.
type disk struct {
	Version int         `json:"version"`
	Entries []diskEntry `json:"entries"`
}

type diskEntry struct {
	Agent  string          `json:"agent"`
	Cursor persistedCursor `json:"cursor"`
}

func (s *fileCursorStore) Load(agent, stream string) *pb.Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.data[keyOf(agent, stream)]
	if !ok {
		return nil
	}
	return &pb.Cursor{NodeId: c.NodeID, Stream: c.Stream, Seq: c.Seq}
}

func (s *fileCursorStore) Save(agent, stream string, c *pb.Cursor) {
	if c == nil {
		return
	}
	s.mu.Lock()
	prev, ok := s.data[keyOf(agent, stream)]
	// Monotonic guard: only accept advancing seqs. Agents should never emit
	// a lower seq than last seen, but a bug or restart-in-flight could — and
	// rewinding the cursor would make us re-subscribe with stale state.
	if ok && c.Seq < prev.Seq && c.NodeId == prev.NodeID {
		s.mu.Unlock()
		return
	}
	s.data[keyOf(agent, stream)] = persistedCursor{
		NodeID: c.NodeId,
		Stream: c.Stream,
		Seq:    c.Seq,
	}
	s.dirty = true
	s.mu.Unlock()
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// loop wakes on every Save or every flushIval, whichever comes first, and
// persists when dirty. A single-slot wakeCh coalesces bursts.
func (s *fileCursorStore) loop() {
	defer close(s.doneCh)
	t := time.NewTicker(s.flushIval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case _, ok := <-s.wakeCh:
			if !ok {
				_ = s.flushLocked()
				return
			}
		}
		s.mu.Lock()
		if s.closing {
			_ = s.flushLockedBare()
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		_ = s.flushLocked()
	}
}

func (s *fileCursorStore) flushLocked() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLockedBare()
}

// flushLockedBare writes the in-memory map to disk via temp+rename if dirty.
// Caller must hold s.mu.
func (s *fileCursorStore) flushLockedBare() error {
	if !s.dirty {
		return nil
	}
	payload := disk{Version: 1}
	for k, c := range s.data {
		agent := k
		if i := indexNull(agent); i >= 0 {
			agent = agent[:i]
		}
		payload.Entries = append(payload.Entries, diskEntry{Agent: agent, Cursor: c})
	}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("cursor store: marshal: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("cursor store: write tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("cursor store: rename: %w", err)
	}
	s.dirty = false
	return nil
}

func indexNull(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return i
		}
	}
	return -1
}

func (s *fileCursorStore) Flush() error { return s.flushLocked() }

func (s *fileCursorStore) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.mu.Unlock()
	close(s.wakeCh)
	<-s.doneCh
	return nil
}
