package wal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplaySegmentPreservesFileOnCallbackError(t *testing.T) {
	w := newTestWriter(t)
	path := sealedSegmentPath(t, w, "project-a")

	replayed, err := w.replaySegment(path, func(Entry) error {
		return errors.New("sink busy")
	})
	if err == nil {
		t.Fatal("expected replay error")
	}
	if replayed != 0 {
		t.Fatalf("replayed = %d, want 0", replayed)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("segment should be preserved: %v", statErr)
	}
}

func TestReplaySealedSegmentsDeletesOnlyAfterSuccess(t *testing.T) {
	w := newTestWriter(t)
	path := sealedSegmentPath(t, w, "project-a")

	w.replaySealedSegments(func(Entry) error { return nil })

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment should be deleted after successful replay, stat err=%v", err)
	}
}

func TestReplaySealedSegmentsRecoversPanicAndPreservesFile(t *testing.T) {
	w := newTestWriter(t)
	path := sealedSegmentPath(t, w, "project-a")

	w.replaySealedSegments(func(Entry) error {
		panic("boom")
	})

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("segment should be preserved after panic: %v", err)
	}
}

func TestReadAllDoesNotDeleteSegments(t *testing.T) {
	w := newTestWriter(t)
	path := sealedSegmentPath(t, w, "project-a")

	entries, errs := ReadAll(w.dir)
	var count int
	for range entries {
		count++
	}
	if err := <-errs; err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if count == 0 {
		t.Fatal("expected entries")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("ReadAll must not delete segment: %v", err)
	}
}

func TestNewWriterSealsPreviousActiveSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(Config{Dir: dir, MaxSegmentBytes: 1 << 20, MaxSegments: 10})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := w.Write(sampleEntry("project-a")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w, err = NewWriter(Config{Dir: dir, MaxSegmentBytes: 1 << 20, MaxSegments: 10})
	if err != nil {
		t.Fatalf("reopen writer: %v", err)
	}
	defer w.Close()

	var replayed atomic.Int32
	w.replaySealedSegments(func(Entry) error {
		replayed.Add(1)
		return nil
	})
	if got := replayed.Load(); got != 1 {
		t.Fatalf("replayed = %d, want 1", got)
	}
}

func newTestWriter(t *testing.T) *Writer {
	t.Helper()
	w, err := NewWriter(Config{
		Dir:             t.TempDir(),
		MaxSegmentBytes: 1 << 20,
		MaxSegments:     10,
		SyncInterval:    0,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	t.Cleanup(func() {
		_ = w.Close()
	})
	return w
}

func sealedSegmentPath(t *testing.T, w *Writer, project string) string {
	t.Helper()
	if err := w.Write(sampleEntry(project)); err != nil {
		t.Fatalf("write: %v", err)
	}
	name := w.ActiveSegmentName()
	if err := w.rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	return filepath.Join(w.dir, name)
}

func sampleEntry(project string) Entry {
	data, _ := json.Marshal(map[string]string{"k": "v"})
	return Entry{
		Project:   project,
		Router:    "r",
		Data:      data,
		RequestID: "rid",
		TraceID:   "tid",
		Timestamp: time.Now(),
	}
}
