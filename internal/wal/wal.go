// Package wal provides a disk-based Write-Ahead Log for message durability.
// When the in-memory channel is full and backpressure strategy is "fallback",
// messages are written to WAL segments on disk and replayed on next startup.
package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dongrv/logateway/internal/message"
)

// Config holds WAL configuration.
type Config struct {
	// Dir is the directory where WAL segment files are stored.
	Dir string
	// MaxSegmentBytes is the maximum size of a single segment file before rotation.
	MaxSegmentBytes int64
	// MaxSegments is the maximum number of segment files to retain.
	MaxSegments int
	// SyncInterval is how often to fsync the active segment to disk.
	// Zero means sync after every write.
	SyncInterval time.Duration
}

// DefaultConfig returns a Config with safe defaults.
func DefaultConfig() Config {
	return Config{
		Dir:             "data/wal",
		MaxSegmentBytes: 64 << 20, // 64MB
		MaxSegments:     10,
		SyncInterval:    100 * time.Millisecond,
	}
}

// Entry is a single record in the WAL, wrapping the delivered Envelope.
type Entry struct {
	Sequence  uint64          `json:"seq"`
	Project   string          `json:"project"`
	Router    string          `json:"router"`
	Data      json.RawMessage `json:"data"`
	RequestID string          `json:"request_id"`
	TraceID   string          `json:"trace_id"`
	Timestamp time.Time       `json:"timestamp"`
}

// Writer appends message entries to disk-based segment files.
// Safe for concurrent use.
type Writer struct {
	cfg Config
	mu  sync.Mutex

	dir        string
	seq        uint64
	activeSeg  *os.File
	activeBuf  *bufio.Writer
	activeSize int64
	activeName string

	stopCh     chan struct{}
	syncTicker *time.Ticker
}

// NewWriter creates or opens a WAL writer.
func NewWriter(cfg Config) (*Writer, error) {
	if cfg.Dir == "" {
		cfg.Dir = "data/wal"
	}
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = 64 << 20
	}
	if cfg.MaxSegments <= 0 {
		cfg.MaxSegments = 10
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal mkdir %s: %w", cfg.Dir, err)
	}

	w := &Writer{
		cfg:    cfg,
		dir:    cfg.Dir,
		stopCh: make(chan struct{}),
	}

	// Find the highest existing segment number and resume from it
	if err := w.openLatest(); err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}

	// Start periodic fsync
	if cfg.SyncInterval > 0 {
		w.syncTicker = time.NewTicker(cfg.SyncInterval)
		go w.syncLoop()
	}

	return w, nil
}

// Write appends a message entry to the WAL. Safe for concurrent use.
func (w *Writer) Write(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("wal marshal: %w", err)
	}
	data = append(data, '\n')

	// Rotate if needed
	if w.activeSeg != nil && w.activeSize+int64(len(data)) > w.cfg.MaxSegmentBytes {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("wal rotate: %w", err)
		}
	}

	// Open first segment if none active
	if w.activeSeg == nil {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("wal initial rotate: %w", err)
		}
	}

	n, err := w.activeBuf.Write(data)
	if err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	w.activeSize += int64(n)
	w.seq++

	// Immediate fsync if no periodic sync configured
	if w.cfg.SyncInterval == 0 {
		if err := w.activeBuf.Flush(); err != nil {
			return fmt.Errorf("wal flush: %w", err)
		}
		if err := w.activeSeg.Sync(); err != nil {
			return fmt.Errorf("wal sync: %w", err)
		}
	}

	return nil
}

// WriteMessage is a convenience method that converts a Message to a WAL Entry and writes it.
func (w *Writer) WriteMessage(msg *message.Message) error {
	return w.Write(Entry{
		Sequence:  0, // set in Write
		Project:   msg.Project,
		Router:    msg.Router,
		Data:      msg.Data,
		RequestID: msg.RequestID,
		TraceID:   msg.TraceID,
		Timestamp: msg.Timestamp,
	})
}

func (w *Writer) rotate() error {
	// Flush and close current segment
	if w.activeSeg != nil {
		if err := w.activeBuf.Flush(); err != nil {
			return err
		}
		if err := w.activeSeg.Sync(); err != nil {
			return err
		}
		if err := w.activeSeg.Close(); err != nil {
			return err
		}
	}

	// Create new segment
	segNum := w.nextSegmentNum()
	name := fmt.Sprintf("wal-%06d.log", segNum)
	path := filepath.Join(w.dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("wal create segment %s: %w", name, err)
	}

	w.activeSeg = f
	w.activeBuf = bufio.NewWriterSize(f, 64<<10) // 64KB buffer
	w.activeSize = 0
	w.activeName = name

	// Enforce max segments retention
	w.purgeOldSegments()

	return nil
}

func (w *Writer) nextSegmentNum() int {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return 1
	}
	maxNum := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			trimmed := strings.TrimPrefix(e.Name(), "wal-")
			trimmed = strings.TrimSuffix(trimmed, ".log")
			if n, err := strconv.Atoi(trimmed); err == nil && n > maxNum {
				maxNum = n
			}
		}
	}
	return maxNum + 1
}

func (w *Writer) openLatest() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	var latest string
	var latestNum int
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "wal-") || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		trimmed := strings.TrimPrefix(e.Name(), "wal-")
		trimmed = strings.TrimSuffix(trimmed, ".log")
		n, err := strconv.Atoi(trimmed)
		if err != nil {
			continue
		}
		if n > latestNum {
			latestNum = n
			latest = e.Name()
		}
	}

	if latest == "" {
		return nil // no existing segments, will create on first write
	}

	path := filepath.Join(w.dir, latest)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("wal open segment %s: %w", latest, err)
	}

	// Determine current size to know when to rotate
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	w.activeSeg = f
	w.activeBuf = bufio.NewWriterSize(f, 64<<10)
	w.activeSize = info.Size()
	w.activeName = latest

	return nil
}

func (w *Writer) purgeOldSegments() {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}

	type segInfo struct {
		name string
		num  int
		info fs.DirEntry
	}
	var segments []segInfo
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "wal-") || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		trimmed := strings.TrimPrefix(e.Name(), "wal-")
		trimmed = strings.TrimSuffix(trimmed, ".log")
		n, _ := strconv.Atoi(trimmed)
		segments = append(segments, segInfo{name: e.Name(), num: n, info: e})
	}

	if len(segments) <= w.cfg.MaxSegments {
		return
	}

	// Sort by segment number ascending, delete oldest
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].num < segments[j].num
	})

	for _, seg := range segments[:len(segments)-w.cfg.MaxSegments] {
		path := filepath.Join(w.dir, seg.name)
		if err := os.Remove(path); err != nil {
			log.Printf("[WARN] wal purge %s: %v", seg.name, err)
		}
	}
}

func (w *Writer) syncLoop() {
	defer w.syncTicker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-w.syncTicker.C:
			w.mu.Lock()
			if w.activeBuf != nil {
				if err := w.activeBuf.Flush(); err != nil {
					log.Printf("[ERROR] wal flush: %v", err)
				}
				if w.activeSeg != nil {
					if err := w.activeSeg.Sync(); err != nil {
						log.Printf("[ERROR] wal sync: %v", err)
					}
				}
			}
			w.mu.Unlock()
		}
	}
}

// Close flushes and closes the active segment.
func (w *Writer) Close() error {
	close(w.stopCh)
	if w.syncTicker != nil {
		w.syncTicker.Stop()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeSeg != nil {
		if err := w.activeBuf.Flush(); err != nil {
			return fmt.Errorf("wal close flush: %w", err)
		}
		if err := w.activeSeg.Sync(); err != nil {
			return fmt.Errorf("wal close sync: %w", err)
		}
		if err := w.activeSeg.Close(); err != nil {
			return fmt.Errorf("wal close: %w", err)
		}
		w.activeSeg = nil
		w.activeBuf = nil
	}
	return nil
}

// SegmentCount returns the number of WAL segment files on disk.
func (w *Writer) SegmentCount() int {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".log") {
			count++
		}
	}
	return count
}

// ActiveSegmentSize returns the current active segment file size.
func (w *Writer) ActiveSegmentSize() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeSize
}

// ---------- Reader (replay) ----------

// ReadAll reads all WAL entries from all segment files, ordered by sequence.
// Returns a channel that yields entries. The caller must drain the channel.
func ReadAll(dir string) (<-chan Entry, <-chan error) {
	entryCh := make(chan Entry, 256)
	errCh := make(chan error, 1)

	go func() {
		defer close(entryCh)
		defer close(errCh)

		entries, err := os.ReadDir(dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return // no WAL directory, nothing to replay
			}
			errCh <- fmt.Errorf("wal read dir: %w", err)
			return
		}

		// Collect and sort segment files by number
		type segFile struct {
			path string
			num  int
		}
		var segments []segFile
		for _, e := range entries {
			if e.IsDir() || !strings.HasPrefix(e.Name(), "wal-") || !strings.HasSuffix(e.Name(), ".log") {
				continue
			}
			trimmed := strings.TrimPrefix(e.Name(), "wal-")
			trimmed = strings.TrimSuffix(trimmed, ".log")
			n, err := strconv.Atoi(trimmed)
			if err != nil {
				continue
			}
			segments = append(segments, segFile{
				path: filepath.Join(dir, e.Name()),
				num:  n,
			})
		}

		sort.Slice(segments, func(i, j int) bool {
			return segments[i].num < segments[j].num
		})

		for _, seg := range segments {
			if err := readSegment(seg.path, entryCh); err != nil {
				errCh <- fmt.Errorf("wal read segment %s: %w", seg.path, err)
				return
			}
		}
	}()

	return entryCh, errCh
}

func readSegment(path string, out chan<- Entry) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1<<20), 10<<20) // 10MB max line

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("[WARN] wal replay skip corrupt line in %s: %v", path, err)
			continue
		}
		out <- entry
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("wal scan: %w", err)
	}

	// After successful replay, delete the segment
	if err := os.Remove(path); err != nil {
		log.Printf("[WARN] wal remove replayed segment %s: %v", path, err)
	}

	return nil
}
