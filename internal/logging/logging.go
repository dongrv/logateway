// Package logging provides file-based error/warn log output.
// Uses Go standard log package with custom writers.
package logging

import (
	"io"
	"log"
	"os"
	"sync"
)

var (
	mu          sync.Mutex
	fileWriters []io.Closer
	consoleOut  io.Writer = os.Stdout
)

// Setup configures log output. Console goes to stdout, error/warn to daily files.
func Setup(dir string) {
	mu.Lock()
	defer mu.Unlock()

	writers := []io.Writer{consoleOut}

	// Error file writer (lazy-create on first write)
	ew := newDailyWriter(dir, "error")
	fileWriters = append(fileWriters, ew)
	writers = append(writers, &levelFilter{inner: ew, level: "error"})

	// Warn file writer
	ww := newDailyWriter(dir, "warn")
	fileWriters = append(fileWriters, ww)
	writers = append(writers, &levelFilter{inner: ww, level: "warn"})

	log.SetOutput(io.MultiWriter(writers...))
	log.SetFlags(log.LstdFlags)
	log.Println("[INFO] logging initialized: console + file (error/warn)")
}

// Close flushes and closes all file writers.
func Close() {
	mu.Lock()
	defer mu.Unlock()
	for _, w := range fileWriters {
		w.Close()
	}
	fileWriters = nil
}

// levelFilter only passes through log lines that contain the level prefix.
type levelFilter struct {
	inner io.Writer
	level string
}

func (f *levelFilter) Write(p []byte) (int, error) {
	if containsLevel(p, f.level) {
		return f.inner.Write(p)
	}
	return len(p), nil
}

func containsLevel(p []byte, level string) bool {
	prefix := "[" + level + "]"
	for i := 0; i < len(p); i++ {
		if i+len(prefix) <= len(p) && string(p[i:i+len(prefix)]) == prefix {
			return true
		}
	}
	return false
}
