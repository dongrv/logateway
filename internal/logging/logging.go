// Package logging provides file-based error/warn log output.
// Uses Go standard log package with custom writers.
package logging

import (
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	mu          sync.Mutex
	fileWriters []io.Closer
	consoleOut  io.Writer = os.Stdout
)

// Setup configures log output. levels specifies which log levels get their
// own daily file (e.g. ["error", "warn", "info"]). If empty, defaults to
// ["error", "warn"]. If consoleEnabled is false, stdout is suppressed.
func Setup(dir string, consoleEnabled bool, levels []string) {
	mu.Lock()
	defer mu.Unlock()

	if len(levels) == 0 {
		levels = []string{"error", "warn"}
	}

	var writers []io.Writer
	if consoleEnabled {
		writers = append(writers, consoleOut)
	}

	for _, lvl := range levels {
		dw := newDailyWriter(dir, lvl)
		fileWriters = append(fileWriters, dw)
		writers = append(writers, &levelFilter{inner: dw, level: lvl})
	}

	log.SetOutput(io.MultiWriter(writers...))
	log.SetFlags(log.LstdFlags)
	log.Printf("[INFO] logging initialized: console=%v file_levels=%v", consoleEnabled, levels)
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
	prefixLen := len(prefix)
	for i := 0; i <= len(p)-prefixLen; i++ {
		if strings.EqualFold(string(p[i:i+prefixLen]), prefix) {
			return true
		}
	}
	return false
}
