package logging

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// dailyWriter writes to a daily rotating log file.
// Thread-safe via internal mutex.
type dailyWriter struct {
	mu    sync.Mutex
	dir   string
	level string
	file  *os.File
	buf   *bufio.Writer
	today string
}

func newDailyWriter(dir, level string) *dailyWriter {
	return &dailyWriter{dir: filepath.Join(dir, level), level: level}
}

func (w *dailyWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	today := time.Now().UTC().Format("2006-01-02")
	if w.today != today {
		w.rotate(today)
	}
	if w.file == nil {
		return len(p), nil
	}
	n, err := w.buf.Write(p)
	if err == nil {
		w.buf.Flush()
	}
	return n, err
}

func (w *dailyWriter) rotate(today string) {
	if w.file != nil {
		w.buf.Flush()
		w.file.Close()
	}
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return
	}
	path := filepath.Join(w.dir, fmt.Sprintf("%s.log", today))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		w.file = nil
		w.buf = nil
		return
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, 64<<10)
	w.today = today
}

func (w *dailyWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		w.buf.Flush()
		w.file.Close()
		w.file = nil
	}
	return nil
}
