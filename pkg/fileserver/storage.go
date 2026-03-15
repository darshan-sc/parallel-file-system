package fileserver

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// fileHandle wraps an open file with a per-file RWMutex to serialize
// multi-syscall reads and writes without blocking across files.
type fileHandle struct {
	mu sync.RWMutex
	f  *os.File
}

// Storage manages raw block data on disk for one file server.
type Storage struct {
	dataDir string

	mu      sync.Mutex
	handles map[string]*fileHandle
}

func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	return &Storage{
		dataDir: dataDir,
		handles: make(map[string]*fileHandle),
	}, nil
}

func (s *Storage) filePath(filename string) string {
	return filepath.Join(s.dataDir, filepath.Base(filename))
}

// handle returns (or creates) the fileHandle for a filename.
// The file is opened with O_RDWR|O_CREATE.
func (s *Storage) handle(filename string) (*fileHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if h, ok := s.handles[filename]; ok {
		return h, nil
	}
	path := s.filePath(filename)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	h := &fileHandle{f: f}
	s.handles[filename] = h
	return h, nil
}

// ReadAt reads up to length bytes at offset from the named file.
func (s *Storage) ReadAt(filename string, offset, length int64) ([]byte, error) {
	h, err := s.handle(filename)
	if err != nil {
		return nil, err
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	info, err := h.f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if offset >= fileSize {
		return []byte{}, nil
	}
	if offset+length > fileSize {
		length = fileSize - offset
	}

	buf := make([]byte, length)
	n, err := h.f.ReadAt(buf, offset)
	if err != nil && n == 0 {
		return nil, err
	}
	return buf[:n], nil
}

// WriteAt writes data at offset in the named file, extending it if necessary.
func (s *Storage) WriteAt(filename string, offset int64, data []byte) (int64, error) {
	h, err := s.handle(filename)
	if err != nil {
		return 0, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	n, err := h.f.WriteAt(data, offset)
	if err != nil {
		return int64(n), fmt.Errorf("write %q at %d: %w", filename, offset, err)
	}
	// Ensure data is persisted
	if syncErr := h.f.Sync(); syncErr != nil {
		return int64(n), fmt.Errorf("sync %q: %w", filename, syncErr)
	}
	return int64(n), nil
}
