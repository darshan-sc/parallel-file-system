package metaserver

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	metapb "pfs/gen/metadatapb"
)

// metaStore holds in-memory file metadata.
type metaStore struct {
	mu    sync.RWMutex
	files map[string]*metapb.FileMetadata
}

func newMetaStore() *metaStore {
	return &metaStore{files: make(map[string]*metapb.FileMetadata)}
}

func (m *metaStore) create(filename string, stripeWidth int32, serverAddrs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.files[filename]; exists {
		return fmt.Errorf("file %q already exists", filename)
	}
	now := time.Now().UnixNano()
	m.files[filename] = &metapb.FileMetadata{
		Filename: filename,
		Size:     0,
		Ctime:    now,
		Mtime:    now,
		Recipe: &metapb.StripeRecipe{
			BlockSize:   512,
			StripeWidth: stripeWidth,
			ServerAddrs: serverAddrs,
		},
	}
	return nil
}

func (m *metaStore) get(filename string) (*metapb.FileMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	md, ok := m.files[filename]
	if !ok {
		return nil, fmt.Errorf("file %q not found", filename)
	}
	// Clone via proto to avoid copying the embedded mutex in MessageState
	return proto.Clone(md).(*metapb.FileMetadata), nil
}

func (m *metaStore) delete(filename string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.files[filename]; !ok {
		return fmt.Errorf("file %q not found", filename)
	}
	delete(m.files, filename)
	return nil
}

// updateSize bumps the file's size if newSize > current, and updates mtime.
func (m *metaStore) updateSize(filename string, newSize int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	md, ok := m.files[filename]
	if !ok {
		return
	}
	if newSize > md.Size {
		md.Size = newSize
	}
	md.Mtime = time.Now().UnixNano()
}
