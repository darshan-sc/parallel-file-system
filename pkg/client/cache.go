package client

import (
	"sync"
)

// CacheKey uniquely identifies a cached block.
type CacheKey struct {
	Filename   string
	BlockIndex int64
}

// CacheBlock is a node in the doubly-linked LRU list.
type CacheBlock struct {
	Key  CacheKey
	Data []byte
	Dirty bool

	prev, next *CacheBlock // sentinel-based doubly-linked list
}

// WritebackFn is called with dirty blocks that must be flushed to a file server.
// It must NOT be called while LRUCache.mu is held.
type WritebackFn func(key CacheKey, data []byte) error

// LRUCache is a fixed-capacity block cache with write-back eviction.
// The zero value is not valid; use NewLRUCache.
type LRUCache struct {
	mu     sync.Mutex
	cap    int
	blocks map[CacheKey]*CacheBlock

	// Sentinel nodes: head.next is MRU, tail.prev is LRU.
	head, tail *CacheBlock

	stats *PfsExecstat
}

func NewLRUCache(capacity int, stats *PfsExecstat) *LRUCache {
	head := &CacheBlock{}
	tail := &CacheBlock{}
	head.next = tail
	tail.prev = head
	return &LRUCache{
		cap:    capacity,
		blocks: make(map[CacheKey]*CacheBlock),
		head:   head,
		tail:   tail,
		stats:  stats,
	}
}

// insertFront places b immediately after head (MRU position). Called with mu held.
func (c *LRUCache) insertFront(b *CacheBlock) {
	b.prev = c.head
	b.next = c.head.next
	c.head.next.prev = b
	c.head.next = b
}

// remove unlinks b from the list. Called with mu held.
func (c *LRUCache) remove(b *CacheBlock) {
	b.prev.next = b.next
	b.next.prev = b.prev
}

// Get returns the data for key if present (cache hit).
// It moves the block to MRU position and increments the hit counter.
// Returns nil if not found.
func (c *LRUCache) Get(key CacheKey, isWrite bool) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, ok := c.blocks[key]
	if !ok {
		return nil
	}
	c.remove(b)
	c.insertFront(b)
	if isWrite {
		c.stats.NumWriteHits++
	} else {
		c.stats.NumReadHits++
	}
	// Return a copy so callers can't mutate the cache entry directly.
	cp := make([]byte, len(b.Data))
	copy(cp, b.Data)
	return cp
}

// Put inserts or updates a block in the cache, marking it dirty if requested.
// If the cache is full, it evicts the LRU block, writing it back via wb if dirty.
// wb may be nil; pass nil only if you know there are no dirty evictions possible.
func (c *LRUCache) Put(key CacheKey, data []byte, dirty bool, wb WritebackFn) {
	// Collect any dirty eviction candidates outside the lock so writeback
	// gRPC calls don't hold LRUCache.mu.
	var evictKey CacheKey
	var evictData []byte
	needEvict := false

	c.mu.Lock()

	if b, ok := c.blocks[key]; ok {
		// Update existing entry in place.
		c.remove(b)
		b.Data = data
		if dirty {
			b.Dirty = true
		}
		c.insertFront(b)
		c.mu.Unlock()
		return
	}

	// Evict LRU if at capacity.
	if len(c.blocks) >= c.cap {
		lru := c.tail.prev
		if lru != c.head { // shouldn't be, but guard anyway
			c.remove(lru)
			delete(c.blocks, lru.Key)
			c.stats.NumEvictions++
			if lru.Dirty {
				needEvict = true
				evictKey = lru.Key
				evictData = lru.Data
			}
		}
	}

	// Insert new block.
	b := &CacheBlock{Key: key, Data: data, Dirty: dirty}
	c.blocks[key] = b
	c.insertFront(b)
	c.mu.Unlock()

	// Write back dirty evicted block outside the lock.
	if needEvict && wb != nil {
		_ = wb(evictKey, evictData)
		c.mu.Lock()
		c.stats.NumWritebacks++
		c.mu.Unlock()
	}
}

// MarkDirty marks an existing cached block as dirty.
func (c *LRUCache) MarkDirty(key CacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.blocks[key]; ok {
		b.Dirty = true
	}
}

// Update replaces the data for an existing cached block and marks it dirty.
func (c *LRUCache) Update(key CacheKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.blocks[key]; ok {
		b.Data = data
		b.Dirty = true
		c.remove(b)
		c.insertFront(b)
	}
}

// DirtyBlockIndices returns the block indices of all dirty cached blocks for filename.
func (c *LRUCache) DirtyBlockIndices(filename string) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var indices []int64
	for key, b := range c.blocks {
		if key.Filename == filename && b.Dirty {
			indices = append(indices, key.BlockIndex)
		}
	}
	return indices
}

// InvalidateBlocks removes only the specified blocks for filename, writing back dirty ones first.
// Calls wb for each dirty block (outside the lock).
func (c *LRUCache) InvalidateBlocks(filename string, blockIndices []int64, wb WritebackFn) {
	indexSet := make(map[int64]struct{}, len(blockIndices))
	for _, i := range blockIndices {
		indexSet[i] = struct{}{}
	}

	type dirtyEntry struct {
		key  CacheKey
		data []byte
	}
	var dirty []dirtyEntry
	var toRemove []*CacheBlock

	c.mu.Lock()
	for key, b := range c.blocks {
		if key.Filename != filename {
			continue
		}
		if _, ok := indexSet[key.BlockIndex]; !ok {
			continue
		}
		if b.Dirty {
			cp := make([]byte, len(b.Data))
			copy(cp, b.Data)
			dirty = append(dirty, dirtyEntry{key, cp})
		}
		toRemove = append(toRemove, b)
	}
	c.mu.Unlock()

	for _, d := range dirty {
		if wb != nil {
			_ = wb(d.key, d.data)
		}
	}

	c.mu.Lock()
	for _, b := range toRemove {
		c.remove(b)
		delete(c.blocks, b.Key)
	}
	c.stats.NumInvalidations++
	c.mu.Unlock()
}

// Invalidate removes all cached blocks for filename, writing back dirty ones first.
// Calls wb for each dirty block (outside the lock).
func (c *LRUCache) Invalidate(filename string, wb WritebackFn) {
	// Collect dirty blocks under lock, then flush outside.
	type dirtyEntry struct {
		key  CacheKey
		data []byte
	}
	var dirty []dirtyEntry
	var toRemove []*CacheBlock

	c.mu.Lock()
	for key, b := range c.blocks {
		if key.Filename == filename {
			if b.Dirty {
				cp := make([]byte, len(b.Data))
				copy(cp, b.Data)
				dirty = append(dirty, dirtyEntry{key, cp})
			}
			toRemove = append(toRemove, b)
		}
	}
	c.mu.Unlock()

	// Flush dirty blocks
	for _, d := range dirty {
		if wb != nil {
			_ = wb(d.key, d.data)
		}
	}

	// Remove all blocks for this file
	c.mu.Lock()
	for _, b := range toRemove {
		c.remove(b)
		delete(c.blocks, b.Key)
	}
	c.stats.NumInvalidations++
	c.mu.Unlock()
}

// FlushFile writes back all dirty blocks for filename and marks them clean.
// Used by pfs_close. Calls wb for each dirty block outside the lock.
// Returns the count of written-back blocks and total evicted blocks.
func (c *LRUCache) FlushFile(filename string, wb WritebackFn) (writebacks, evictions int) {
	type dirtyEntry struct {
		key  CacheKey
		data []byte
	}
	var dirty []dirtyEntry
	var allBlocks []*CacheBlock

	c.mu.Lock()
	for key, b := range c.blocks {
		if key.Filename == filename {
			allBlocks = append(allBlocks, b)
			if b.Dirty {
				cp := make([]byte, len(b.Data))
				copy(cp, b.Data)
				dirty = append(dirty, dirtyEntry{key, cp})
			}
		}
	}
	c.mu.Unlock()

	// Writeback outside the lock
	for _, d := range dirty {
		if wb != nil {
			_ = wb(d.key, d.data)
		}
		writebacks++
	}

	// Remove all blocks for this file
	c.mu.Lock()
	for _, b := range allBlocks {
		c.remove(b)
		delete(c.blocks, b.Key)
		evictions++
	}
	c.stats.NumCloseWritebacks += writebacks
	c.stats.NumCloseEvictions += evictions
	c.mu.Unlock()
	return writebacks, evictions
}
