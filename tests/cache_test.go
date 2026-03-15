package tests

import (
	"sync"
	"testing"

	"pfs/pkg/client"
)

func newTestCache(cap int) (*client.LRUCache, *client.PfsExecstat) {
	stats := &client.PfsExecstat{}
	return client.NewLRUCache(cap, stats), stats
}

func key(filename string, idx int64) client.CacheKey {
	return client.CacheKey{Filename: filename, BlockIndex: idx}
}

func TestCacheHit(t *testing.T) {
	c, stats := newTestCache(4)
	k := key("foo", 0)
	c.Put(k, []byte("hello"), false, nil)

	got := c.Get(k, false)
	if string(got) != "hello" {
		t.Fatalf("want hello, got %q", got)
	}
	if stats.NumReadHits != 1 {
		t.Errorf("want 1 read hit, got %d", stats.NumReadHits)
	}
}

func TestCacheMiss(t *testing.T) {
	c, stats := newTestCache(4)
	k := key("foo", 99)

	got := c.Get(k, false)
	if got != nil {
		t.Fatalf("want nil on miss, got %v", got)
	}
	if stats.NumReadHits != 0 {
		t.Errorf("want 0 read hits, got %d", stats.NumReadHits)
	}
}

func TestLRUEviction(t *testing.T) {
	c, stats := newTestCache(3)

	// Fill to capacity with clean blocks
	for i := int64(0); i < 3; i++ {
		c.Put(key("f", i), []byte{byte(i)}, false, nil)
	}

	// Insert a 4th block — should evict block 0 (LRU)
	var evictedKey client.CacheKey
	var evictedData []byte
	wb := func(k client.CacheKey, d []byte) error {
		evictedKey = k
		evictedData = d
		return nil
	}
	// Access block 1 and 2 to make block 0 the true LRU
	c.Get(key("f", 1), false)
	c.Get(key("f", 2), false)

	c.Put(key("f", 3), []byte{3}, false, wb)

	if stats.NumEvictions != 1 {
		t.Errorf("want 1 eviction, got %d", stats.NumEvictions)
	}
	// Evicted block should be block 0 (LRU)
	if evictedKey.BlockIndex != 0 {
		t.Errorf("want block 0 evicted, got block %d", evictedKey.BlockIndex)
	}
	_ = evictedData
}

func TestDirtyEviction(t *testing.T) {
	c, stats := newTestCache(2)

	var wbMu sync.Mutex
	var wbKeys []client.CacheKey

	wb := func(k client.CacheKey, d []byte) error {
		wbMu.Lock()
		wbKeys = append(wbKeys, k)
		wbMu.Unlock()
		return nil
	}

	// Put block 0 as dirty, block 1 as clean
	c.Put(key("f", 0), []byte("dirty"), true, wb)
	c.Put(key("f", 1), []byte("clean"), false, wb)

	// Inserting block 2 evicts LRU (block 0 = dirty)
	// Need to access block 1 to make block 0 the LRU
	c.Get(key("f", 1), false)
	c.Put(key("f", 2), []byte("new"), false, wb)

	if stats.NumEvictions < 1 {
		t.Fatalf("expected eviction")
	}
	if stats.NumWritebacks != 1 {
		t.Errorf("want 1 writeback, got %d", stats.NumWritebacks)
	}
	wbMu.Lock()
	if len(wbKeys) != 1 || wbKeys[0].BlockIndex != 0 {
		t.Errorf("expected writeback of block 0, got %v", wbKeys)
	}
	wbMu.Unlock()
}

func TestFlushFile(t *testing.T) {
	c, stats := newTestCache(10)

	var wbCount int
	wb := func(k client.CacheKey, d []byte) error {
		wbCount++
		return nil
	}

	c.Put(key("foo", 0), []byte("a"), true, nil)
	c.Put(key("foo", 1), []byte("b"), true, nil)
	c.Put(key("foo", 2), []byte("c"), false, nil) // clean
	c.Put(key("bar", 0), []byte("x"), true, nil)  // different file

	writebacks, evictions := c.FlushFile("foo", wb)
	if writebacks != 2 {
		t.Errorf("want 2 writebacks, got %d", writebacks)
	}
	if evictions != 3 {
		t.Errorf("want 3 evictions, got %d", evictions)
	}
	if stats.NumCloseWritebacks != 2 {
		t.Errorf("stat: want 2 close writebacks, got %d", stats.NumCloseWritebacks)
	}
	if stats.NumCloseEvictions != 3 {
		t.Errorf("stat: want 3 close evictions, got %d", stats.NumCloseEvictions)
	}
	// bar block should still be in cache
	if c.Get(key("bar", 0), false) == nil {
		t.Error("bar block should remain in cache")
	}
	// foo blocks should be gone
	if c.Get(key("foo", 0), false) != nil {
		t.Error("foo block 0 should have been evicted")
	}
}

func TestInvalidate(t *testing.T) {
	c, stats := newTestCache(10)

	var wbCount int
	wb := func(k client.CacheKey, d []byte) error {
		wbCount++
		return nil
	}

	c.Put(key("foo", 0), []byte("dirty"), true, nil)
	c.Put(key("foo", 1), []byte("clean"), false, nil)
	c.Put(key("bar", 0), []byte("other"), false, nil)

	c.Invalidate("foo", wb)

	if stats.NumInvalidations != 1 {
		t.Errorf("want 1 invalidation, got %d", stats.NumInvalidations)
	}
	if wbCount != 1 {
		t.Errorf("want 1 dirty writeback on invalidate, got %d", wbCount)
	}
	// foo blocks gone
	if c.Get(key("foo", 0), false) != nil || c.Get(key("foo", 1), false) != nil {
		t.Error("foo blocks should be invalidated")
	}
	// bar untouched
	if c.Get(key("bar", 0), false) == nil {
		t.Error("bar block should remain")
	}
}

func TestCacheUpdate(t *testing.T) {
	c, _ := newTestCache(4)
	k := key("f", 0)

	c.Put(k, []byte("original"), false, nil)
	c.Update(k, []byte("updated"))

	got := c.Get(k, false)
	if string(got) != "updated" {
		t.Fatalf("want updated, got %q", got)
	}
}
