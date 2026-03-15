package metaserver

import (
	"fmt"
	"sync"
)

// LockMode represents the type of lock held on a file.
type LockMode int

const (
	LockNone  LockMode = 0
	LockRead  LockMode = 1
	LockWrite LockMode = 2
)

// fdRecord maps a file descriptor back to its context.
type fdRecord struct {
	filename string
	clientID string
	mode     LockMode
}

// lockEntry tracks lock state for a single file.
type lockEntry struct {
	mode    LockMode
	readers map[string]struct{} // set of clientIDs holding read locks
	writer  string              // clientID holding write lock (empty if none)
	cond    *sync.Cond
}

// LockTable manages file-level read/write locks across clients.
type LockTable struct {
	mu     sync.Mutex
	locks  map[string]*lockEntry // filename → lock state
	fds    map[int32]*fdRecord   // fd → record
	nextFD int32
}

func newLockTable() *LockTable {
	return &LockTable{
		locks:  make(map[string]*lockEntry),
		fds:    make(map[int32]*fdRecord),
		nextFD: 1,
	}
}

// NewLockTableForTest creates a LockTable for use in tests.
func NewLockTableForTest() *LockTable { return newLockTable() }

func (lt *LockTable) entry(filename string) *lockEntry {
	e, ok := lt.locks[filename]
	if !ok {
		e = &lockEntry{
			mode:    LockNone,
			readers: make(map[string]struct{}),
		}
		e.cond = sync.NewCond(&lt.mu)
		lt.locks[filename] = e
	}
	return e
}

// Acquire blocks until the requested lock can be granted, then returns an fd.
// mode must be LockRead or LockWrite.
func (lt *LockTable) Acquire(filename, clientID string, mode LockMode) (int32, error) {
	if mode != LockRead && mode != LockWrite {
		return 0, fmt.Errorf("invalid lock mode %d", mode)
	}

	lt.mu.Lock()
	defer lt.mu.Unlock()

	e := lt.entry(filename)

	// Mesa-style: loop until the lock state allows acquisition.
	for !lt.canAcquire(e, clientID, mode) {
		e.cond.Wait()
	}

	// Grant the lock
	if mode == LockRead {
		e.readers[clientID] = struct{}{}
		e.mode = LockRead
	} else {
		e.writer = clientID
		e.mode = LockWrite
	}

	fd := lt.nextFD
	lt.nextFD++
	lt.fds[fd] = &fdRecord{filename: filename, clientID: clientID, mode: mode}
	return fd, nil
}

// canAcquire checks whether a lock can be granted immediately.
// Called with lt.mu held.
func (lt *LockTable) canAcquire(e *lockEntry, clientID string, mode LockMode) bool {
	switch mode {
	case LockRead:
		// Can read if no writer holds the lock
		return e.writer == ""
	case LockWrite:
		// Can write if no readers and no writer
		return len(e.readers) == 0 && e.writer == ""
	}
	return false
}

// Release releases the lock associated with fd.
// It returns the filename so the caller can send invalidation notices.
// The caller MUST send invalidation notices OUTSIDE this method
// (after the lock is released) to avoid holding lt.mu during gRPC calls.
func (lt *LockTable) Release(fd int32, clientID string) (filename string, wasWrite bool, err error) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	rec, ok := lt.fds[fd]
	if !ok {
		return "", false, fmt.Errorf("unknown fd %d", fd)
	}
	if rec.clientID != clientID {
		return "", false, fmt.Errorf("fd %d not owned by client %q", fd, clientID)
	}

	e := lt.entry(rec.filename)
	switch rec.mode {
	case LockRead:
		delete(e.readers, clientID)
		if len(e.readers) == 0 {
			e.mode = LockNone
		}
	case LockWrite:
		wasWrite = true
		e.writer = ""
		e.mode = LockNone
	}

	delete(lt.fds, fd)
	e.cond.Broadcast() // wake all waiters; they'll re-check canAcquire

	return rec.filename, wasWrite, nil
}

// LookupFD returns the fdRecord for a given fd, or an error if not found.
func (lt *LockTable) LookupFD(fd int32, clientID string) (*fdRecord, error) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	rec, ok := lt.fds[fd]
	if !ok {
		return nil, fmt.Errorf("unknown fd %d", fd)
	}
	if rec.clientID != clientID {
		return nil, fmt.Errorf("fd %d not owned by client %q", fd, clientID)
	}
	cp := *rec
	return &cp, nil
}
