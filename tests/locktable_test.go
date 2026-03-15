package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"pfs/pkg/metaserver"
)

func TestConcurrentReaders(t *testing.T) {
	lt := metaserver.NewLockTableForTest()
	const n = 5
	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("reader-%d", id)
			fd, err := lt.Acquire("file.txt", clientID, metaserver.LockRead)
			if err != nil {
				t.Errorf("reader %d acquire: %v", id, err)
				return
			}
			time.Sleep(10 * time.Millisecond)
			if _, _, err := lt.Release(fd, clientID); err != nil {
				t.Errorf("reader %d release: %v", id, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestWriterExclusion(t *testing.T) {
	lt := metaserver.NewLockTableForTest()

	fd1, err := lt.Acquire("file.txt", "writer1", metaserver.LockWrite)
	if err != nil {
		t.Fatalf("writer1 acquire: %v", err)
	}

	type result struct {
		fd  int32
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fd, err := lt.Acquire("file.txt", "writer2", metaserver.LockWrite)
		ch <- result{fd, err}
	}()

	time.Sleep(20 * time.Millisecond)

	select {
	case <-ch:
		t.Fatal("writer2 acquired lock while writer1 still holds it")
	default:
	}

	if _, _, err := lt.Release(fd1, "writer1"); err != nil {
		t.Fatalf("release writer1: %v", err)
	}

	var res result
	select {
	case res = <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writer2 never unblocked")
	}
	if res.err != nil {
		t.Fatalf("writer2 acquire error: %v", res.err)
	}
	// Clean up writer2's lock
	lt.Release(res.fd, "writer2") //nolint:errcheck
}

func TestReaderBlocksWriter(t *testing.T) {
	lt := metaserver.NewLockTableForTest()

	fd, err := lt.Acquire("file.txt", "reader", metaserver.LockRead)
	if err != nil {
		t.Fatalf("reader acquire: %v", err)
	}

	type result struct {
		fd  int32
		err error
	}
	ch := make(chan result, 1)
	go func() {
		fd, err := lt.Acquire("file.txt", "writer", metaserver.LockWrite)
		ch <- result{fd, err}
	}()

	time.Sleep(20 * time.Millisecond)

	select {
	case <-ch:
		t.Fatal("writer acquired lock while reader holds it")
	default:
	}

	if _, _, err := lt.Release(fd, "reader"); err != nil {
		t.Fatalf("release reader: %v", err)
	}

	var res result
	select {
	case res = <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writer never unblocked")
	}
	if res.err != nil {
		t.Fatalf("writer acquire error: %v", res.err)
	}
	lt.Release(res.fd, "writer") //nolint:errcheck
}
