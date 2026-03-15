package tests

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	fspb "pfs/gen/fileserverpb"
	"pfs/pkg/common"
	"pfs/tests/testutil"
)

// helper: create → open (write) → write → close → open (read) → read → verify
func TestCreateWriteReadRoundtrip(t *testing.T) {
	cl := testutil.StartCluster(t, 3)
	c := cl.NewClient(t, "clientA")

	if r := c.PfsCreate("hello.txt", 3); r != 0 {
		t.Fatalf("create: %d", r)
	}

	fd := c.PfsOpen("hello.txt", common.ModeWrite)
	if fd < 0 {
		t.Fatal("open for write failed")
	}
	data := []byte("hello, distributed world!")
	if n := c.PfsWrite(fd, data, len(data), 0); n != len(data) {
		t.Fatalf("write: got %d, want %d", n, len(data))
	}
	if r := c.PfsClose(fd); r != 0 {
		t.Fatal("close failed")
	}

	fd2 := c.PfsOpen("hello.txt", common.ModeRead)
	if fd2 < 0 {
		t.Fatal("open for read failed")
	}
	buf := make([]byte, len(data))
	n := c.PfsRead(fd2, buf, len(data), 0)
	if n != len(data) {
		t.Fatalf("read: got %d bytes, want %d", n, len(data))
	}
	if !bytes.Equal(buf[:n], data) {
		t.Fatalf("data mismatch: got %q, want %q", buf[:n], data)
	}
	c.PfsClose(fd2)
}

func TestMultiBlockWrite(t *testing.T) {
	cl := testutil.StartCluster(t, 3)
	c := cl.NewClient(t, "clientA")

	c.PfsCreate("multi.bin", 3)

	// Write 3 full blocks + a bit (touches all 3 servers)
	size := 3*common.BlockSize + 100
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	fd := c.PfsOpen("multi.bin", common.ModeWrite)
	if fd < 0 {
		t.Fatal("open")
	}
	if n := c.PfsWrite(fd, payload, size, 0); n != size {
		t.Fatalf("write: got %d, want %d", n, size)
	}
	c.PfsClose(fd)

	fd2 := c.PfsOpen("multi.bin", common.ModeRead)
	if fd2 < 0 {
		t.Fatal("open read")
	}
	buf := make([]byte, size)
	n := c.PfsRead(fd2, buf, size, 0)
	if n != size {
		t.Fatalf("read: got %d, want %d", n, size)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatal("multi-block data mismatch")
	}
	c.PfsClose(fd2)
}

func TestTwoSimultaneousReaders(t *testing.T) {
	cl := testutil.StartCluster(t, 2)

	// Prepare file
	writer := cl.NewClient(t, "writer")
	writer.PfsCreate("shared.txt", 2)
	fd := writer.PfsOpen("shared.txt", common.ModeWrite)
	writer.PfsWrite(fd, []byte("data"), 4, 0)
	writer.PfsClose(fd)

	r1 := cl.NewClient(t, "reader1")
	r2 := cl.NewClient(t, "reader2")

	fd1 := r1.PfsOpen("shared.txt", common.ModeRead)
	fd2 := r2.PfsOpen("shared.txt", common.ModeRead)

	if fd1 < 0 || fd2 < 0 {
		t.Fatalf("both readers should open concurrently: fd1=%d fd2=%d", fd1, fd2)
	}
	r1.PfsClose(fd1)
	r2.PfsClose(fd2)
}

func TestWriterBlocksReader(t *testing.T) {
	cl := testutil.StartCluster(t, 2)

	writer := cl.NewClient(t, "writer")
	writer.PfsCreate("locked.txt", 2)

	fd := writer.PfsOpen("locked.txt", common.ModeWrite)
	if fd < 0 {
		t.Fatal("writer open")
	}

	// Write some data
	msg := []byte("written data")
	writer.PfsWrite(fd, msg, len(msg), 0)

	// Reader should block until writer closes
	type readResult struct {
		fd int
		t  time.Time
	}
	ch := make(chan readResult, 1)

	reader := cl.NewClient(t, "reader")
	go func() {
		rfd := reader.PfsOpen("locked.txt", common.ModeRead)
		ch <- readResult{rfd, time.Now()}
	}()

	time.Sleep(30 * time.Millisecond)
	closeTime := time.Now()
	writer.PfsClose(fd)

	var res readResult
	select {
	case res = <-ch:
	case <-time.After(time.Second):
		t.Fatal("reader never unblocked")
	}

	if res.fd < 0 {
		t.Fatal("reader open failed after writer closed")
	}
	if res.t.Before(closeTime) {
		t.Error("reader opened before writer closed")
	}

	// Verify reader sees written data
	buf := make([]byte, len(msg))
	n := reader.PfsRead(res.fd, buf, len(msg), 0)
	if n != len(msg) || !bytes.Equal(buf[:n], msg) {
		t.Errorf("read after write: got %q, want %q", buf[:n], msg)
	}
	reader.PfsClose(res.fd)
}

func TestReadCacheHit(t *testing.T) {
	cl := testutil.StartCluster(t, 2)

	// Set up file with data
	w := cl.NewClient(t, "writer")
	w.PfsCreate("cached.txt", 2)
	fd := w.PfsOpen("cached.txt", common.ModeWrite)
	w.PfsWrite(fd, []byte("block data"), 10, 0)
	w.PfsClose(fd)

	r := cl.NewClient(t, "reader")
	fd2 := r.PfsOpen("cached.txt", common.ModeRead)

	buf := make([]byte, 10)
	r.PfsRead(fd2, buf, 10, 0)
	r.PfsRead(fd2, buf, 10, 0) // second read = cache hit

	stats := r.PfsExecstat()
	if stats.NumReadHits < 1 {
		t.Errorf("want ≥1 read hit after double read, got %d", stats.NumReadHits)
	}
	r.PfsClose(fd2)
}

func TestCacheInvalidationOnWriterClose(t *testing.T) {
	cl := testutil.StartCluster(t, 2)

	// ClientA creates and writes initial data
	a := cl.NewClient(t, "clientA")
	a.PfsCreate("inv.txt", 2)
	fdA := a.PfsOpen("inv.txt", common.ModeWrite)
	a.PfsWrite(fdA, []byte("version1__"), 10, 0)
	a.PfsClose(fdA)

	// ClientB reads and caches the data
	b := cl.NewClient(t, "clientB")
	fdB := b.PfsOpen("inv.txt", common.ModeRead)
	buf := make([]byte, 10)
	b.PfsRead(fdB, buf, 10, 0)
	b.PfsClose(fdB)

	// ClientA writes new data
	fdA2 := a.PfsOpen("inv.txt", common.ModeWrite)
	a.PfsWrite(fdA2, []byte("version2!!"), 10, 0)
	a.PfsClose(fdA2) // triggers invalidation notice to B

	// Small pause for the async invalidation to arrive
	time.Sleep(50 * time.Millisecond)

	statsBefore := b.PfsExecstat()
	if statsBefore.NumInvalidations == 0 {
		t.Error("clientB should have received at least one invalidation notice")
	}
}

func TestDeleteThenOpenFails(t *testing.T) {
	cl := testutil.StartCluster(t, 2)
	c := cl.NewClient(t, "clientA")

	c.PfsCreate("todelete.txt", 2)
	if r := c.PfsDelete("todelete.txt"); r != 0 {
		t.Fatalf("delete: %d", r)
	}
	if fd := c.PfsOpen("todelete.txt", common.ModeRead); fd >= 0 {
		t.Errorf("open after delete should fail, got fd=%d", fd)
	}
}

func TestCloseFlushesCache(t *testing.T) {
	cl := testutil.StartCluster(t, 2)

	c := cl.NewClient(t, "writer")
	c.PfsCreate("flush.txt", 2)

	fd := c.PfsOpen("flush.txt", common.ModeWrite)
	payload := bytes.Repeat([]byte("X"), common.BlockSize)
	c.PfsWrite(fd, payload, common.BlockSize, 0)
	c.PfsClose(fd) // dirty block must be flushed here

	// Verify data is on the file server via direct RPC
	fsConn := cl.DialFileServer(t, 0)
	fsCl := fspb.NewFileServiceClient(fsConn)
	resp, err := fsCl.ReadBytes(context.Background(), &fspb.ReadRequest{
		Filename: "flush.txt",
		Offset:   0,
		Length:   int64(common.BlockSize),
	})
	if err != nil {
		t.Fatalf("direct read from file server: %v", err)
	}
	if !bytes.Equal(resp.Data, payload) {
		t.Errorf("file server data mismatch after close flush")
	}
}

func TestFstatReturnsMetadata(t *testing.T) {
	cl := testutil.StartCluster(t, 2)
	c := cl.NewClient(t, "clientA")

	c.PfsCreate("stat.txt", 2)
	fd := c.PfsOpen("stat.txt", common.ModeRead)
	if fd < 0 {
		t.Fatal("open")
	}
	defer c.PfsClose(fd)

	md, err := c.PfsFstat(fd)
	if err != nil {
		t.Fatalf("fstat: %v", err)
	}
	if md.Filename != "stat.txt" {
		t.Errorf("filename: got %q, want stat.txt", md.Filename)
	}
	if md.StripeWidth != 2 {
		t.Errorf("stripe width: got %d, want 2", md.StripeWidth)
	}
}

func TestConcurrentWritesToDifferentFiles(t *testing.T) {
	cl := testutil.StartCluster(t, 3)

	const numFiles = 5
	var wg sync.WaitGroup

	for i := range numFiles {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("file%d.bin", id)
			c := cl.NewClient(t, fmt.Sprintf("client%d", id))
			c.PfsCreate(name, 3)
			fd := c.PfsOpen(name, common.ModeWrite)
			data := bytes.Repeat([]byte{byte(id)}, common.BlockSize*2)
			c.PfsWrite(fd, data, len(data), 0)
			c.PfsClose(fd)

			// Read back and verify
			fd2 := c.PfsOpen(name, common.ModeRead)
			buf := make([]byte, len(data))
			n := c.PfsRead(fd2, buf, len(data), 0)
			c.PfsClose(fd2)
			if n != len(data) || !bytes.Equal(buf[:n], data) {
				t.Errorf("file %d: data mismatch", id)
			}
		}(i)
	}
	wg.Wait()
}
