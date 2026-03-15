// demo starts an in-process cluster and runs through the key PFS operations
// with visible output so you can see everything working.
//
//	go run ./cmd/demo
package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	fspb "pfs/gen/fileserverpb"
	metapb "pfs/gen/metadatapb"
	"pfs/pkg/client"
	"pfs/pkg/common"
	"pfs/pkg/fileserver"
	"pfs/pkg/metaserver"
)

func main() {
	// ── Start 3 file servers on random ports ──────────────────────────────────
	fsAddrs := make([]string, 3)
	for i := range 3 {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		must(err)
		fsAddrs[i] = lis.Addr().String()

		dir, err := os.MkdirTemp("", fmt.Sprintf("pfs-fs%d-*", i))
		must(err)
		storage, err := fileserver.NewStorage(dir)
		must(err)

		srv := grpc.NewServer()
		fspb.RegisterFileServiceServer(srv, fileserver.NewServer(storage))
		go srv.Serve(lis) //nolint:errcheck
		fmt.Printf("  file server %d  → %s  (data: %s)\n", i, fsAddrs[i], dir)
	}

	// ── Start metadata server ─────────────────────────────────────────────────
	metaLis, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	metaAddr := metaLis.Addr().String()
	metaSrv := grpc.NewServer()
	metapb.RegisterMetadataServiceServer(metaSrv, metaserver.NewServer(fsAddrs))
	go metaSrv.Serve(metaLis) //nolint:errcheck
	fmt.Printf("  metadata server → %s\n\n", metaAddr)

	time.Sleep(50 * time.Millisecond) // let servers start

	dial := func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// ── Demo ──────────────────────────────────────────────────────────────────

	section("1. Create a file striped across 3 servers")
	alice, err := client.NewClient("alice", metaAddr, dial)
	must(err)
	defer alice.Shutdown()

	rc := alice.PfsCreate("notes.txt", 3)
	printf("  pfs_create(notes.txt, stripe_width=3) → %d (0=ok)\n", rc)

	section("2. Write 2 KB of data (spans 4 blocks across 3 servers)")
	payload := make([]byte, 2*common.BlockSize) // 1024 bytes
	for i := range payload {
		payload[i] = byte('A' + (i / common.BlockSize))
	}
	fd := alice.PfsOpen("notes.txt", common.ModeWrite)
	printf("  pfs_open(notes.txt, WRITE) → fd=%d\n", fd)
	n := alice.PfsWrite(fd, payload, len(payload), 0)
	printf("  pfs_write(%d bytes) → wrote %d bytes\n", len(payload), n)
	printf("  (data lives in client cache — not yet on disk)\n")
	alice.PfsClose(fd)
	printf("  pfs_close(fd) → dirty blocks flushed to file servers ✓\n")

	section("3. Read back and verify")
	bob, err := client.NewClient("bob", metaAddr, dial)
	must(err)
	defer bob.Shutdown()

	fd2 := bob.PfsOpen("notes.txt", common.ModeRead)
	printf("  pfs_open(notes.txt, READ) → fd=%d\n", fd2)

	buf := make([]byte, len(payload))
	got := bob.PfsRead(fd2, buf, len(payload), 0)
	printf("  pfs_read(%d bytes) → got %d bytes\n", len(payload), got)
	if bytes.Equal(buf[:got], payload) {
		printf("  data matches original ✓\n")
	} else {
		printf("  ✗ DATA MISMATCH\n")
	}

	// Read same block again → should hit cache
	bob.PfsRead(fd2, buf, len(payload), 0)
	stats := bob.PfsExecstat()
	printf("  read same data again → NumReadHits=%d ✓\n", stats.NumReadHits)
	bob.PfsClose(fd2)

	section("4. Read/write lock contention")
	charlie, err := client.NewClient("charlie", metaAddr, dial)
	must(err)
	defer charlie.Shutdown()

	fdW := charlie.PfsOpen("notes.txt", common.ModeWrite)
	printf("  charlie opens for WRITE → fd=%d (holds exclusive lock)\n", fdW)

	blocked := make(chan int, 1)
	go func() {
		// dave will block here until charlie closes
		dave, _ := client.NewClient("dave", metaAddr, dial)
		defer dave.Shutdown()
		t0 := time.Now()
		fdR := dave.PfsOpen("notes.txt", common.ModeRead)
		blocked <- int(time.Since(t0).Milliseconds())
		dave.PfsClose(fdR)
	}()

	printf("  dave tries to open for READ → BLOCKING (writer holds lock)...\n")
	time.Sleep(100 * time.Millisecond)
	charlie.PfsClose(fdW)
	printf("  charlie closed → lock released\n")

	waited := <-blocked
	printf("  dave unblocked after ~%dms ✓\n", waited)

	section("5. Cache invalidation")
	// eve reads the file and caches it
	eve, err := client.NewClient("eve", metaAddr, dial)
	must(err)
	defer eve.Shutdown()

	fdE := eve.PfsOpen("notes.txt", common.ModeRead)
	eve.PfsRead(fd2, buf, 10, 0)
	eve.PfsClose(fdE)

	// frank writes new data
	frank, err := client.NewClient("frank", metaAddr, dial)
	must(err)
	defer frank.Shutdown()

	fdF := frank.PfsOpen("notes.txt", common.ModeWrite)
	frank.PfsWrite(fdF, []byte("UPDATED!  "), 10, 0)
	frank.PfsClose(fdF) // triggers invalidation notice to eve

	time.Sleep(50 * time.Millisecond)
	eveStats := eve.PfsExecstat()
	printf("  frank wrote + closed → eve received %d invalidation notice(s) ✓\n", eveStats.NumInvalidations)

	section("6. Execstat summary (bob)")
	s := bob.PfsExecstat()
	printf("  NumReadHits=%d  NumWriteHits=%d  NumEvictions=%d\n", s.NumReadHits, s.NumWriteHits, s.NumEvictions)
	printf("  NumWritebacks=%d  NumCloseWritebacks=%d  NumCloseEvictions=%d\n",
		s.NumWritebacks, s.NumCloseWritebacks, s.NumCloseEvictions)

	section("7. Delete")
	rc = alice.PfsDelete("notes.txt")
	printf("  pfs_delete(notes.txt) → %d\n", rc)
	fd3 := alice.PfsOpen("notes.txt", common.ModeRead)
	printf("  pfs_open after delete → fd=%d (-1 = error, file gone) ✓\n", fd3)

	fmt.Println("\nAll done.")
}

func section(s string) { fmt.Printf("\n── %s\n", s) }
func printf(f string, a ...any) { fmt.Printf(f+"\n", a...) }
func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
