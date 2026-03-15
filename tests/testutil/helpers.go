package testutil

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	fspb "pfs/gen/fileserverpb"
	metapb "pfs/gen/metadatapb"
	"pfs/pkg/client"
	"pfs/pkg/fileserver"
	"pfs/pkg/metaserver"
)

const bufSize = 1 << 20 // 1 MiB

// Cluster holds all in-memory server listeners for a test cluster.
type Cluster struct {
	// Logical address strings (used as keys in routing tables).
	MetaAddr  string
	FileAddrs []string

	metaLis *bufconn.Listener
	fsLis   []*bufconn.Listener
}

// StartCluster spins up one metadata server and n file servers using bufconn
// (in-memory, no real TCP). All servers are cleaned up via t.Cleanup.
func StartCluster(t *testing.T, n int) *Cluster {
	t.Helper()

	fsLis := make([]*bufconn.Listener, n)
	fsAddrs := make([]string, n)

	for i := range n {
		lis := bufconn.Listen(bufSize)
		fsLis[i] = lis
		fsAddrs[i] = fmt.Sprintf("bufconn-fs-%d", i)

		dir := t.TempDir()
		storage, err := fileserver.NewStorage(dir)
		if err != nil {
			t.Fatalf("fileserver %d storage: %v", i, err)
		}
		srv := grpc.NewServer()
		fspb.RegisterFileServiceServer(srv, fileserver.NewServer(storage))
		go func(s *grpc.Server, l *bufconn.Listener) { _ = s.Serve(l) }(srv, lis)
		t.Cleanup(func() {
			srv.Stop()
			lis.Close()
		})
	}

	metaLis := bufconn.Listen(bufSize)
	metaSrv := grpc.NewServer()
	metapb.RegisterMetadataServiceServer(metaSrv, metaserver.NewServer(fsAddrs))
	go func() { _ = metaSrv.Serve(metaLis) }()
	t.Cleanup(func() {
		metaSrv.Stop()
		metaLis.Close()
	})

	return &Cluster{
		MetaAddr:  "bufconn-meta",
		FileAddrs: fsAddrs,
		metaLis:   metaLis,
		fsLis:     fsLis,
	}
}

// lisForAddr returns the bufconn listener for the given address.
func (cl *Cluster) lisForAddr(addr string) *bufconn.Listener {
	if addr == cl.MetaAddr {
		return cl.metaLis
	}
	for i, a := range cl.FileAddrs {
		if a == addr {
			return cl.fsLis[i]
		}
	}
	return nil
}

// DialFunc returns a client.DialFunc that routes all connections through bufconn.
func (cl *Cluster) DialFunc() client.DialFunc {
	return func(addr string) (*grpc.ClientConn, error) {
		lis := cl.lisForAddr(addr)
		if lis == nil {
			return nil, fmt.Errorf("testutil: no bufconn listener for addr %q", addr)
		}
		return grpc.NewClient(
			"passthrough://bufnet",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
		)
	}
}

// NewClient creates a PFS client wired through the cluster's bufconn listeners.
func (cl *Cluster) NewClient(t *testing.T, clientID string) *client.Client {
	t.Helper()
	c, err := client.NewClient(clientID, cl.MetaAddr, cl.DialFunc())
	if err != nil {
		t.Fatalf("NewClient(%q): %v", clientID, err)
	}
	t.Cleanup(c.Shutdown)
	return c
}

// DialMeta returns a raw gRPC connection to the metadata server (for direct RPC calls).
func (cl *Cluster) DialMeta(t *testing.T) *grpc.ClientConn {
	t.Helper()
	return cl.rawDial(t, cl.metaLis)
}

// DialFileServer returns a raw gRPC connection to file server i.
func (cl *Cluster) DialFileServer(t *testing.T, i int) *grpc.ClientConn {
	t.Helper()
	return cl.rawDial(t, cl.fsLis[i])
}

func (cl *Cluster) rawDial(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("rawDial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
