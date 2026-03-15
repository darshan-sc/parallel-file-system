package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	fspb "pfs/gen/fileserverpb"
	metapb "pfs/gen/metadatapb"
	"pfs/pkg/common"
)

// openFile tracks client-side state for an open file descriptor.
type openFile struct {
	filename string
	serverFD int32 // fd assigned by the metadata server
	mode     int   // common.ModeRead or common.ModeWrite
	recipe   *metapb.StripeRecipe
}

// DialFunc is a function that opens a gRPC connection to addr.
// Inject a custom one in tests (e.g. bufconn dialer).
type DialFunc func(addr string) (*grpc.ClientConn, error)

// Client is the top-level PFS client. Create one per process via NewClient.
type Client struct {
	mu sync.Mutex

	clientID   string
	metaClient metapb.MetadataServiceClient
	dial       DialFunc
	fsClients  map[string]fspb.FileServiceClient // server addr → stub
	openFiles  map[int]*openFile
	nextFD     int

	cache *LRUCache
	stats PfsExecstat

	cancelSub context.CancelFunc
}

// defaultDialFunc returns a plain insecure gRPC connection.
func defaultDialFunc(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// NewClient dials the metadata server and starts the background Subscribe goroutine.
// Pass dialFn=nil to use the default insecure dialer.
func NewClient(clientID, metaAddr string, dialFn DialFunc) (*Client, error) {
	if dialFn == nil {
		dialFn = defaultDialFunc
	}
	metaConn, err := dialFn(metaAddr)
	if err != nil {
		return nil, fmt.Errorf("dial metaserver %q: %w", metaAddr, err)
	}

	c := &Client{
		clientID:   clientID,
		metaClient: metapb.NewMetadataServiceClient(metaConn),
		dial:       dialFn,
		fsClients:  make(map[string]fspb.FileServiceClient),
		openFiles:  make(map[int]*openFile),
		nextFD:     1,
	}
	c.cache = NewLRUCache(common.NumCacheBlocks, &c.stats)

	ctx, cancel := context.WithCancel(context.Background())
	c.cancelSub = cancel
	go c.runSubscribe(ctx)

	return c, nil
}

// runSubscribe opens a long-lived Subscribe stream and processes invalidations.
func (c *Client) runSubscribe(ctx context.Context) {
	stream, err := c.metaClient.Subscribe(ctx, &metapb.SubscribeRequest{ClientId: c.clientID})
	if err != nil {
		log.Printf("[%s] subscribe error: %v", c.clientID, err)
		return
	}
	for {
		notice, err := stream.Recv()
		if err == io.EOF || ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("[%s] subscribe recv: %v", c.clientID, err)
			return
		}
		wb := c.makeWriteback(notice.Filename)
		if len(notice.BlockIndices) > 0 {
			c.cache.InvalidateBlocks(notice.Filename, notice.BlockIndices, wb)
		} else {
			c.cache.Invalidate(notice.Filename, wb)
		}
	}
}

// Shutdown shuts down the subscribe goroutine.
func (c *Client) Shutdown() {
	if c.cancelSub != nil {
		c.cancelSub()
	}
}

// fsClient returns (or lazily dials) a FileService stub for the given address.
func (c *Client) fsClient(addr string) (fspb.FileServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cl, ok := c.fsClients[addr]; ok {
		return cl, nil
	}
	conn, err := c.dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dial fileserver %q: %w", addr, err)
	}
	cl := fspb.NewFileServiceClient(conn)
	c.fsClients[addr] = cl
	return cl, nil
}

// makeWriteback returns a WritebackFn that flushes one block to its server.
func (c *Client) makeWriteback(filename string) WritebackFn {
	return func(key CacheKey, data []byte) error {
		recipe := c.recipeForFile(filename)
		if recipe == nil {
			return nil
		}
		return c.writeBlock(key.Filename, key.BlockIndex, data, recipe)
	}
}

// recipeForFile returns the StripeRecipe for a filename, scanning open files.
func (c *Client) recipeForFile(filename string) *metapb.StripeRecipe {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, of := range c.openFiles {
		if of.filename == filename {
			return of.recipe
		}
	}
	return nil
}

// writeBlock sends one full block to the appropriate file server.
func (c *Client) writeBlock(filename string, blockIndex int64, data []byte, recipe *metapb.StripeRecipe) error {
	sw := int(recipe.StripeWidth)
	bs := recipe.BlockSize
	si := ServerIndex(blockIndex, sw)
	serverOffset := OffsetOnServer(blockIndex, sw, bs)

	if si >= len(recipe.ServerAddrs) {
		return fmt.Errorf("server index %d out of range", si)
	}
	cl, err := c.fsClient(recipe.ServerAddrs[si])
	if err != nil {
		return err
	}
	_, err = cl.WriteBytes(context.Background(), &fspb.WriteRequest{
		Filename: filename,
		Offset:   serverOffset,
		Data:     data,
	})
	return err
}

// readBlockFromServer fetches one full block from the appropriate server.
func (c *Client) readBlockFromServer(filename string, blockIndex int64, recipe *metapb.StripeRecipe) ([]byte, error) {
	sw := int(recipe.StripeWidth)
	bs := recipe.BlockSize
	si := ServerIndex(blockIndex, sw)
	serverOffset := OffsetOnServer(blockIndex, sw, bs)

	if si >= len(recipe.ServerAddrs) {
		return nil, fmt.Errorf("server index %d out of range", si)
	}
	cl, err := c.fsClient(recipe.ServerAddrs[si])
	if err != nil {
		return nil, err
	}
	resp, err := cl.ReadBytes(context.Background(), &fspb.ReadRequest{
		Filename: filename,
		Offset:   serverOffset,
		Length:   bs,
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}
