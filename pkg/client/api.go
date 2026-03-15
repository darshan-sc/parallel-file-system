package client

import (
	"context"
	"fmt"

	metapb "pfs/gen/metadatapb"
	"pfs/pkg/common"
)

// PfsCreate registers a new file in the metadata server.
// stripeWidth determines how many file servers the file spans.
func (c *Client) PfsCreate(filename string, stripeWidth int) int {
	resp, err := c.metaClient.CreateFile(context.Background(), &metapb.CreateFileRequest{
		Filename:    filename,
		StripeWidth: int32(stripeWidth),
	})
	if err != nil || resp.Status != 0 {
		return -1
	}
	return 0
}

// PfsOpen opens filename in the given mode (ModeRead or ModeWrite).
// It acquires the appropriate lock on the metadata server and returns a local fd.
func (c *Client) PfsOpen(filename string, mode int) int {
	resp, err := c.metaClient.OpenFile(context.Background(), &metapb.OpenFileRequest{
		Filename: filename,
		Mode:     int32(mode),
		ClientId: c.clientID,
	})
	if err != nil || resp.Status != 0 {
		return -1
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	fd := c.nextFD
	c.nextFD++
	c.openFiles[fd] = &openFile{
		filename: filename,
		serverFD: resp.Fd,
		mode:     mode,
		recipe:   resp.Metadata.Recipe,
	}
	return fd
}

// PfsRead reads up to n bytes starting at offset from the file identified by fd.
// Returns the number of bytes actually placed into buf, or -1 on error.
func (c *Client) PfsRead(fd int, buf []byte, n int, offset int) int {
	c.mu.Lock()
	of, ok := c.openFiles[fd]
	c.mu.Unlock()
	if !ok {
		return -1
	}
	if of.mode != common.ModeRead && of.mode != common.ModeWrite {
		return -1
	}

	recipe := of.recipe
	bs := recipe.BlockSize
	sw := int(recipe.StripeWidth)
	ranges := ComputeRanges(int64(offset), int64(n), bs, sw)

	written := 0
	for _, r := range ranges {
		key := CacheKey{Filename: of.filename, BlockIndex: r.BlockIndex}

		var blockData []byte
		if cached := c.cache.Get(key, false); cached != nil {
			blockData = cached
		} else {
			// Cache miss — fetch from server
			fetched, err := c.readBlockFromServer(of.filename, r.BlockIndex, recipe)
			if err != nil {
				return -1
			}
			blockData = fetched
			wb := c.makeWriteback(of.filename)
			// Insert as clean (we just read it)
			c.cache.Put(key, blockData, false, wb)
		}

		// Copy the slice we need from this block into buf
		end := r.IntraOffset + r.Length
		if end > int64(len(blockData)) {
			end = int64(len(blockData))
		}
		if r.IntraOffset >= int64(len(blockData)) {
			// No data at this position (reading past EOF)
			break
		}
		n := copy(buf[written:], blockData[r.IntraOffset:end])
		written += n
		if int64(n) < r.Length {
			break // short read = reached EOF
		}
	}
	return written
}

// PfsWrite writes n bytes from buf starting at offset into the file identified by fd.
// Returns bytes written, or -1 on error.
func (c *Client) PfsWrite(fd int, buf []byte, n int, offset int) int {
	c.mu.Lock()
	of, ok := c.openFiles[fd]
	c.mu.Unlock()
	if !ok {
		return -1
	}
	if of.mode != common.ModeWrite {
		return -1
	}

	recipe := of.recipe
	bs := recipe.BlockSize
	sw := int(recipe.StripeWidth)
	ranges := ComputeRanges(int64(offset), int64(n), bs, sw)

	wb := c.makeWriteback(of.filename)
	written := 0

	for _, r := range ranges {
		key := CacheKey{Filename: of.filename, BlockIndex: r.BlockIndex}

		// Fetch or load block data
		var blockData []byte
		if cached := c.cache.Get(key, true); cached != nil {
			blockData = cached
		} else {
			// Cache miss — fetch from server (may be empty/short if new file)
			fetched, err := c.readBlockFromServer(of.filename, r.BlockIndex, recipe)
			if err != nil {
				fetched = []byte{}
			}
			blockData = fetched
		}

		// Grow blockData if necessary
		needed := r.IntraOffset + r.Length
		if int64(len(blockData)) < needed {
			ext := make([]byte, needed-int64(len(blockData)))
			blockData = append(blockData, ext...)
		}

		// Write slice of buf into block
		copy(blockData[r.IntraOffset:r.IntraOffset+r.Length], buf[written:written+int(r.Length)])
		written += int(r.Length)

		// Put back as dirty
		c.cache.Put(key, blockData, true, wb)
	}

	return written
}

// PfsClose flushes dirty cached blocks for fd, evicts them, then notifies the metadata server.
func (c *Client) PfsClose(fd int) int {
	c.mu.Lock()
	of, ok := c.openFiles[fd]
	c.mu.Unlock() // release lock; do NOT delete yet so recipeForFile can find it
	if !ok {
		return -1
	}

	// Collect dirty block indices before flushing so we can tell other clients
	// exactly which blocks to invalidate.
	dirtyBlocks := c.cache.DirtyBlockIndices(of.filename)

	// Flush and evict all cached blocks for this file.
	// The fd must still be in openFiles so makeWriteback can resolve the recipe.
	wb := c.makeWriteback(of.filename)
	c.cache.FlushFile(of.filename, wb)

	// Now remove from the fd table
	c.mu.Lock()
	delete(c.openFiles, fd)
	c.mu.Unlock()

	// Tell metadata server to release the lock; include dirty block indices so
	// it can send targeted invalidations to other clients.
	resp, err := c.metaClient.CloseFile(context.Background(), &metapb.CloseFileRequest{
		Fd:          of.serverFD,
		ClientId:    c.clientID,
		DirtyBlocks: dirtyBlocks,
	})
	if err != nil || resp.Status != 0 {
		return -1
	}
	return 0
}

// PfsDelete removes a file from the metadata server.
func (c *Client) PfsDelete(filename string) int {
	resp, err := c.metaClient.DeleteFile(context.Background(), &metapb.DeleteFileRequest{
		Filename: filename,
	})
	if err != nil || resp.Status != 0 {
		return -1
	}
	return 0
}

// PfsFstat returns metadata for the open file identified by fd.
func (c *Client) PfsFstat(fd int) (PfsMetadata, error) {
	c.mu.Lock()
	of, ok := c.openFiles[fd]
	c.mu.Unlock()
	if !ok {
		return PfsMetadata{}, fmt.Errorf("unknown fd %d", fd)
	}

	resp, err := c.metaClient.StatFile(context.Background(), &metapb.StatFileRequest{
		Fd:       of.serverFD,
		ClientId: c.clientID,
	})
	if err != nil || resp.Status != 0 {
		return PfsMetadata{}, fmt.Errorf("stat failed: %v", err)
	}
	md := resp.Metadata
	return PfsMetadata{
		Filename:    md.Filename,
		Size:        md.Size,
		Ctime:       md.Ctime,
		Mtime:       md.Mtime,
		BlockSize:   md.Recipe.BlockSize,
		StripeWidth: md.Recipe.StripeWidth,
		ServerAddrs: md.Recipe.ServerAddrs,
	}, nil
}

// PfsExecstat returns a snapshot of current I/O statistics.
func (c *Client) PfsExecstat() PfsExecstat {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
