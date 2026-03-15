package metaserver

import (
	"context"
	"log"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metapb "pfs/gen/metadatapb"
)

// subscriberRegistry tracks active Subscribe streams.
type subscriberRegistry struct {
	mu   sync.RWMutex
	subs map[string]metapb.MetadataService_SubscribeServer // clientID → stream
}

func newSubscriberRegistry() *subscriberRegistry {
	return &subscriberRegistry{subs: make(map[string]metapb.MetadataService_SubscribeServer)}
}

func (r *subscriberRegistry) add(clientID string, stream metapb.MetadataService_SubscribeServer) {
	r.mu.Lock()
	r.subs[clientID] = stream
	r.mu.Unlock()
}

func (r *subscriberRegistry) remove(clientID string) {
	r.mu.Lock()
	delete(r.subs, clientID)
	r.mu.Unlock()
}

// sendInvalidation pushes an invalidation notice to all clients except the sender.
// blockIndices lists the specific blocks to invalidate; nil means all blocks.
func (r *subscriberRegistry) sendInvalidation(except, filename string, blockIndices []int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	notice := &metapb.InvalidateNotice{Filename: filename, BlockIndices: blockIndices}
	for cid, stream := range r.subs {
		if cid == except {
			continue
		}
		if err := stream.Send(notice); err != nil {
			log.Printf("invalidation send to %q failed: %v", cid, err)
		}
	}
}

// Server implements MetadataServiceServer.
type Server struct {
	metapb.UnimplementedMetadataServiceServer

	meta    *metaStore
	locks   *LockTable
	subs    *subscriberRegistry
	servers []string // file server addresses
}

func NewServer(fileServerAddrs []string) *Server {
	return &Server{
		meta:    newMetaStore(),
		locks:   newLockTable(),
		subs:    newSubscriberRegistry(),
		servers: fileServerAddrs,
	}
}

func (s *Server) CreateFile(ctx context.Context, req *metapb.CreateFileRequest) (*metapb.CreateFileResponse, error) {
	if req.Filename == "" {
		return nil, status.Error(codes.InvalidArgument, "filename required")
	}
	width := req.StripeWidth
	if width <= 0 || int(width) > len(s.servers) {
		width = int32(len(s.servers))
	}
	addrs := s.servers[:width]
	if err := s.meta.create(req.Filename, width, addrs); err != nil {
		return &metapb.CreateFileResponse{Status: 1, Error: err.Error()}, nil
	}
	return &metapb.CreateFileResponse{Status: 0}, nil
}

func (s *Server) OpenFile(ctx context.Context, req *metapb.OpenFileRequest) (*metapb.OpenFileResponse, error) {
	md, err := s.meta.get(req.Filename)
	if err != nil {
		return &metapb.OpenFileResponse{Status: 1, Error: err.Error()}, nil
	}

	var mode LockMode
	if req.Mode == 0 {
		mode = LockRead
	} else {
		mode = LockWrite
	}

	fd, err := s.locks.Acquire(req.Filename, req.ClientId, mode)
	if err != nil {
		return &metapb.OpenFileResponse{Status: 1, Error: err.Error()}, nil
	}

	return &metapb.OpenFileResponse{
		Status:   0,
		Fd:       fd,
		Metadata: md,
	}, nil
}

func (s *Server) CloseFile(ctx context.Context, req *metapb.CloseFileRequest) (*metapb.CloseFileResponse, error) {
	filename, wasWrite, err := s.locks.Release(req.Fd, req.ClientId)
	if err != nil {
		return &metapb.CloseFileResponse{Status: 1, Error: err.Error()}, nil
	}

	// Send invalidations outside the lock table mutex
	if wasWrite {
		s.subs.sendInvalidation(req.ClientId, filename, req.DirtyBlocks)
	}

	return &metapb.CloseFileResponse{Status: 0}, nil
}

func (s *Server) DeleteFile(ctx context.Context, req *metapb.DeleteFileRequest) (*metapb.DeleteFileResponse, error) {
	if err := s.meta.delete(req.Filename); err != nil {
		return &metapb.DeleteFileResponse{Status: 1, Error: err.Error()}, nil
	}
	return &metapb.DeleteFileResponse{Status: 0}, nil
}

func (s *Server) StatFile(ctx context.Context, req *metapb.StatFileRequest) (*metapb.StatFileResponse, error) {
	rec, err := s.locks.LookupFD(req.Fd, req.ClientId)
	if err != nil {
		return &metapb.StatFileResponse{Status: 1, Error: err.Error()}, nil
	}
	md, err := s.meta.get(rec.filename)
	if err != nil {
		return &metapb.StatFileResponse{Status: 1, Error: err.Error()}, nil
	}
	return &metapb.StatFileResponse{Status: 0, Metadata: md}, nil
}

// Subscribe opens a server-streaming channel for cache invalidation notices.
// The goroutine blocks here until the client disconnects.
func (s *Server) Subscribe(req *metapb.SubscribeRequest, stream metapb.MetadataService_SubscribeServer) error {
	cid := req.ClientId
	s.subs.add(cid, stream)
	defer s.subs.remove(cid)

	// Block until client disconnects or server shuts down
	<-stream.Context().Done()
	return nil
}
