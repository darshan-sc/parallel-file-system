package fileserver

import (
	"context"

	fspb "pfs/gen/fileserverpb"
)

// Server implements the FileService gRPC interface.
type Server struct {
	fspb.UnimplementedFileServiceServer
	storage *Storage
}

func NewServer(storage *Storage) *Server {
	return &Server{storage: storage}
}

func (s *Server) ReadBytes(ctx context.Context, req *fspb.ReadRequest) (*fspb.ReadResponse, error) {
	data, err := s.storage.ReadAt(req.Filename, req.Offset, req.Length)
	if err != nil {
		return nil, err
	}
	return &fspb.ReadResponse{Data: data}, nil
}

func (s *Server) WriteBytes(ctx context.Context, req *fspb.WriteRequest) (*fspb.WriteResponse, error) {
	n, err := s.storage.WriteAt(req.Filename, req.Offset, req.Data)
	if err != nil {
		return nil, err
	}
	return &fspb.WriteResponse{BytesWritten: n}, nil
}
