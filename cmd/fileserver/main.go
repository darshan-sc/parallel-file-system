package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	fspb "pfs/gen/fileserverpb"
	"pfs/pkg/fileserver"
)

func main() {
	port := flag.Int("port", 50052, "gRPC listen port")
	dataDir := flag.String("data-dir", "/tmp/pfs-data", "directory for block storage")
	flag.Parse()

	storage, err := fileserver.NewStorage(*dataDir)
	if err != nil {
		log.Fatalf("storage init: %v", err)
	}

	srv := grpc.NewServer()
	fspb.RegisterFileServiceServer(srv, fileserver.NewServer(storage))

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("file server listening on :%d (data-dir=%s)", *port, *dataDir)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
