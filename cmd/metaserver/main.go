package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"

	metapb "pfs/gen/metadatapb"
	"pfs/pkg/metaserver"
)

func main() {
	port := flag.Int("port", 50051, "gRPC listen port")
	fileServers := flag.String("file-servers", "localhost:50052", "comma-separated file server addresses")
	flag.Parse()

	addrs := strings.Split(*fileServers, ",")
	srv := grpc.NewServer()
	metapb.RegisterMetadataServiceServer(srv, metaserver.NewServer(addrs))

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("metadata server listening on :%d (file-servers=%v)", *port, addrs)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
