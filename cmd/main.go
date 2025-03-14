package main

import (
	"fmt"
	"log"
	"net"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/server"
	"google.golang.org/grpc"
)

const (
	port = "50051"
)

func main() {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftLockServer(grpcServer, &server.RaftLockServer{})

	fmt.Printf("RaftLock gRPC server running on port %s\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
