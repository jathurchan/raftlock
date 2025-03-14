package server

import (
	"context"
	"fmt"

	pb "github.com/jathurchan/raftlock/proto"
)

type RaftLockServer struct {
	pb.UnimplementedRaftLockServer
}

func (s *RaftLockServer) Lock(ctx context.Context, req *pb.LockRequest) (*pb.LockResponse, error) {
	fmt.Printf("Received Lock request for resource: %s from client: %s\n", req.ResourceId, req.ClientId)
	return &pb.LockResponse{Success: true, Message: "Lock acquired (log-only for now)"}, nil
}

func (s *RaftLockServer) Unlock(ctx context.Context, req *pb.UnlockRequest) (*pb.UnlockResponse, error) {
	fmt.Printf("Received Unlock request for resource: %s from client: %s\n", req.ResourceId, req.ClientId)
	return &pb.UnlockResponse{Success: true, Message: "Unlock successful (log-only for now)"}, nil
}
