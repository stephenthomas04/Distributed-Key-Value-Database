package service

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/api"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/consensus"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Service struct {
	api.UnimplementedKeyValueServer
	Node *consensus.Node
}

func (s *Service) validate(ctx context.Context, key string, value []byte) error {
	if err := store.Validate(key, value); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if s.Node.Raft.State() != raft.Leader {
		leader := s.Node.LeaderAddress()
		_ = grpc.SetTrailer(ctx, metadata.Pairs("leader-address", leader))
		return status.Errorf(codes.FailedPrecondition, "not leader; leader=%s", leader)
	}
	return nil
}
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.FromContextError(err).Err()
	}
	return status.Errorf(codes.Unavailable, "raft operation failed (mutation outcome may be unknown): %v", err)
}
func (s *Service) Put(ctx context.Context, r *api.PutRequest) (*api.Empty, error) {
	return s.mutate(ctx, store.Command{Op: "put", Key: r.Key, Value: r.Value})
}
func (s *Service) Delete(ctx context.Context, r *api.KeyRequest) (*api.Empty, error) {
	return s.mutate(ctx, store.Command{Op: "delete", Key: r.Key})
}
func (s *Service) mutate(ctx context.Context, command store.Command) (*api.Empty, error) {
	if err := s.validate(ctx, command.Key, command.Value); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.Node.Apply(ctx, command); err != nil {
		return nil, rpcError(err)
	}
	return &api.Empty{}, nil
}
func (s *Service) Get(ctx context.Context, r *api.KeyRequest) (*api.GetResponse, error) {
	if err := s.validate(ctx, r.Key, nil); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, ok, err := s.Node.Read(ctx, r.Key)
	if err != nil {
		return nil, rpcError(err)
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &api.GetResponse{Value: v}, nil
}
func (s *Service) Status(ctx context.Context, _ *api.Empty) (*api.StatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, rpcError(err)
	}
	return &api.StatusResponse{NodeId: s.Node.ID, LeaderAddress: s.Node.LeaderAddress(), State: s.Node.Raft.State().String()}, nil
}
