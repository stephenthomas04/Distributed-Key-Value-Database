// Package client discovers the leader before submitting an operation.
// It never retries an operation, including when its outcome is ambiguous.
package client

import (
	"context"
	"fmt"

	"github.com/stephenthomas04/DistributedKeyValueDatabase/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
func Leader(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	c, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	s, err := api.NewKeyValueClient(c).Status(ctx, &api.Empty{})
	if err != nil {
		c.Close()
		return nil, err
	}
	if s.State == "Leader" {
		return c, nil
	}
	c.Close()
	if s.LeaderAddress == "" {
		return nil, fmt.Errorf("no known leader; try again after election")
	}
	return Dial(s.LeaderAddress)
}
