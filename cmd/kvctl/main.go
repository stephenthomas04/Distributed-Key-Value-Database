package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/stephenthomas04/DistributedKeyValueDatabase/api"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/client"
)

func run() error {
	addr := flag.String("addr", "localhost:7001", "address of any reachable node")
	timeout := flag.Duration("timeout", 5*time.Second, "overall request deadline")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("usage: kvctl [--addr host:port] put KEY VALUE | get KEY | delete KEY | status")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if args[0] == "status" && len(args) == 1 {
		c, err := client.Dial(*addr)
		if err != nil {
			return err
		}
		defer c.Close()
		s, e := api.NewKeyValueClient(c).Status(ctx, &api.Empty{})
		if e != nil {
			return e
		}
		fmt.Printf("node=%s state=%s leader=%s\n", s.NodeId, s.State, s.LeaderAddress)
		return nil
	}
	if !((args[0] == "put" && len(args) == 3) || ((args[0] == "get" || args[0] == "delete") && len(args) == 2)) {
		return fmt.Errorf("invalid command or argument count")
	}
	leader, err := client.Leader(ctx, *addr)
	if err != nil {
		return err
	}
	defer leader.Close()
	kv := api.NewKeyValueClient(leader)
	switch args[0] {
	case "put":
		_, err = kv.Put(ctx, &api.PutRequest{Key: args[1], Value: []byte(args[2])})
	case "delete":
		_, err = kv.Delete(ctx, &api.KeyRequest{Key: args[1]})
	case "get":
		var r *api.GetResponse
		r, err = kv.Get(ctx, &api.KeyRequest{Key: args[1]})
		if err == nil {
			fmt.Println(string(r.Value))
		}
	}
	if err != nil && args[0] != "get" {
		return fmt.Errorf("%w; mutation was not retried; a timeout or transport failure may mean it committed", err)
	}
	return err
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
