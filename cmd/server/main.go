package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stephenthomas04/DistributedKeyValueDatabase/api"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/consensus"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/service"
	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "", "fixed node ID")
	client := flag.String("listen", ":7001", "gRPC bind address")
	rb := flag.String("raft", ":9001", "Raft bind address")
	dir := flag.String("data", "data", "persistent data directory")
	peers := flag.String("peers", "", "three id=raft-address=client-address entries, comma separated")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap first node only if no state exists")
	flag.Parse()
	ps, err := consensus.ParsePeers(*peers)
	if err != nil {
		log.Fatal(err)
	}
	n, err := consensus.Open(*id, *rb, *dir, ps, *bootstrap)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *client)
	if err != nil {
		n.Close()
		log.Fatal(err)
	}
	server := grpc.NewServer()
	api.RegisterKeyValueServer(server, &service.Service{Node: n})
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, os.Interrupt, syscall.SIGTERM)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	log.Printf("node %s listening on %s", *id, *client)
	select {
	case <-stopped:
	case err := <-serveErr:
		log.Printf("gRPC stopped: %v", err)
	}
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		server.Stop()
	}
	if err := n.Close(); err != nil {
		log.Print(err)
	}
}
