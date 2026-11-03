package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/store"
)

type Peer struct{ ID, Raft, Client string }

// ParsePeers accepts exactly three fixed voters: id=raft-address=client-address.
func ParsePeers(raw string) ([]Peer, error) {
	var peers []Peer
	ids, addrs, clients := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, entry := range strings.Split(raw, ",") {
		p := strings.Split(entry, "=")
		if len(p) != 3 || p[0] == "" {
			return nil, fmt.Errorf("peers must be id=raft-address=client-address")
		}
		for _, a := range p[1:] {
			if _, _, err := net.SplitHostPort(a); err != nil {
				return nil, err
			}
		}
		if ids[p[0]] || addrs[p[1]] || clients[p[2]] {
			return nil, fmt.Errorf("duplicate peer")
		}
		ids[p[0]], addrs[p[1]], clients[p[2]] = true, true, true
		peers = append(peers, Peer{p[0], p[1], p[2]})
	}
	if len(peers) != 3 {
		return nil, fmt.Errorf("exactly three peers required")
	}
	return peers, nil
}

type Node struct {
	ID        string
	Peers     []Peer
	Raft      *raft.Raft
	Store     *store.Store
	db        *boltdb.BoltStore
	transport *raft.NetworkTransport
}

func Open(id, bind, dir string, peers []Peer, bootstrap bool) (*Node, error) {
	found := false
	var advertised string
	for _, p := range peers {
		if p.ID == id {
			found = true
			advertised = p.Raft
		}
	}
	if !found {
		return nil, fmt.Errorf("node ID missing from peers")
	}
	if bootstrap && id != peers[0].ID {
		return nil, fmt.Errorf("only the first configured node may bootstrap")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	db, err := boltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(dir, 2, io.Discard)
	if err != nil {
		db.Close()
		return nil, err
	}
	existing, err := raft.HasExistingState(db, db, snapshots)
	if err != nil {
		db.Close()
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", advertised)
	if err != nil {
		db.Close()
		return nil, err
	}
	transport, err := raft.NewTCPTransport(bind, addr, 3, time.Second, io.Discard)
	if err != nil {
		db.Close()
		return nil, err
	}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	config.LogOutput = io.Discard
	config.SnapshotThreshold = 128
	config.SnapshotInterval = 5 * time.Second
	config.TrailingLogs = 64
	state := store.New()
	r, err := raft.NewRaft(config, state, db, db, snapshots, transport)
	if err != nil {
		transport.Close()
		db.Close()
		return nil, err
	}
	n := &Node{id, peers, r, state, db, transport}
	if bootstrap && !existing {
		cfg := raft.Configuration{}
		for _, p := range peers {
			cfg.Servers = append(cfg.Servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Raft), Suffrage: raft.Voter})
		}
		if err := r.BootstrapCluster(cfg).Error(); err != nil {
			n.Close()
			return nil, err
		}
	}
	return n, nil
}
func (n *Node) Close() error {
	err := n.Raft.Shutdown().Error()
	_ = n.transport.Close()
	if e := n.db.Close(); err == nil {
		err = e
	}
	return err
}
func (n *Node) LeaderAddress() string {
	_, id := n.Raft.LeaderWithID()
	for _, p := range n.Peers {
		if p.ID == string(id) {
			return p.Client
		}
	}
	return ""
}

// Raft futures do not support cancellation. A buffered waiter lets the RPC
// return on deadline; the submitted mutation may still commit afterwards.
func wait(ctx context.Context, f raft.Future) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func remaining(ctx context.Context) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		v := time.Until(d)
		if v > 0 {
			return v
		}
		return time.Nanosecond
	}
	return 5 * time.Second
}
func (n *Node) Apply(ctx context.Context, c store.Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := n.Raft.Apply(b, remaining(ctx))
	if err := wait(ctx, f); err != nil {
		return err
	}
	if e, ok := f.Response().(error); ok {
		return e
	}
	return nil
}
func (n *Node) Read(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := wait(ctx, n.Raft.VerifyLeader()); err != nil {
		return nil, false, err
	}
	if err := wait(ctx, n.Raft.Barrier(remaining(ctx))); err != nil {
		return nil, false, err
	}
	v, ok := n.Store.Get(key)
	return v, ok, nil
}
