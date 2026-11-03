package service_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stephenthomas04/DistributedKeyValueDatabase/api"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/client"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/consensus"
	"github.com/stephenthomas04/DistributedKeyValueDatabase/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The helper is a real child process. Its stdin/stdout control channel is test
// only; it allows direct follower-state inspection without weakening the API.
func TestNodeProcess(t *testing.T) {
	if os.Getenv("KV_TEST_CHILD") != "1" {
		return
	}
	peers, err := consensus.ParsePeers(os.Getenv("KV_PEERS"))
	if err != nil {
		panic(err)
	}
	id := os.Getenv("KV_ID")
	var peer consensus.Peer
	for _, p := range peers {
		if p.ID == id {
			peer = p
		}
	}
	n, err := consensus.Open(id, peer.Raft, os.Getenv("KV_DIR"), peers, id == peers[0].ID)
	if err != nil {
		panic(err)
	}
	l, err := net.Listen("tcp", peer.Client)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	api.RegisterKeyValueServer(s, &service.Service{Node: n})
	go s.Serve(l)
	scanner := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "snapshot" {
			e := n.Raft.Snapshot().Error()
			msg := ""
			if e != nil {
				msg = e.Error()
			}
			_ = enc.Encode(map[string]string{"error": msg})
		} else {
			v, ok := n.Store.Get(line)
			_ = enc.Encode(map[string]interface{}{"value": string(v), "found": ok})
		}
	}
	s.Stop()
	n.Close()
	os.Exit(0)
}

type process struct {
	cmd           *exec.Cmd
	input         io.WriteCloser
	output        *bufio.Reader
	log           *os.File
	addr, dir, id string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	l.Close()
	return a
}
func (p *process) start(t *testing.T, peers string) {
	t.Helper()
	p.cmd = exec.Command(os.Args[0], "-test.run=^TestNodeProcess$")
	p.cmd.Env = append(os.Environ(), "KV_TEST_CHILD=1", "KV_ID="+p.id, "KV_DIR="+p.dir, "KV_PEERS="+peers)
	var err error
	p.input, err = p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.output = bufio.NewReader(out)
	p.log, err = os.OpenFile(p.dir+".log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = p.log
	if err = p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
}
func (p *process) kill() {
	if p.cmd != nil {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
		p.input.Close()
		p.log.Close()
		p.cmd = nil
	}
}
func (p *process) inspect(t *testing.T, key string) map[string]interface{} {
	t.Helper()
	if _, err := fmt.Fprintln(p.input, key); err != nil {
		t.Fatal(err)
	}
	type result struct {
		b []byte
		e error
	}
	ch := make(chan result, 1)
	go func() { b, e := p.output.ReadBytes('\n'); ch <- result{b, e} }()
	select {
	case r := <-ch:
		if r.e != nil {
			t.Fatal(r.e)
		}
		var v map[string]interface{}
		if e := json.Unmarshal(r.b, &v); e != nil {
			t.Fatalf("%s: %v", r.b, e)
		}
		return v
	case <-time.After(8 * time.Second):
		t.Fatal("child control channel timeout")
	}
	return nil
}
func poll(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}
func statusOf(p *process) (*api.StatusResponse, error) {
	c, e := client.Dial(p.addr)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	return api.NewKeyValueClient(c).Status(ctx, &api.Empty{})
}
func leader(t *testing.T, ps []*process) *process {
	t.Helper()
	var found *process
	poll(t, "leader election", func() bool {
		for _, p := range ps {
			if p.cmd == nil {
				continue
			}
			s, e := statusOf(p)
			if e == nil && s.State == "Leader" {
				found = p
				return true
			}
		}
		return false
	})
	return found
}
func rpc(t *testing.T, p *process, fn func(context.Context, api.KeyValueClient) error) error {
	t.Helper()
	c, e := client.Dial(p.addr)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return fn(ctx, api.NewKeyValueClient(c))
}
func put(t *testing.T, p *process, k, v string) {
	t.Helper()
	if e := rpc(t, p, func(ctx context.Context, c api.KeyValueClient) error {
		_, e := c.Put(ctx, &api.PutRequest{Key: k, Value: []byte(v)})
		return e
	}); e != nil {
		t.Fatal(e)
	}
}
func get(t *testing.T, p *process, k, want string) {
	t.Helper()
	if e := rpc(t, p, func(ctx context.Context, c api.KeyValueClient) error {
		r, e := c.Get(ctx, &api.KeyRequest{Key: k})
		if e == nil && string(r.Value) != want {
			return fmt.Errorf("got %q want %q", r.Value, want)
		}
		return e
	}); e != nil {
		t.Fatal(e)
	}
}
func TestClusterRecovery(t *testing.T) {
	var ps []*process
	var entries []string
	root := t.TempDir()
	for i := 1; i <= 3; i++ {
		p := &process{id: fmt.Sprintf("node%d", i), addr: freeAddr(t), dir: filepath.Join(root, fmt.Sprintf("node%d", i))}
		ps = append(ps, p)
		entries = append(entries, p.id+"="+freeAddr(t)+"="+p.addr)
	}
	peers := strings.Join(entries, ",")
	t.Cleanup(func() {
		for _, p := range ps {
			p.kill()
			if t.Failed() {
				b, _ := os.ReadFile(p.dir + ".log")
				t.Log(string(b))
			}
		}
	})
	for _, p := range ps {
		p.start(t, peers)
	}
	l := leader(t, ps)
	put(t, l, "before", "acknowledged")
	get(t, l, "before", "acknowledged")
	for _, p := range ps {
		poll(t, "replication", func() bool { return p.inspect(t, "before")["value"] == "acknowledged" })
	}
	e := rpc(t, l, func(ctx context.Context, c api.KeyValueClient) error {
		_, e := c.Get(ctx, &api.KeyRequest{Key: "missing"})
		return e
	})
	if status.Code(e) != codes.NotFound {
		t.Fatal(e)
	}
	e = rpc(t, l, func(ctx context.Context, c api.KeyValueClient) error { _, e := c.Put(ctx, &api.PutRequest{}); return e })
	if status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	for _, p := range ps {
		if p != l {
			e = rpc(t, p, func(ctx context.Context, c api.KeyValueClient) error {
				_, e := c.Get(ctx, &api.KeyRequest{Key: "before"})
				return e
			})
			if status.Code(e) != codes.FailedPrecondition {
				t.Fatal(e)
			}
		}
	}
	put(t, l, "deleted", "value")
	e = rpc(t, l, func(ctx context.Context, c api.KeyValueClient) error {
		_, e := c.Delete(ctx, &api.KeyRequest{Key: "deleted"})
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
	failed := l
	l.kill()
	l = leader(t, ps)
	get(t, l, "before", "acknowledged")
	put(t, l, "after", "new leader")
	failed.start(t, peers)
	poll(t, "restarted follower catch-up", func() bool { return failed.inspect(t, "after")["value"] == "new leader" })
	// Force real on-disk snapshots, then add a log-only write to test both paths.
	for _, p := range ps {
		poll(t, "apply before snapshot", func() bool { return p.inspect(t, "after")["value"] == "new leader" })
		if r := p.inspect(t, "snapshot"); r["error"] != "" {
			t.Fatal(r)
		}
	}
	put(t, l, "log-tail", "replayed")
	for _, p := range ps {
		poll(t, "tail replication", func() bool { return p.inspect(t, "log-tail")["value"] == "replayed" })
	}
	for _, p := range ps {
		p.kill()
	}
	for _, p := range ps {
		p.start(t, peers)
	}
	l = leader(t, ps)
	get(t, l, "before", "acknowledged")
	get(t, l, "after", "new leader")
	get(t, l, "log-tail", "replayed")
	e = rpc(t, l, func(ctx context.Context, c api.KeyValueClient) error {
		_, e := c.Get(ctx, &api.KeyRequest{Key: "deleted"})
		return e
	})
	if status.Code(e) != codes.NotFound {
		t.Fatal(e)
	}
	for _, p := range ps {
		if p != l {
			p.kill()
		}
	}
	for _, op := range []string{"read", "write", "delete"} {
		start := time.Now()
		e = rpc(t, l, func(ctx context.Context, c api.KeyValueClient) error {
			switch op {
			case "read":
				_, e := c.Get(ctx, &api.KeyRequest{Key: "before"})
				return e
			case "delete":
				_, e := c.Delete(ctx, &api.KeyRequest{Key: "before"})
				return e
			default:
				_, e := c.Put(ctx, &api.PutRequest{Key: "no-quorum", Value: []byte("x")})
				return e
			}
		})
		if e == nil {
			t.Fatalf("%s succeeded without majority", op)
		}
		if time.Since(start) > 3*time.Second {
			t.Fatal("deadline not respected")
		}
	}
}
