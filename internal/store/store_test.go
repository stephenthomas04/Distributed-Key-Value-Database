package store

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func apply(t *testing.T, s *Store, c Command) {
	t.Helper()
	b, _ := json.Marshal(c)
	if e := s.Apply(&raft.Log{Data: b}); e != nil {
		t.Fatal(e)
	}
}

type sink struct{ bytes.Buffer }

func (s *sink) ID() string    { return "test" }
func (s *sink) Close() error  { return nil }
func (s *sink) Cancel() error { return nil }
func TestStateAndSnapshot(t *testing.T) {
	s := New()
	apply(t, s, Command{Op: "put", Key: "a", Value: []byte("one")})
	v, ok := s.Get("a")
	if !ok || string(v) != "one" {
		t.Fatal(v, ok)
	}
	v[0] = 'X'
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()
	apply(t, s, Command{Op: "put", Key: "a", Value: []byte("two")})
	v, _ = s.Get("a")
	if string(v) != "two" {
		t.Fatal(string(v))
	}
	out := &sink{}
	if err := snap.Persist(out); err != nil {
		t.Fatal(err)
	}
	restored := New()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(out.Bytes()))); err != nil {
		t.Fatal(err)
	}
	v, ok = restored.Get("a")
	if !ok || string(v) != "one" {
		t.Fatal(v, ok)
	}
	apply(t, s, Command{Op: "delete", Key: "a"})
	apply(t, s, Command{Op: "delete", Key: "a"})
	if _, ok := s.Get("a"); ok {
		t.Fatal("delete failed")
	}
	if err := restored.Restore(io.NopCloser(bytes.NewBufferString("broken"))); err == nil {
		t.Fatal("accepted bad snapshot")
	}
}
func TestValidation(t *testing.T) {
	for _, c := range []Command{{Op: "put"}, {Op: "put", Key: string(bytes.Repeat([]byte("x"), 257))}, {Op: "put", Key: "a", Value: make([]byte, MaxValueBytes+1)}, {Op: "bad", Key: "a"}} {
		b, _ := json.Marshal(c)
		if New().Apply(&raft.Log{Data: b}) == nil {
			t.Fatal("accepted invalid command")
		}
	}
	if err := Validate("a", nil); err != nil {
		t.Fatal(err)
	}
}
