// Package store implements the deterministic Raft state machine.
package store

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

const MaxKeyBytes = 256
const MaxValueBytes = 1024 * 1024

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func Validate(key string, value []byte) error {
	if len(key) == 0 || len(key) > MaxKeyBytes {
		return fmt.Errorf("key must be 1–%d bytes", MaxKeyBytes)
	}
	if len(value) > MaxValueBytes {
		return fmt.Errorf("value exceeds %d bytes", MaxValueBytes)
	}
	return nil
}

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *Store { return &Store{data: make(map[string][]byte)} }
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return append([]byte(nil), v...), ok
}
func (s *Store) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return err
	}
	if err := Validate(c.Key, c.Value); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c.Op {
	case "put":
		s.data[c.Key] = append([]byte(nil), c.Value...)
	case "delete":
		delete(s.data, c.Key)
	default:
		return fmt.Errorf("unknown operation %q", c.Op)
	}
	return nil
}

type snapshot struct{ data map[string][]byte }

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		data[k] = append([]byte(nil), v...)
	}
	return &snapshot{data}, nil
}
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}
func (s *snapshot) Release() {}
func (s *Store) Restore(r io.ReadCloser) error {
	defer r.Close()
	var data map[string][]byte
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("invalid null snapshot")
	}
	for k, v := range data {
		if err := Validate(k, v); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
	return nil
}
