// Package kv implements the JSON key/value state machine.
package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"unicode/utf8"

	"github.com/kestfor/raft/pkg/raft"
)

// MaxValueSize accounts for base64 and the shortest possible v1 put envelope.
const MaxValueSize = (raft.MaxCommandSize - len(`{"version":1,"op":"put","key":"","value":""}`)) / 4 * 3

var (
	ErrTooLarge    = errors.New("encoded KV command exceeds Raft size limit")
	ErrNotFound    = errors.New("key not found")
	ErrInvalid     = errors.New("invalid command")
	ErrNotLeader   = errors.New("not leader")
	ErrUnavailable = errors.New("store unavailable")
)

type Record struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type Status struct {
	ID           string `json:"id"`
	Role         string `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
	Available    bool   `json:"available"`
}

type Log interface {
	Propose(context.Context, []byte) (uint64, any, error)
	ReadIndex(context.Context) (uint64, error)
	LogEntries() []raft.Entry
	Status() Status
}

type command struct {
	Version int    `json:"version"`
	Op      string `json:"op"`
	Key     string `json:"key"`
	Value   []byte `json:"value"`
}

type Store struct {
	log    Log
	mu     sync.RWMutex
	values map[string][]byte
}

var _ raft.StateMachine = (*Store)(nil)

func New(log Log) *Store { return &Store{log: log, values: make(map[string][]byte)} }

func decode(data []byte) (command, error) {
	var cmd command
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cmd); err != nil {
		return cmd, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return cmd, fmt.Errorf("trailing command data")
	}
	return cmd, cmd.validate()
}

func (s *Store) Do(entry raft.Entry) (any, error) {
	cmd, err := decode(entry.Data)
	if err != nil {
		return nil, fmt.Errorf("entry %d: %w", entry.Index, err)
	}
	s.mu.Lock()
	s.values[cmd.Key] = slices.Clone(cmd.Value)
	s.mu.Unlock()
	return Record{Key: cmd.Key, Value: slices.Clone(cmd.Value)}, nil
}

func ValidValue(value []byte) bool {
	value = bytes.TrimSpace(value)
	return json.Valid(value) && !bytes.Equal(value, []byte("true")) && !bytes.Equal(value, []byte("false"))
}

func (c command) validate() error {
	if c.Version != 1 || c.Op != "put" || !utf8.ValidString(c.Key) || !ValidValue(c.Value) {
		return fmt.Errorf("%w: unsupported command or JSON value", ErrInvalid)
	}
	if len(c.Value) > MaxValueSize {
		return ErrTooLarge
	}
	return nil
}

func (s *Store) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	cmd := command{Version: 1, Op: "put", Key: key, Value: value}
	if err := cmd.validate(); err != nil {
		return 0, err
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return 0, err
	}
	// Escaping the key and encoding the value both count toward the Raft command.
	if len(data) > raft.MaxCommandSize {
		return 0, ErrTooLarge
	}
	index, _, err := s.log.Propose(ctx, data)
	return index, err
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" || !utf8.ValidString(key) {
		return nil, fmt.Errorf("%w: key must be nonempty UTF-8", ErrInvalid)
	}
	if _, err := s.log.ReadIndex(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[key]
	if !ok {
		return nil, ErrNotFound
	}
	return slices.Clone(value), nil
}

func (s *Store) Log() ([]Record, error) {
	entries := s.log.LogEntries()
	records := make([]Record, 0, len(entries))
	for _, entry := range entries {
		if entry.NoOp {
			continue
		}
		cmd, err := decode(entry.Data)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", entry.Index, err)
		}
		records = append(records, Record{Key: cmd.Key, Value: cmd.Value})
	}
	return records, nil
}

func (s *Store) Status() Status { return s.log.Status() }
