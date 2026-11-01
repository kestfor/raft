// Package kv applies a replicated command stream to an in-memory key/value store.
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
)

const (
	MaxKeySize   = 4096
	MaxValueSize = 512 << 10
)

var (
	ErrNotFound    = errors.New("key not found")
	ErrInvalid     = errors.New("invalid command")
	ErrNotLeader   = errors.New("not leader")
	ErrUnavailable = errors.New("store unavailable")
)

type Entry struct {
	Index uint64
	Data  []byte
	NoOp  bool
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

// Log is the only consensus dependency. Entries must be committed and ordered.
type Log interface {
	Propose(context.Context, []byte) (uint64, error)
	ReadIndex(context.Context) (uint64, error)
	CommittedEntries(context.Context, uint64) ([]Entry, error)
	Status() Status
}

type command struct {
	Version int    `json:"version"`
	Op      string `json:"op"`
	Key     string `json:"key"`
	Value   []byte `json:"value,omitempty"`
}

type Store struct {
	log Log

	mu      sync.Mutex
	values  map[string][]byte
	applied uint64
	changed chan struct{}

	started bool
	running bool
	failure error
}

func New(log Log) *Store {
	return &Store{log: log, values: make(map[string][]byte), changed: make(chan struct{})}
}

// Run is called once. Every member applies the same committed stream, including
// no-ops. A decode/apply failure stops service instead of exposing stale data.
func (s *Store) Run(ctx context.Context) (result error) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("store already started")
	}

	s.started, s.running = true, true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.running = false
		s.failure = result
		s.notify()
	}()

	var after uint64
	for {
		entries, err := s.log.CommittedEntries(ctx, after)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}

		for _, entry := range entries {
			if err := s.apply(entry); err != nil {
				return err
			}

			after = entry.Index
		}
	}
}

func (s *Store) apply(entry Entry) error {
	var cmd command
	if !entry.NoOp {
		decoder := json.NewDecoder(bytes.NewReader(entry.Data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cmd); err != nil {
			return fmt.Errorf("decode entry %d: %w", entry.Index, err)
		}

		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return fmt.Errorf("entry %d: trailing command data", entry.Index)
		}

		if err := cmd.validate(); err != nil {
			return fmt.Errorf("entry %d: %w", entry.Index, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.Index != s.applied+1 {
		return fmt.Errorf("non-contiguous applied index %d", entry.Index)
	}

	if !entry.NoOp {
		switch cmd.Op {
		case "put":
			s.values[cmd.Key] = slices.Clone(cmd.Value)
		case "delete":
			delete(s.values, cmd.Key)
		}
	}

	s.applied = entry.Index
	s.notify()

	return nil
}

func (c command) validate() error {
	if c.Version != 1 || (c.Op != "put" && c.Op != "delete") {
		return fmt.Errorf("%w: unsupported version or operation", ErrInvalid)
	}

	if err := validateKey(c.Key); err != nil {
		return err
	}

	if len(c.Value) > MaxValueSize || (c.Op == "delete" && len(c.Value) != 0) {
		return fmt.Errorf("%w: invalid value size", ErrInvalid)
	}

	return nil
}

func validateKey(key string) error {
	if key == "" || len(key) > MaxKeySize || !utf8.ValidString(key) {
		return fmt.Errorf("%w: key must be valid UTF-8, 1..%d bytes", ErrInvalid, MaxKeySize)
	}

	return nil
}

func (s *Store) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return s.replicate(ctx, command{Version: 1, Op: "put", Key: key, Value: value})
}

func (s *Store) Delete(ctx context.Context, key string) (uint64, error) {
	return s.replicate(ctx, command{Version: 1, Op: "delete", Key: key})
}

func (s *Store) replicate(ctx context.Context, cmd command) (uint64, error) {
	if err := cmd.validate(); err != nil {
		return 0, err
	}

	if err := s.waitApplied(ctx, 0); err != nil {
		return 0, err
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return 0, err
	}

	index, err := s.log.Propose(ctx, data)
	if err != nil {
		return index, err
	}

	return index, s.waitApplied(ctx, index)
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	if err := s.waitApplied(ctx, 0); err != nil {
		return nil, err
	}

	index, err := s.log.ReadIndex(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.waitApplied(ctx, index); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil, ErrUnavailable
	}

	value, ok := s.values[key]
	if !ok {
		return nil, ErrNotFound
	}

	return slices.Clone(value), nil
}

func (s *Store) waitApplied(ctx context.Context, index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if !s.running {
			return errors.Join(ErrUnavailable, s.failure)
		}

		if s.applied >= index {
			return nil
		}

		changed := s.changed
		s.mu.Unlock()

		select {
		case <-ctx.Done():
		case <-changed:
		}

		s.mu.Lock()
	}
}

func (s *Store) Status() Status {
	state := s.log.Status()
	s.mu.Lock()
	defer s.mu.Unlock()

	state.AppliedIndex = s.applied
	state.Available = s.running

	return state
}

func (s *Store) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}
