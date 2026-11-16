package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/kestfor/raft/pkg/raft"
)

type testLog struct {
	store   *Store
	entries []raft.Entry
	readErr error
}

func (l *testLog) Propose(_ context.Context, data []byte) (uint64, any, error) {
	index := uint64(len(l.entries) + 1)
	entry := raft.Entry{Index: index, Term: 1, Data: bytes.Clone(data)}
	l.entries = append(l.entries, entry)
	result, err := l.store.Do(entry)
	return index, result, err
}

func (l *testLog) ReadIndex(context.Context) (uint64, error) {
	return uint64(len(l.entries)), l.readErr
}
func (l *testLog) LogEntries() []raft.Entry { return l.entries }
func (*testLog) Status() Status             { return Status{Role: "leader", Available: true} }

func TestJSONValuesAndLogOrder(t *testing.T) {
	log := &testLog{entries: []raft.Entry{{Index: 1, Term: 1, NoOp: true}}}
	store := New(log)
	log.store = store
	for _, value := range []string{`"text"`, `9007199254740993`, `null`, `[]`, `{"nested":[true,null,1]}`} {
		if _, err := store.Put(t.Context(), "key", []byte(value)); err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(t.Context(), "key")
		if err != nil || string(got) != value {
			t.Fatalf("got=%s err=%v", got, err)
		}
		got[0] = 'X'
		again, _ := store.Get(t.Context(), "key")
		if string(again) != value {
			t.Fatal("returned value aliases state")
		}
	}
	records, err := store.Log()
	if err != nil || len(records) != 5 || string(records[0].Value) != `"text"` ||
		string(records[4].Value) != `{"nested":[true,null,1]}` {
		t.Fatalf("records=%v err=%v", records, err)
	}
	log.readErr = ErrUnavailable
	if _, err := store.Get(t.Context(), "key"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestDoRejectsMalformedCommands(t *testing.T) {
	store := New(&testLog{})
	for _, data := range []string{`raw`, `{"version":2,"op":"put","key":"k"}`, `{"version":1,"op":"put","key":"k"} {}`, `{"version":1,"op":"put","key":"k","unknown":true}`} {
		if _, err := store.Do(raft.Entry{Index: 1, Data: []byte(data)}); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	for _, value := range []string{`true`, `false`, `invalid`, ``} {
		if _, err := store.Put(t.Context(), "k", []byte(value)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("value=%s err=%v", value, err)
		}
	}
}

func TestDoResultRemainsIndependent(t *testing.T) {
	store := New(&testLog{})
	commandData := func(value string) []byte {
		data, _ := json.Marshal(command{Version: 1, Op: "put", Key: "k", Value: []byte(value)})
		return data
	}
	result, err := store.Do(raft.Entry{Index: 1, Data: commandData(`1`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Do(raft.Entry{Index: 2, Data: commandData(`2`)}); err != nil {
		t.Fatal(err)
	}
	if string(result.(Record).Value) != "1" {
		t.Fatal("later apply changed earlier result")
	}
}
