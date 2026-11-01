package kv

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

type testLog struct {
	entries  chan []Entry
	proposed chan []byte
}

func (l *testLog) Propose(_ context.Context, data []byte) (uint64, error) {
	l.proposed <- data
	return 1, nil
}
func (*testLog) ReadIndex(context.Context) (uint64, error) { return 1, nil }
func (*testLog) Status() Status                            { return Status{Role: "leader"} }
func (l *testLog) CommittedEntries(ctx context.Context, _ uint64) ([]Entry, error) {
	select {
	case entries := <-l.entries:
		return entries, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestPutAndGetWaitForApply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &testLog{entries: make(chan []Entry), proposed: make(chan []byte, 1)}
		store := New(log)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- store.Run(ctx) }()
		synctest.Wait()
		put := make(chan error, 1)
		go func() { _, err := store.Put(ctx, "k", []byte("v")); put <- err }()
		data := <-log.proposed
		get := make(chan error, 1)
		go func() {
			v, err := store.Get(ctx, "k")
			if err == nil && string(v) != "v" {
				err = errors.New("wrong value")
			}

			get <- err
		}()
		synctest.Wait()
		select {
		case <-put:
			t.Fatal("put returned before apply")
		default:
		}

		select {
		case <-get:
			t.Fatal("get returned before apply")
		default:
		}

		log.entries <- []Entry{{Index: 1, Data: data}}
		if err := <-put; err != nil {
			t.Fatal(err)
		}

		if err := <-get; err != nil {
			t.Fatal(err)
		}

		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestInvalidCommandsStopStore(t *testing.T) {
	for _, data := range []string{`raw`, `{"version":2,"op":"put","key":"k"}`, `{"version":1,"op":"put","key":"k"} {}`, `{"version":1,"op":"put","key":"k","unknown":true}`} {
		log := &testLog{entries: make(chan []Entry, 1)}
		log.entries <- []Entry{{Index: 1, Data: []byte(data)}}
		store := New(log)
		if err := store.Run(t.Context()); err == nil {
			t.Fatal("invalid command accepted")
		}

		if _, err := store.Get(t.Context(), "k"); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
	}
}
