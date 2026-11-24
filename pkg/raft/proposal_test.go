package raft

import (
	"encoding/json"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kestfor/raft/pkg/wal"
)

func TestConcurrentProposalsAppendImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := &wal.Memory{}
		cfg := testConfig("a", nil)
		cfg.Storage = journal
		n, cancel, done := startStateMachineNode(t, cfg)
		defer stopStateMachineNode(t, cancel, done)
		time.Sleep(150 * time.Millisecond)

		const count = 4
		results := make(chan error, count)
		start := time.Now()
		for i := range count {
			go func() {
				_, _, err := n.Propose(t.Context(), []byte{byte(i)})
				results <- err
			}()
		}
		for range count {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("proposals waited for a timer: %s", elapsed)
		}
		commands := 0
		err := journal.Replay(func(data []byte) error {
			var record diskRecord
			if err := json.Unmarshal(data, &record); err != nil {
				return err
			}
			if len(record.Entries) == 0 {
				return nil
			}
			if len(record.Entries) != 1 {
				return fmt.Errorf("single WAL record contains %d entries", len(record.Entries))
			}
			if !record.Entries[0].NoOp {
				commands++
			}
			return nil
		})
		if err != nil || commands != count {
			t.Fatalf("WAL replay: got %d commands, err=%v", commands, err)
		}
	})
}
