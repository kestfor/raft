package raft

import (
	"errors"
	"math"
	"testing"
)

func TestLogSnapshotBoundsAndOwnership(t *testing.T) {
	n, _ := newElectionTestNode(t, "a", nil)
	n.log.entries = append(n.log.entries,
		Entry{Index: 1, Term: 1, NoOp: true},
		Entry{Index: 2, Term: 1, Data: []byte("committed")},
		Entry{Index: 3, Term: 2, Data: []byte("pending")},
	)
	n.log.commitIndex = 2

	for _, tt := range []struct {
		from         uint64
		limit, count int
	}{
		{1, 10, 3}, {2, 1, 1}, {3, 10, 1}, {4, 10, 0}, {math.MaxUint64, 10, 0},
	} {
		page, err := n.Log(tt.from, tt.limit)
		if err != nil || len(page.Entries) != tt.count || page.CommitIndex != 2 || page.LastLogIndex != 3 {
			t.Fatalf("Log(%d, %d) = %+v, %v", tt.from, tt.limit, page, err)
		}

		if tt.count > 0 && page.Entries[0].Index != tt.from {
			t.Fatalf("wrong start: %+v", page)
		}
	}

	page, err := n.Log(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	page.Entries[0].Data[0] = 'X'
	page.Entries[0].Term = 99
	if string(n.log.entries[2].Data) != "committed" || n.log.entries[2].Term != 1 {
		t.Fatal("snapshot aliases the log")
	}
	n.log.entries[3].Data[0] = 'Y'
	if string(page.Entries[1].Data) != "pending" {
		t.Fatal("log mutation changed snapshot")
	}

	for _, tt := range []struct {
		from  uint64
		limit int
	}{{0, 1}, {1, 0}, {1, -1}} {
		if _, err := n.Log(tt.from, tt.limit); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("accepted invalid bounds: %+v, %v", tt, err)
		}
	}
}

func TestLogSnapshotEmptyAndByteLimit(t *testing.T) {
	n, _ := newElectionTestNode(t, "a", nil)
	page, err := n.Log(1, 100)
	if err != nil || page.Entries == nil || len(page.Entries) != 0 || page.LastLogIndex != 0 || page.CommitIndex != 0 {
		t.Fatalf("empty log: %+v, %v", page, err)
	}
	for i := range 5 {
		n.log.entries = append(n.log.entries, Entry{Index: uint64(i + 1), Term: 1, Data: make([]byte, 1<<20)})
	}
	page, err = n.Log(1, 100)
	if err != nil || len(page.Entries) != 4 || page.LastLogIndex != 5 {
		t.Fatalf("byte limit: count=%d, last=%d, err=%v", len(page.Entries), page.LastLogIndex, err)
	}
}

func TestLogReturnsLargeEntryAlone(t *testing.T) {
	n, _ := newElectionTestNode(t, "a", nil)
	n.log.entries = append(n.log.entries, Entry{Index: 1, Term: 1, Data: make([]byte, (4<<20)+1)}, Entry{Index: 2, Term: 1})
	page, err := n.Log(1, 2)
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Index != 1 {
		t.Fatalf("large entry page: count=%d, err=%v", len(page.Entries), err)
	}
}
