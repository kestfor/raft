package raft

import (
	"encoding/json"
	"fmt"
	"slices"
)

// Storage is an append-only journal, such as wal.Log. Append must make a record
// durable before returning success. Replay returns complete records in order.
// The caller owns Storage and closes it after Run and all callers have finished.
type Storage interface {
	Append([]byte) error
	Replay(func([]byte) error) error
}

// Each record carries the current hard state and an optional log suffix. A
// replacement is one WAL record, so a crash cannot replay half a replacement.
type diskRecord struct {
	NodeID   NodeID  `json:"node_id"`
	Term     uint64  `json:"term"`
	VotedFor NodeID  `json:"voted_for"`
	Entries  []Entry `json:"entries,omitempty"`
}

func (n *Node) restore() error {
	if n.config.Storage == nil {
		return nil
	}

	return n.config.Storage.Replay(func(data []byte) error {
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("raft: decode WAL record: %w", err)
		}

		if record.NodeID != n.config.ID || record.Term < n.election.currentTerm || record.Term == 0 {
			return fmt.Errorf("raft: invalid node or term in WAL")
		}

		if record.Term == n.election.currentTerm && n.election.votedFor != "" &&
			record.VotedFor != n.election.votedFor {
			return fmt.Errorf("raft: conflicting vote in WAL")
		}

		if len(record.Entries) > 0 {
			first := record.Entries[0].Index
			if first == 0 || first > n.lastIndex()+1 {
				return fmt.Errorf("raft: non-contiguous log in WAL")
			}

			if err := validateEntries(first-1, n.log.entries[first-1].Term, record.Term, record.Entries); err != nil {
				return fmt.Errorf("raft: invalid WAL entries: %w", err)
			}

			n.log.entries = append(n.log.entries[:first], record.Entries...)
		}

		n.election.currentTerm = record.Term
		n.election.votedFor = record.VotedFor

		return nil
	})
}

// save does not change the in-memory hard state or log. Callers publish changes
// only after it succeeds. Any storage failure stops participation in the protocol.
func (n *Node) save(term uint64, vote NodeID, entries []Entry) error {
	if n.config.Storage == nil {
		return nil
	}

	data, err := json.Marshal(diskRecord{NodeID: n.config.ID, Term: term, VotedFor: vote, Entries: entries})
	if err == nil {
		err = n.config.Storage.Append(data)
	}

	if err != nil {
		return n.fail(fmt.Errorf("raft: persist state: %w", err))
	}

	return nil
}

func cloneEntries(entries []Entry) []Entry {
	result := slices.Clone(entries)
	for i := range result {
		result[i].Data = slices.Clone(result[i].Data)
	}

	return result
}
