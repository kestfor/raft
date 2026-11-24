package raft

import "fmt"

// LogSnapshot describes a local log at one instant. Entries may include an
// uncommitted suffix; this is a diagnostic view, not a linearizable read.
type LogSnapshot struct {
	Entries      []Entry
	LastLogIndex uint64
	CommitIndex  uint64
}

// Log returns independent copies of at most limit entries starting at from
// (inclusive). Index zero is the internal empty prefix and cannot be requested.
// A page targets 4 MiB of command data. A larger entry gets its own page. Pages may observe
// different log versions when an uncommitted suffix is replaced.
func (n *Node) Log(from uint64, limit int) (LogSnapshot, error) {
	if from == 0 || limit <= 0 {
		return LogSnapshot{}, fmt.Errorf("%w: from and limit must be positive", ErrInvalidRequest)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	page := LogSnapshot{
		Entries: []Entry{}, LastLogIndex: n.lastIndex(), CommitIndex: n.log.commitIndex,
	}
	if from > page.LastLogIndex {
		return page, nil
	}

	end := from
	size := 0
	for end <= page.LastLogIndex && end-from < uint64(limit) {
		entrySize := len(n.log.entries[end].Data)
		if end > from && entrySize > (4<<20)-size {
			break
		}

		size += entrySize
		end++
	}

	page.Entries = cloneEntries(n.log.entries[from:end])
	return page, nil
}
