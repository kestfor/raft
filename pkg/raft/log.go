package raft

import (
	"context"
	"fmt"
	"math"
	"slices"
)

const (
	MaxCommandSize   = 1 << 20
	maxAppendEntries = 64
	maxAppendBytes   = 1 << 20
)

func (n *Node) AppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.validateRequest(ctx, req.LeaderID, req.Term); err != nil {
		return AppendEntriesResponse{}, err
	}

	if err := validateEntries(req.PrevLogIndex, req.PrevLogTerm, req.Term, req.Entries); err != nil {
		return AppendEntriesResponse{}, err
	}

	if req.Term < n.election.currentTerm {
		return AppendEntriesResponse{Term: n.election.currentTerm}, nil
	}

	// Even a consistency rejection is contact with the current leader.
	if err := n.becomeFollower(ctx, req.Term, req.LeaderID); err != nil {
		return AppendEntriesResponse{}, err
	}

	response := AppendEntriesResponse{Term: n.election.currentTerm}
	if req.PrevLogIndex > n.lastIndex() || n.log.entries[req.PrevLogIndex].Term != req.PrevLogTerm {
		return response, nil
	}

	if err := n.mergeEntries(req.Entries); err != nil {
		return response, err
	}

	// An empty or delayed request must neither truncate a matching suffix nor
	// commit beyond the prefix confirmed by this particular request.
	confirmed := req.PrevLogIndex + uint64(len(req.Entries))
	if commit := min(req.LeaderCommit, confirmed); commit > n.log.commitIndex {
		n.log.commitIndex = commit
		n.notifyLogChange()
	}

	response.Success = true

	return response, nil
}

// mergeEntries requires mu to be held and entries to follow a matching prefix.
func (n *Node) mergeEntries(entries []Entry) error {
	for i, entry := range entries {
		if entry.Index <= n.lastIndex() && n.log.entries[entry.Index].Term == entry.Term {
			continue
		}

		if entry.Index <= n.log.commitIndex {
			return fmt.Errorf("%w: cannot replace committed entry %d", ErrInvalidRequest, entry.Index)
		}

		suffix := cloneEntries(entries[i:])
		if err := n.save(n.election.currentTerm, n.election.votedFor, suffix); err != nil {
			return err
		}

		n.log.entries = append(n.log.entries[:entry.Index], suffix...)

		return nil
	}

	return nil
}

func validateEntries(prevIndex, prevTerm, term uint64, entries []Entry) error {
	if (prevIndex == 0) != (prevTerm == 0) || prevTerm > term || uint64(len(entries)) > math.MaxUint64-prevIndex {
		return fmt.Errorf("%w: invalid log prefix", ErrInvalidRequest)
	}

	if len(entries) > maxAppendEntries {
		return fmt.Errorf("%w: too many entries", ErrInvalidRequest)
	}

	size := 0
	for i, entry := range entries {
		if entry.Index != prevIndex+uint64(i)+1 || entry.Term == 0 || entry.Term < prevTerm || entry.Term > term {
			return fmt.Errorf("%w: invalid index or term at entry %d", ErrInvalidRequest, i)
		}

		if len(entry.Data) > MaxCommandSize || (entry.NoOp && len(entry.Data) != 0) {
			return fmt.Errorf("%w: invalid command at entry %d", ErrInvalidRequest, i)
		}

		size += len(entry.Data)
		prevTerm = entry.Term
	}

	if size > maxAppendBytes {
		return fmt.Errorf("%w: entries exceed batch size", ErrInvalidRequest)
	}

	return nil
}

func (n *Node) lastIndex() uint64 { return uint64(len(n.log.entries) - 1) }

func (n *Node) notifyLogChange() {
	close(n.log.changed)
	n.log.changed = make(chan struct{})
}

func (n *Node) replicatePeer(ctx context.Context, peer NodeID) {
	if !n.runtime.running || ctx.Err() != nil || n.runtime.rpcInFlight.Contains(peer) {
		return
	}

	next := n.log.nextIndex[peer]
	end := next
	size := 0
	for end <= n.lastIndex() && end-next < maxAppendEntries {
		entrySize := len(n.log.entries[end].Data)
		if size+entrySize > maxAppendBytes {
			break
		}

		size += entrySize
		end++
	}

	req := AppendEntriesRequest{
		Term:         n.election.currentTerm,
		LeaderID:     n.config.ID,
		PrevLogIndex: next - 1,
		PrevLogTerm:  n.log.entries[next-1].Term,
		Entries:      cloneEntries(n.log.entries[next:end]),
		LeaderCommit: n.log.commitIndex,
	}

	n.startRPC(ctx, peer, func() { n.sendAppendEntries(ctx, peer, req) })
}

func (n *Node) handleAppendResponse(
	ctx context.Context,
	peer NodeID,
	req AppendEntriesRequest,
	response AppendEntriesResponse,
) {
	if err := n.advanceTerm(ctx, response.Term); err != nil {
		return
	}

	if n.election.role != Leader || req.Term != n.election.currentTerm || response.Term != n.election.currentTerm {
		return
	}

	if req.PrevLogIndex+1 != n.log.nextIndex[peer] {
		return // A duplicated reply must not undo later progress.
	}

	if !response.Success {
		if n.log.nextIndex[peer] > 1 {
			n.log.nextIndex[peer]--
			n.replicatePeer(ctx, peer)
		}

		return
	}

	matched := req.PrevLogIndex + uint64(len(req.Entries))
	n.log.matchIndex[peer] = max(n.log.matchIndex[peer], matched)
	n.log.nextIndex[peer] = n.log.matchIndex[peer] + 1
	n.advanceCommit()

	if n.log.nextIndex[peer] <= n.lastIndex() || req.LeaderCommit < n.log.commitIndex {
		n.replicatePeer(ctx, peer)
	}
}

func (n *Node) advanceCommit() {
	matched := make([]uint64, 0, len(n.config.Peers)+1)
	matched = append(matched, n.lastIndex())
	for _, peer := range n.config.Peers {
		matched = append(matched, n.log.matchIndex[peer])
	}

	slices.Sort(matched)
	index := matched[(len(matched)-1)/2]
	if index > n.log.commitIndex && n.log.entries[index].Term == n.election.currentTerm {
		n.log.commitIndex = index
		n.notifyLogChange()
		n.wakeRun()
	}
}
