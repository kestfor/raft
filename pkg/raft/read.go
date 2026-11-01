package raft

import "context"

// ReadIndex confirms leadership with a fresh quorum in the current term.
// The caller must apply committed entries through the returned index before
// reading its state machine. No entry is appended for the read itself.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.canPropose(ctx); err != nil {
		return 0, err
	}

	term := n.election.currentTerm

	for n.log.entries[n.log.commitIndex].Term != term {
		if err := n.waitLogChange(ctx); err != nil {
			return 0, err
		}

		if err := n.checkReadTerm(ctx, term); err != nil {
			return 0, err
		}
	}

	index := n.log.commitIndex
	n.log.readRound++
	round := n.log.readRound

	// Schedule through Run: outgoing RPCs must use its lifetime context, not
	// a client's context. Already in-flight RPCs carry the previous round.
	n.wakeRun()

	for {
		if err := n.checkReadTerm(ctx, term); err != nil {
			return 0, err
		}

		votes := 1
		for _, peer := range n.config.Peers {
			if n.log.readAck[peer] >= round {
				votes++
			}
		}

		if votes >= (len(n.config.Peers)+1)/2+1 {
			return index, nil
		}

		if err := n.waitLogChange(ctx); err != nil {
			return 0, err
		}
	}
}

func (n *Node) checkReadTerm(ctx context.Context, term uint64) error {
	if err := n.canPropose(ctx); err != nil {
		return err
	}

	if n.election.currentTerm != term {
		return ErrLeadershipLost
	}

	return nil
}

// CommittedEntries waits for a bounded, owned batch after the given index.
// It works on every role and may be called before Run starts. After shutdown
// it returns ErrNotRunning. Consumers replay from zero after a restart.
func (n *Node) CommittedEntries(ctx context.Context, after uint64) ([]Entry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if n.runtime.failure != nil {
			return nil, n.runtime.failure
		}

		if n.runtime.started && !n.runtime.running {
			return nil, ErrNotRunning
		}

		if after < n.log.commitIndex {
			end := after + 1
			size := 0
			for end <= n.log.commitIndex && end-after <= maxAppendEntries {
				size += len(n.log.entries[end].Data)
				if size > maxAppendBytes {
					break
				}

				end++
			}

			return cloneEntries(n.log.entries[after+1 : end]), nil
		}

		if err := n.waitLogChange(ctx); err != nil {
			return nil, err
		}
	}
}

// waitLogChange temporarily releases mu and returns with it held again.
func (n *Node) waitLogChange(ctx context.Context) error {
	changed := n.log.changed
	n.mu.Unlock()

	select {
	case <-ctx.Done():
	case <-changed:
	}

	n.mu.Lock()

	return ctx.Err()
}
