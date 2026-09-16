package raft

import (
	"raftkv/internal/storage"
)

// applierLoop runs as a dedicated goroutine applying committed entries in strict sequential order (I-005, I-009).
func (n *Node) applierLoop() {
	defer n.applierWg.Done()

	for {
		n.mu.Lock()
		for n.state.commitIndex <= n.state.lastApplied {
			if n.stopped {
				n.mu.Unlock()
				return
			}
			ch := n.commitNotifyCh
			n.mu.Unlock()

			select {
			case <-n.stop:
				return
			case <-ch:
			}

			n.mu.Lock()
		}

		idx := n.state.lastApplied + 1
		entry, err := n.cfg.LogStore.Get(idx) // disk I/O under Raft mutex — permitted per ADR-005
		n.mu.Unlock()

		if err != nil {
			// Bounded by lastLogIndex; continue loop if entry cannot be retrieved
			continue
		}

		// Apply under StateMachine write lock (Raft mutex released, SM mutex acquired — rule 34)
		var result storage.CommandResult
		var applyErr error
		if n.sm != nil && entry.Command != nil {
			n.sm.Lock()
			result, applyErr = n.sm.ApplyLocked(storage.Command{
				OperationType: storage.OperationType(entry.Command.OperationType),
				Key:           entry.Command.Key,
				Value:         entry.Command.Value,
				RequestID:     entry.Command.RequestId,
			})
			n.sm.Unlock()
		}

		n.mu.Lock()
		// I-005 / I-017 / Rule 8: advance strictly AFTER apply (I-009).
		// Application-level errors (e.g. ErrRequestIDReused) do NOT roll back commitIndex
		// or prevent lastApplied from advancing monotonically.
		n.state.lastApplied = idx

		// Wake read barrier waiters waiting for lastApplied >= barrier.CommitIndex
		n.notifyLocked(&n.applyNotifyCh)

		n.resolvePendingWriteLocked(idx, entry, result, applyErr)
		n.checkNoOpAppliedLocked(idx)
		n.mu.Unlock()
	}
}

// checkNoOpAppliedLocked verifies if the current leader's NOOP entry has been applied.
// When applied, sets readReadyTerm to currentTerm and wakes waiting readers (I-023).
func (n *Node) checkNoOpAppliedLocked(idx uint64) {
	if n.state.role == Leader &&
		n.leaderNoOpIndex != 0 &&
		idx == n.leaderNoOpIndex {
		n.readReadyTerm = n.state.currentTerm
		n.notifyLocked(&n.readReadyNotifyCh)
	}
}
