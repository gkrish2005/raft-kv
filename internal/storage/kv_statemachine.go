package storage

import (
	"bytes"
	"fmt"
	"sync"
)

// OperationType defines the supported state machine operation types.
type OperationType string

const (
	Set    OperationType = "SET"
	Delete OperationType = "DELETE"
	Noop   OperationType = "NOOP"
)

// Command represents a state machine command.
type Command struct {
	OperationType OperationType
	Key           string
	Value         []byte
	RequestID     string
}

// CommandResult represents the result returned by StateMachine.Apply.
type CommandResult struct {
	Value []byte
}

// StateMachine is the core state machine interface.
//
// GET is explicitly NOT a Raft log command: Get(key) does not call Apply(),
// does not create a log entry, and does not participate in Raft replication.
// It is a direct local read of the in-memory KV state.
type StateMachine interface {
	Apply(cmd Command) (result CommandResult, err error)
	Get(key string) (value []byte, found bool)
}

// KVStateMachine is a thread-safe in-memory key-value state machine.
type KVStateMachine struct {
	mu sync.RWMutex
	kv map[string][]byte
}

// NewKVStateMachine creates a new initialized KVStateMachine.
func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		kv: make(map[string][]byte),
	}
}

// Get performs a plain local read from in-memory state.
// It distinguishes between key not found (found=false) and an empty value (found=true).
func (s *KVStateMachine) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.kv[key]
	if !ok {
		return nil, false
	}
	return bytes.Clone(val), true
}

// Apply executes a state machine command against the in-memory KV store.
func (s *KVStateMachine) Apply(cmd Command) (CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.OperationType {
	case Set:
		s.kv[cmd.Key] = bytes.Clone(cmd.Value)
		return CommandResult{}, nil

	case Delete:
		delete(s.kv, cmd.Key)
		return CommandResult{}, nil

	case Noop:
		// No-op has no KV mutation effect.
		return CommandResult{}, nil

	default:
		return CommandResult{}, fmt.Errorf("unknown operation type: %s", cmd.OperationType)
	}
}
