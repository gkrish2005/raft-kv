package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// ErrRequestIDReused is returned when a command arrives with a RequestID that has
// already been applied, but with a different payload hash (I-017).
var ErrRequestIDReused = errors.New("request_id reused with different payload")

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

// AppliedRequest records the outcome and payload hash of an applied client request for deduplication (I-017).
type AppliedRequest struct {
	RequestID   string
	PayloadHash []byte // SHA-256 of CanonicalEncode(cmd)
	Result      CommandResult
}

// StateMachine is the core state machine interface.
//
// GET is explicitly NOT a Raft log command: Get(key) does not call Apply(),
// does not create a log entry, and does not participate in Raft replication.
// It is a direct local read of the in-memory KV state.
type StateMachine interface {
	Apply(cmd Command) (result CommandResult, err error)
	Get(key string) (value []byte, found bool)
	ApplyLocked(cmd Command) (result CommandResult, err error)
	GetLocked(key string) (value []byte, found bool)
}

// KVStateMachine is a thread-safe in-memory key-value state machine.
// Callers hold the embedded sync.RWMutex externally to synchronize operations,
// particularly across the read-barrier revalidation-to-read sequence (I-016).
type KVStateMachine struct {
	sync.RWMutex
	kv           map[string][]byte
	requestTable map[string]AppliedRequest
}

// NewKVStateMachine creates a new initialized KVStateMachine.
func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		kv:           make(map[string][]byte),
		requestTable: make(map[string]AppliedRequest),
	}
}

// CanonicalEncode computes the frozen, deterministic binary serialization of a command's
// identity fields {OperationType, Key, Value} per docs/client-semantics.md.
// RequestID is deliberately excluded (I-017). Multi-byte integers are encoded in little-endian.
func CanonicalEncode(cmd Command) []byte {
	valLen := len(cmd.Value)
	totalLen := 1 + 1 + len(cmd.OperationType) + 4 + len(cmd.Key) + 4 + valLen
	buf := make([]byte, totalLen)
	buf[0] = 1 // version 1
	buf[1] = byte(len(cmd.OperationType))
	copy(buf[2:], cmd.OperationType)
	offset := 2 + len(cmd.OperationType)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(cmd.Key)))
	offset += 4
	copy(buf[offset:], cmd.Key)
	offset += len(cmd.Key)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(valLen))
	offset += 4
	if valLen > 0 {
		copy(buf[offset:], cmd.Value)
	}
	return buf
}

// CommandPayloadHash computes the SHA-256 hash of the canonical encoding of cmd.
func CommandPayloadHash(cmd Command) []byte {
	h := sha256.Sum256(CanonicalEncode(cmd))
	return h[:]
}

// GetLocked performs a plain local read from in-memory state.
// Caller must hold s.RLock() or s.Lock().
// It distinguishes between key not found (found=false) and an empty value (found=true).
func (s *KVStateMachine) GetLocked(key string) ([]byte, bool) {
	val, ok := s.kv[key]
	if !ok {
		return nil, false
	}
	return bytes.Clone(val), true
}

// ApplyLocked executes a state machine command against the in-memory KV store.
// Dedup check and state machine mutation are performed atomically under s.Lock() (I-017).
// Caller must hold s.Lock().
func (s *KVStateMachine) ApplyLocked(cmd Command) (CommandResult, error) {
	if cmd.OperationType == Noop {
		// No-op has no KV mutation effect and is never recorded in requestTable (I-023).
		return CommandResult{}, nil
	}

	if cmd.RequestID != "" {
		hash := CommandPayloadHash(cmd)
		if entry, exists := s.requestTable[cmd.RequestID]; exists {
			if !bytes.Equal(entry.PayloadHash, hash) {
				// I-017: duplicate RequestID with different payload.
				// Application error — does NOT roll back commitIndex or block lastApplied (rule 8 / I-005).
				return CommandResult{}, ErrRequestIDReused
			}
			// Dedup hit — return cached result without re-executing KV mutation.
			return entry.Result, nil
		}

		result, err := s.applyOpLocked(cmd)
		if err != nil {
			return CommandResult{}, err
		}
		s.requestTable[cmd.RequestID] = AppliedRequest{
			RequestID:   cmd.RequestID,
			PayloadHash: hash,
			Result:      result,
		}
		return result, nil
	}

	return s.applyOpLocked(cmd)
}

func (s *KVStateMachine) applyOpLocked(cmd Command) (CommandResult, error) {
	switch cmd.OperationType {
	case Set:
		s.kv[cmd.Key] = bytes.Clone(cmd.Value)
		return CommandResult{}, nil

	case Delete:
		delete(s.kv, cmd.Key)
		return CommandResult{}, nil

	default:
		return CommandResult{}, fmt.Errorf("unknown operation type: %s", cmd.OperationType)
	}
}

// Get performs a plain local read from in-memory state with internal locking.
func (s *KVStateMachine) Get(key string) ([]byte, bool) {
	s.RLock()
	defer s.RUnlock()
	return s.GetLocked(key)
}

// Apply executes a state machine command against the in-memory KV store with internal locking.
func (s *KVStateMachine) Apply(cmd Command) (CommandResult, error) {
	s.Lock()
	defer s.Unlock()
	return s.ApplyLocked(cmd)
}

// KVSnapshot returns a point-in-time copy of all key-value pairs under RLock.
func (s *KVStateMachine) KVSnapshot() map[string][]byte {
	s.RLock()
	defer s.RUnlock()
	snap := make(map[string][]byte, len(s.kv))
	for k, v := range s.kv {
		snap[k] = bytes.Clone(v)
	}
	return snap
}

// RequestTableSnapshot returns a point-in-time copy of the replicated RequestTable under RLock.
func (s *KVStateMachine) RequestTableSnapshot() map[string]AppliedRequest {
	s.RLock()
	defer s.RUnlock()
	snap := make(map[string]AppliedRequest, len(s.requestTable))
	for k, v := range s.requestTable {
		snap[k] = AppliedRequest{
			RequestID:   v.RequestID,
			PayloadHash: bytes.Clone(v.PayloadHash),
			Result:      v.Result,
		}
	}
	return snap
}

