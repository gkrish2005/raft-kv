package raft

type Role string

const (
	Follower      Role = "Follower"
	Candidate     Role = "Candidate"
	Leader        Role = "Leader"
	StorageFailed Role = "StorageFailed"
)

type nodeState struct {
	currentTerm  uint64
	votedFor     string
	role         Role
	electionTerm uint64
	bootID       uint64
	// Test-only seedable vote-freshness metadata. Production nodes always retain (0, 0)
	// until Phase 2 introduces a real log; it is never populated from external input.
	lastLogIndex uint64
	lastLogTerm  uint64
}
