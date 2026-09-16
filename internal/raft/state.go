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
	commitIndex  uint64
	lastApplied  uint64
	nextIndex    map[string]uint64
	matchIndex   map[string]uint64
	leaderID     string
}

