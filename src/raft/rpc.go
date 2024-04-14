package raft

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateId  int64
	Term         int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	// currentTerm, for candidate to update itself
	Term int
	// voteGranted true means candidate received vote
	Grant bool
}

type AppendEntriesArgs struct {
	// leader 's term
	Term int
	// so follower can redirect clients
	LeaderId int64
}

type AppendEntriesReply struct {
	// 	currentTerm, for leader to update itself
	Term int

	// success true if follower contained entry matching
	// prevLogIndex and prevLogTerm
	Success bool
}
