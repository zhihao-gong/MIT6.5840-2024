package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower int32 = iota
	Candidate
	Leader
)

type TermMeta struct {
	// latest term server has seen (initialized to 0
	// on first boot, increases monotonically)
	currentTerm int
	// whether has voted other in current term
	hasVoted bool
	// Leader, follower or candidate in this term
	role int32
	// time of last ping from leader
	lastPingTime time.Time
}

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	sync.RWMutex

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	TermMeta

	// log entries; each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	// log []LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	return rf.currentTerm, rf.isLeader()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.RLock()
	defer rf.RUnlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Grant = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if !rf.hasVoted {
		reply.Term = rf.currentTerm
		reply.Grant = true
		rf.hasVoted = true
	} else {
		reply.Term = rf.currentTerm
		reply.Grant = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		rf.lastPingTime = time.Now()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

// func (rf *Raft)

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	return atomic.LoadInt32(&rf.role) == Leader
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.hasVoted = false
	rf.lastPingTime = time.Now()
}

func (rf *Raft) loopPing(interval time.Duration) {
	for !rf.killed() {
		if !rf.isLeader() {
			time.Sleep(interval)
		}

		start := time.Now()
		rf.pingPeers()
		elapsed := time.Since(start)

		// Adjust the ticker interval to account for function execution time
		if elapsed < interval {
			time.Sleep(interval - elapsed)
		}
	}
}

func (rf *Raft) pingPeers() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// FIXME: limit the number of concurrent pings
		go func(i int) {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}

			ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.Lock()
					defer rf.Unlock()
					rf.becomeFollower(reply.Term)
				}
			}
		}(i)
	}
}

func (rf *Raft) loopElection(interval time.Duration) {
	for !rf.killed() {
		if rf.isLeader() {
			time.Sleep(interval)
		}

		electonTimeout := time.Duration((150 + rand.Intn(150))) * time.Millisecond
		rf.Lock()
		defer rf.Unlock()

		if time.Since(rf.lastPingTime) > electonTimeout {
			rf.role = Candidate
			rf.currentTerm++
			rf.hasVoted = true

			// start election
			if rf.startElection() {
				rf.role = Leader
			}
		}

		// FIXME: is randomness necessary?
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() bool {
	var wg sync.WaitGroup

	getVotes := 0
	for i := range rf.peers {
		if i == rf.me {
			// vote for itself
			getVotes += 1
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			// raft.AppendEntriesArgs
			reply := RequestVoteReply{}
			ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)
			if ok {
				if reply.Grant {
					getVotes += 1
				}
			}
		}(i)
	}

	wg.Wait()

	if getVotes > len(rf.peers)/2 {
		// becomes leader
		rf.role = Leader
		return true
	}

	// TODO: 根据投票结果返回 true 或 false
	return true
}

func (rf *Raft) ticker() {
	go rf.loopPing(100 * time.Millisecond)
	go rf.loopElection(100 * time.Millisecond)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.hasVoted = false
	rf.role = Follower
	rf.lastPingTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
