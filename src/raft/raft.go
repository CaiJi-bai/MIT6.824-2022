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
	"unsafe"

	//	"6.824/labgob"

	"6.824/labrpc"
)

type State string

const (
	Leader    = "Leader"
	Follower  = "Follower"
	Candidate = "Candidate"
)

const (
	electionTimeout     time.Duration = 150 * time.Millisecond
	electionTimeoutMore time.Duration = 150 * time.Millisecond
	heartbeatInterval   time.Duration = 100 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type Heartbeat struct {
	Peer int
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	id        uintptr
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state             State
	electionTimeoutAt time.Time

	// nv
	currentTerm int
	votedFor    int
	log         []LogEntry

	// v
	commitIndex int
	lastApplied int

	// v leader's follower info
	nextIndex  []int
	matchIndex []int

	heartbeats  chan Heartbeat
	stateSwitch chan func()

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()

	return
}

func (rf *Raft) GetStateX() (term int, who string) {
	rf.mu.Lock()
	term = rf.currentTerm
	who = string(rf.state)
	rf.mu.Unlock()

	return
}

func (rf *Raft) GetStateXX() (term int, who string, me int, g int) {
	rf.mu.Lock()
	term = rf.currentTerm
	who = string(rf.state)
	me = rf.me
	g = int(rf.id)
	rf.mu.Unlock()

	return
}

func (rf *Raft) Me() int {
	return rf.me
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.lastLogEntry().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = rf.log[lastIncludedIndex-rf.firstLogEntry().Index:]
		rf.log[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.log[0].Term, rf.log[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.firstLogEntry().Index
	if index <= snapshotIndex {
		return
	}
	rf.log = rf.log[index-snapshotIndex:]
	rf.log[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

func (rf *Raft) switchState(state State, term int) {
	defer rf.persist()

	if term > rf.currentTerm && state == Follower {
		rf.votedFor = -1
	}

	if rf.currentTerm != term || rf.state != state {
		DPrintf("[switchState %v:%v] %v:%v => %v:%v", rf.id, rf.me, rf.state, rf.currentTerm, state, term)
	}

	rf.currentTerm = term

	if state == rf.state {
		return
	}
	rf.state = state
	switch state {
	case Leader:
		// TODO Leader 在网络分区时可能会有脑裂的问题
		lastLog := rf.lastLogEntry()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(heartbeatInterval)
	case Follower:
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.heartbeatTimer.Stop()
	case Candidate:
		// 单节点不断离线，再次上线会发生 leader 迁移问题
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.heartbeatTimer.Stop()
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC argum ents in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

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
//

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.switchState(Candidate, rf.currentTerm+1)
			rf.election()
			rf.electionTimer.Reset(randomElectionTimeout())
			rf.mu.Unlock() // TODO 这里会不会导致 candidate 一直 term 增加后无法回到主区
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcastHeartbeats()
				rf.heartbeatTimer.Reset(heartbeatInterval)
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	n := len(peers)
	rf := &Raft{
		id:        (uintptr(unsafe.Pointer(peers[0])) / 1024) % 128,
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		state:             Follower,
		electionTimeoutAt: time.Now(), // TODO

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, n),
		matchIndex: make([]int, n),

		heartbeats:  make(chan Heartbeat, 64),
		stateSwitch: make(chan func(), 64),

		electionTimer:  time.NewTimer(randomElectionTimeout()),
		heartbeatTimer: time.NewTimer(heartbeatInterval),

		applyCh: applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())
	lastEntry := rf.lastLogEntry()
	for peer := range peers {
		rf.matchIndex[peer] = lastEntry.Index
		rf.nextIndex[peer] = lastEntry.Index + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}

func (rf *Raft) lastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) firstLogEntry() LogEntry {
	return rf.log[0]
}

func randomElectionTimeout() time.Duration {
	return time.Duration(rand.Int())%electionTimeoutMore + electionTimeout
}

func (rf *Raft) matchLog(term, index int) bool {
	if index > rf.lastLogEntry().Index {
		return false
	}
	if term != rf.log[index-rf.firstLogEntry().Index].Term {
		return false
	}

	// if index == rf.commitIndex {
	// 	panic("unexpected")
	// }

	if index < rf.commitIndex {
		// TestFailAgree2B
	}
	return true
}
