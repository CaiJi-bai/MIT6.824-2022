package raft

import "fmt"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	if args.Term > rf.currentTerm {
		rf.switchState(Follower, args.Term)
	}

	// TODO is log up-to-date
	// if rf.commitIndex > args.LastLogIndex {
	// 	return
	// }
	rf.votedFor = args.CandidateID
	rf.electionTimer.Reset(randomElectionTimeout())
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	if args.Term < rf.currentTerm {
		return
	}

	rf.switchState(Follower, args.Term)
	rf.electionTimer.Reset(randomElectionTimeout())

	if args.PrevLogIndex < rf.firstLogEntry().Index {
		panic("")
		return
	}

	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		fmt.Println("failed match: ", args, rf.currentTerm, rf.lastLogEntry().Index)
		if rf.lastLogEntry().Index < args.PrevLogIndex {
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.lastLogEntry().Index + 1
		} else {
			firstIndex := rf.firstLogEntry().Index
			reply.ConflictTerm = rf.log[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.log[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.firstLogEntry().Index
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
			rf.log = append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...)
			break
		}
	}

	rf.advanceCommitIndexForFollower(args.LeaderCommit)

	reply.Success = true
	reply.Term = rf.currentTerm
}
