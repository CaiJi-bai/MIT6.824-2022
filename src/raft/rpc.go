package raft

import (
	"fmt"
)

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
	defer func() { fmt.Println(args, reply) }()

	fmt.Println(rf.state, rf.currentTerm, rf.lastLogEntry().Index, rf.lastLogEntry().Term)

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	if args.Term > rf.currentTerm {
		rf.switchState(Follower, args.Term)
	}

	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		return // 不够新不给投
	}
	rf.newVotedFor(args.CandidateID)
	rf.electionTimer.Reset(randomElectionTimeout())
	reply.VoteGranted = true
}

func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.lastLogEntry()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() { reply.Term = rf.currentTerm }()

	if args.Term < rf.currentTerm {
		return
	}

	rf.switchState(Follower, args.Term) // TODO candidate term 领先还是会一直？
	rf.electionTimer.Reset(randomElectionTimeout())

	if args.PrevLogIndex < rf.firstLogEntry().Index {
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.switchState(Follower, rf.currentTerm)
	rf.electionTimer.Reset(randomElectionTimeout())

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}
