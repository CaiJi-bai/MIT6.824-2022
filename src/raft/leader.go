package raft

import (
	"fmt"
	"sort"
)

func (rf *Raft) broadcastHeartbeats() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.replicateOneRound(peer)
		}(peer)
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.firstLogEntry().Index {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.firstLogEntry().Index,
			LastIncludedTerm:  rf.firstLogEntry().Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		reply := &InstallSnapshotReply{}
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			rf.handleInstallSnapshotReply(peer, args, reply)
			rf.mu.Unlock()
		}
	} else {
		firstIndex := rf.firstLogEntry().Index
		entries := make([]LogEntry, len(rf.log[prevLogIndex+1-firstIndex:]))
		copy(entries, rf.log[prevLogIndex+1-firstIndex:])
		request := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex-firstIndex].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		response := &AppendEntriesReply{}
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			fmt.Println("[sendAppendEntries]", rf.me, "->", peer, ":", request, response)
			rf.handleAppendEntriesReply(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.switchState(Follower, reply.Term)
		rf.newVotedFor(-1)
	} else {
		rf.matchIndex[peer], rf.nextIndex[peer] = args.LastIncludedIndex, args.LastIncludedIndex+1
	}
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Success {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeader()
		} else {
			if reply.Term > rf.currentTerm {
				rf.switchState(Follower, reply.Term)
			} else if reply.Term == rf.currentTerm { // TODO
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstIndex := rf.firstLogEntry().Index
					for i := args.PrevLogIndex; i >= firstIndex; i-- {
						if rf.log[i-firstIndex].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
}

// used to compute and advance commitIndex by matchIndex[]
func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	fmt.Println(srt)
	sort.Ints(srt)
	newCommitIndex := srt[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		// only advance commitIndex for current term's log
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			rf.commitIndex = newCommitIndex
			fmt.Println("[advance]leader advance commit: ", rf.commitIndex)
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	entry := LogEntry{rf.lastLogEntry().Index + 1, rf.currentTerm, command}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = entry.Index
	rf.nextIndex[rf.me] = entry.Index + 1

	rf.persist()

	fmt.Println("[cmd]new cmd ->", rf.me, ":", entry)

	rf.broadcastHeartbeats()
	return entry.Index, entry.Term, true
}
