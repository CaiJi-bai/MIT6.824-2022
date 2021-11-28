package raft

import "fmt"

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.lastLogEntry().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		fmt.Println("[advance]follower advance commit: ", rf.commitIndex)
		rf.applyCond.Signal()
	}
}
