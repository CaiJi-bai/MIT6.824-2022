package raft

import "fmt"

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.lastLogEntry().Index)
	if newCommitIndex > rf.commitIndex {
		l := rf.commitIndex - rf.firstLogEntry().Index + 1
		r := newCommitIndex - rf.firstLogEntry().Index + 1
		rf.commitIndex = newCommitIndex
		fmt.Println("follower advance commit: ", rf.commitIndex)

		for i := l; i < r; i++ {
			go func(entry LogEntry) {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
			}(rf.log[i])
		}
	}
}
