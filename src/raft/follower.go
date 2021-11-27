package raft

func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.lastLogEntry().Index)
	if newCommitIndex > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
			go func(commitIndex int) {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-rf.firstLogEntry().Index].Command,
					CommandIndex: rf.log[i-rf.firstLogEntry().Index].Index,
				}
			}(i)
		}
		rf.commitIndex = newCommitIndex
	}
}
