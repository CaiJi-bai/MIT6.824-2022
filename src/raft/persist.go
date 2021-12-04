package raft

import (
	"bytes"

	"6.824/labgob"
)

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // bootstrap without any state?
		return
	}
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("")
	}
	rf.currentTerm, rf.votedFor, rf.log = currentTerm, votedFor, log
	// there will always be at least one entry in rf.logs
	rf.lastApplied, rf.commitIndex = rf.log[0].Index, rf.log[0].Index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	rf.persister.SaveRaftState(buf.Bytes())
}

func (rf *Raft) newVotedFor(newVotedFor int) {
	rf.votedFor = newVotedFor
	rf.persist()
}
