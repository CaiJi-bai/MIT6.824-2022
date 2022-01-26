package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OperationContext struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	LastResponse *CommandResponse
	CommandId    int64
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int // record the lastApplied to prevent stateMachine from rollback

	stateMachine   KVStateMachine                // KV stateMachine
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}

func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	// return result directly without raft layer's participation if request is duplicated
	kv.mu.RLock()
	if request.Op != "Get" && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.lastOperations[request.ClientId].LastResponse
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	// do not hold lock to improve throughput
	// when KVServer holds the lock to take snapshot, underlying raft can still commit raft logs
	index, _, isLeader := kv.rf.Start(request)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(time.Second):
		response.Err = "ErrTimeout"
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		kv.mu.Lock()
		// kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicateRequest(clientId, CommandId int64) bool {
	r, ok := kv.lastOperations[clientId]
	if ok && r.CommandId == CommandId {
		return true
	}
	if ok && r.CommandId > CommandId {
		panic("")
	}
	return false
}

func (kv *KVServer) getNotifyChan(clientId int) chan *CommandResponse {
	c, ok := kv.notifyChans[clientId]
	if ok {
		return c
	}
	c = make(chan *CommandResponse)
	kv.notifyChans[clientId] = c
	return c
}

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v} tries to apply message %v", kv.rf.Me(), message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandResponse
				command, ok := message.Command.(*CommandRequest)
				if !ok {
					c_ := message.Command.(CommandRequest)
					command = &c_
				}
				if command.Op != "Get" && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(), message, kv.lastOperations[command.ClientId], command.ClientId)
					response = kv.lastOperations[command.ClientId].LastResponse
				} else {
					response = kv.applyLogToStateMachine(command)
					if command.Op != "Get" {
						kv.lastOperations[command.ClientId] = OperationContext{response, command.CommandId}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				// needSnapshot := kv.needSnapshot()
				// if needSnapshot {
				// 	kv.takeSnapshot(message.CommandIndex)
				// }
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					//kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

func (kv *KVServer) applyLogToStateMachine(command *CommandRequest) *CommandResponse {
	rsp := &CommandResponse{}
	switch command.Op {
	case "Get":
		res, err := kv.stateMachine.Get(command.Key)
		rsp.Value, rsp.Err = res, string(err)
	case "Put":
		err := kv.stateMachine.Put(command.Key, command.Value)
		rsp.Value, rsp.Err = "", string(err)
	case "Append":
		err := kv.stateMachine.Append(command.Key, command.Value)
		rsp.Value, rsp.Err = "", string(err)
	default:
		panic("")
	}
	return rsp
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(CommandRequest{})
	labgob.Register(CommandResponse{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = NewMemoryKV()
	kv.lastOperations = map[int64]OperationContext{}
	kv.notifyChans = map[int]chan *CommandResponse{}

	go kv.applier()

	return kv
}
