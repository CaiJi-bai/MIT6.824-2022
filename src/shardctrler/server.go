package shardctrler

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	lastApplied int

	stateMachine   MemoryShardCtrler             // KV stateMachine
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
	persister      *raft.Persister
}

type OperationContext struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	LastResponse *CommandResponse
	CommandId    int64
}

type CommandResponse struct {
	JoinReply  *JoinReply
	LeaveReply *LeaveReply
	MoveReply  *MoveReply
	QueryReply *QueryReply

	WrongLeader bool
	Err         string
}

type MemoryShardCtrler interface {
	Join(args *JoinArgs) *JoinReply
	Leave(args *LeaveArgs) *LeaveReply
	Move(args *MoveArgs) *MoveReply
	Query(args *QueryArgs) *QueryReply
}

type CommandRequest struct {
	JoinArgs  *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
	QueryArgs *QueryArgs

	ClientId, CommandId int64
	Op                  string
}

func (sc *ShardCtrler) applyLogToStateMachine(command *CommandRequest) *CommandResponse {
	rsp := &CommandResponse{}
	switch command.Op {
	case "Join":
		reply := sc.stateMachine.Join(command.JoinArgs)
		rsp.JoinReply = reply
	case "Leave":
		reply := sc.stateMachine.Leave(command.LeaveArgs)
		rsp.LeaveReply = reply
	case "Move":
		reply := sc.stateMachine.Move(command.MoveArgs)
		rsp.MoveReply = reply
	case "Query":
		reply := sc.stateMachine.Query(command.QueryArgs)
		rsp.QueryReply = reply
	default:
		panic("")
	}
	return rsp
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	args_ := &CommandRequest{}
	args_.JoinArgs = args
	args_.ClientId = args.ClientId
	args_.CommandId = args.CommandId
	args_.Op = "Join"

	reply_ := sc.Command(args_)
	reply.Err = Err(reply_.Err)
	reply.WrongLeader = reply_.WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	args_ := &CommandRequest{}
	args_.LeaveArgs = args
	args_.ClientId = args.ClientId
	args_.CommandId = args.CommandId
	args_.Op = "Leave"

	reply_ := sc.Command(args_)
	reply.Err = Err(reply_.Err)
	reply.WrongLeader = reply_.WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	args_ := &CommandRequest{}
	args_.MoveArgs = args
	args_.ClientId = args.ClientId
	args_.CommandId = args.CommandId
	args_.Op = "Move"

	reply_ := sc.Command(args_)
	reply.Err = Err(reply_.Err)
	reply.WrongLeader = reply_.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	args_ := &CommandRequest{}
	args_.QueryArgs = args
	args_.ClientId = args.ClientId
	args_.CommandId = args.CommandId
	args_.Op = "Query"

	reply_ := sc.Command(args_)
	reply.Err = Err(reply_.Err)
	reply.WrongLeader = reply_.WrongLeader
	if reply_.QueryReply != nil {
		reply.Config = reply_.QueryReply.Config
	}
}

func (sc *ShardCtrler) Command(request *CommandRequest) (response *CommandResponse) {
	response = &CommandResponse{}

	// return result directly without raft layer's participation if request is duplicated
	sc.mu.RLock()
	if request.Op != "Query" && sc.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := sc.lastOperations[request.ClientId].LastResponse
		response = lastResponse
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	index, _, isLeader := sc.rf.Start(request)
	if !isLeader {
		response.Err = "ErrWrongLeader"
		response.WrongLeader = true
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		response = result
	case <-time.After(500 * time.Millisecond):
		response.Err = "ErrTimeout"
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	// go func() {
	// 	sc.mu.Lock()
	// 	// kv.removeOutdatedNotifyChan(index)
	// 	sc.mu.Unlock()
	// }()
	return
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(CommandRequest{})
	labgob.Register(CommandResponse{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.stateMachine = NewMemoryShardCtrler()
	sc.lastOperations = map[int64]OperationContext{}
	sc.notifyChans = map[int]chan *CommandResponse{}

	sc.persister = persister
	sc.readPersist(persister.ReadSnapshot())

	go sc.applier()

	return sc
}

func (sc *ShardCtrler) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	sm := &memoryShardCtrler{}
	var ls map[int64]OperationContext = make(map[int64]OperationContext)
	if d.Decode(sm) == nil && d.Decode(&ls) == nil {
		sc.stateMachine = sm
		sc.lastOperations = ls
	} else {
		panic("")
	}
}

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (sc *ShardCtrler) applier() {
	for {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex

				var response *CommandResponse
				command, ok := message.Command.(*CommandRequest)
				if !ok {
					c_ := message.Command.(CommandRequest)
					command = &c_
				}
				if command.Op != "Query" && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					response = sc.lastOperations[command.ClientId].LastResponse
				} else {
					response = sc.applyLogToStateMachine(command)
					if command.Op != "Query" {
						sc.lastOperations[command.ClientId] = OperationContext{response, command.CommandId}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sc.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

func (sc *ShardCtrler) getNotifyChan(clientId int) chan *CommandResponse {
	c, ok := sc.notifyChans[clientId]
	if ok {
		return c
	}
	c = make(chan *CommandResponse, 128)
	sc.notifyChans[clientId] = c
	return c
}

func (sc *ShardCtrler) isDuplicateRequest(clientId, CommandId int64) bool {
	r, ok := sc.lastOperations[clientId]
	if ok && r.CommandId == CommandId {
		return true
	}
	if ok && r.CommandId > CommandId {
		panic("")
	}
	return false
}
