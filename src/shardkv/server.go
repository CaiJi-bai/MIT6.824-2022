package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu       sync.RWMutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	// shards       []int
	// newShards    map[int]shardctrler.Config
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	lastApplied  int // record the lastApplied to prevent stateMachine from rollback

	stateMachine *MemoryKV                     // KV stateMachine
	notifyChans  map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
	persister    *raft.Persister

	mck *shardctrler.Clerk

	dead int32 // set by Kill()
}

func (kv *ShardKV) getNotifyChan(clientId int) chan *CommandResponse {
	c, ok := kv.notifyChans[clientId]
	if ok {
		return c
	}
	c = make(chan *CommandResponse, 32)
	kv.notifyChans[clientId] = c
	return c
}

func (kv *ShardKV) CanGc(request *CanGcRequest, response *CanGcResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cfgNum := kv.stateMachine.Cfg.Num
	_, has := kv.stateMachine.NeedPullShards[request.Shard]

	response.Err = OK
	response.Shard = request.Shard
	response.Can = cfgNum > request.ConfigNum || cfgNum == request.ConfigNum && !has
}

func (kv *ShardKV) PullShards(request *PullShardsRequest, response *PullShardsResponse) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// _, isLeader := kv.rf.GetState()
	// if !isLeader {
	// 	response.Err = ErrWrongLeader
	// 	return
	// }

	cfgNum := kv.stateMachine.Cfg.Num // 保证 s 内的 key 到这个 g 会被拒绝
	if request.ConfigNum > cfgNum {
		DPrintf("[Why Old? -%v] cfg: %v, needPull: %v", kv.gid, kv.stateMachine.Cfg, kv.stateMachine.NeedPullShards)
		response.Err = ErrOldConfigNum
		return
	}

	// TODO check shard

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	res := kv.stateMachine.PullShards(request.Shards, request.ConfigNum, request.Gid)
	e.Encode(res)
	response.StateMachine = w.Bytes()
	response.Shards = request.Shards
	response.Err = OK
}

func (kv *ShardKV) Command(request *CommandRequest, response *CommandResponse) {
	// return result directly without raft layer's participation if request is duplicated
	kv.mu.RLock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		term, who, me, g := kv.rf.GetStateXX()
		DPrintf("ErrWrongLeader1: %v:%v - T:%v - %v", g, me, term, who)
		kv.mu.RUnlock()
		return
	}

	if !kv.stateMachine.Contains(request.Key) {
		response.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	kv.mu.RUnlock()
	// do not hold lock to improve throughput
	// when KVServer holds the lock to take snapshot, underlying raft can still commit raft logs
	index, _, isLeader := kv.rf.Start(request)
	if !isLeader {
		term, who := kv.rf.GetStateX()
		DPrintf("ErrWrongLeader2: %v:%v - %v - %v", kv.gid, kv.me, term, who)
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

L1:
	for {
		select {
		case result := <-ch:
			if result.CommandId != request.CommandId {
				continue
			}
			response.Value, response.Err = result.Value, result.Err
			break L1
		case <-time.After(time.Second):
			response.Err = "ErrTimeout"
			break L1
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {

	// Your initialization code here.

	labgob.Register(CommandRequest{})
	labgob.Register(CommandResponse{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardsRequest{})
	labgob.Register(PullShardsResponse{})
	labgob.Register(CanGcRequest{})
	labgob.Register(CanGcResponse{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg, 128)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = NewMemoryKV(gid)
	kv.notifyChans = map[int]chan *CommandResponse{}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.persister = persister
	kv.readPersist(persister.ReadSnapshot())
	go kv.shardsPuller()
	go kv.applier()
	go kv.cfgFetcher()
	go kv.gc()

	return kv
}

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *ShardKV) applier() {
	for !kv.killed() {
		message := <-kv.applyCh
		// DPrintf("{Node %v} tries to apply message %v", kv.rf.Me(), reflect.TypeOf(message.Command))
		if message.CommandValid {
			kv.mu.Lock()
			if message.CommandIndex <= kv.lastApplied {
				DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), message, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = message.CommandIndex

			var response *CommandResponse
			needResponse := false
			switch ins := message.Command.(type) {
			case *CommandRequest:
				needResponse = true
				response = kv.applyLogToStateMachine(ins)
			case *shardctrler.Config:
				ok := kv.stateMachine.ApplyConfig(ins)
				_ = ok
			case *PullShardsResponse:
				ok := kv.stateMachine.ApplyPullShards(ins)
				if !ok {
					DPrintf("error pull data")
				}
			case *CanGcResponse:
				kv.stateMachine.DeleteShard(ins.Shard)

			case CommandRequest:
				needResponse = true
				response = kv.applyLogToStateMachine(&ins)
			case shardctrler.Config:
				ok := kv.stateMachine.ApplyConfig(&ins)
				_ = ok
			case PullShardsResponse:
				ok := kv.stateMachine.ApplyPullShards(&ins)
				if !ok {
					DPrintf("error pull data")
				}
			case CanGcResponse:
				kv.stateMachine.DeleteShard(ins.Shard)
			default:
				panic(ins)
			}

			// only notify related channel for currentTerm's log when node is leader
			if currentTerm, isLeader := kv.rf.GetState(); needResponse && isLeader && message.CommandTerm == currentTerm {
				ch := kv.getNotifyChan(message.CommandIndex)
				ch <- response
			}

			needSnapshot := kv.needSnapshot()
			if needSnapshot {
				// kv.takeSnapshot(message.CommandIndex)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.stateMachine)
				data := w.Bytes()
				kv.rf.Snapshot(message.CommandIndex, data)
			}
			kv.mu.Unlock()
		} else if message.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
				kv.readPersist(message.Snapshot)
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", message))
		}

	}
}

func (kv *ShardKV) shardsPuller() {
	tk := time.NewTicker(125 * time.Millisecond)
	for {
		select {
		case <-tk.C:
		L1:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			kv.mu.RLock()
			cfg := kv.stateMachine.Cfg
			needPullShards := kv.stateMachine.NeedPullShards
			kv.mu.RUnlock()

			DPrintf("[shardsPuller - %v] cfg: %v, need: %v", kv.gid, cfg, needPullShards)

			var wg sync.WaitGroup

			for s, n := range needPullShards {
				DPrintf("stage doing: %v:%v from %v", s, n.Gid, n.Svrs)
				wg.Add(1)
				go func(s int, n HistoryNode) {
					defer wg.Done()
					for _, server := range n.Svrs {
						srv := kv.make_end(server)
						reply := &PullShardsResponse{}
						args := &PullShardsRequest{}
						args.Shards = []int{s}
						args.ConfigNum = cfg.Num
						args.Gid = kv.gid
						args.ConfigNum = kv.stateMachine.Cfg.Num
						DPrintf("pulling %v from %v", s, n.Gid)
						ok := srv.Call("ShardKV.PullShards", args, reply)
						DPrintf("pulled %v from %v, res: %v", s, n.Gid, reply.Err)
						if ok && reply.Err == OK {
							kv.rf.Start(reply)
							return
						}
					}
				}(s, n)
			}
			wg.Wait()
			if len(needPullShards) > 0 {
				goto L1
			}
		}
	}
}

func (kv *ShardKV) cfgFetcher() {
	tk := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-tk.C:
			kv.mu.RLock()
			cfgNum := kv.stateMachine.GetConfigNum()
			kv.mu.RUnlock()
			cfg := kv.mck.Query(cfgNum + 1)
			if _, isLeader := kv.rf.GetState(); cfg.Num == cfgNum+1 && isLeader {
				_, _, ok := kv.rf.Start(&cfg)
				term, who := kv.rf.GetStateX()
				me := kv.rf.Me()
				DPrintf("is leader: %v - %v:%v:%v", ok, term, me, who)
			}
		}
	}
}

func (kv *ShardKV) gc() {
	tk := time.NewTicker(200 * time.Millisecond)
	for {
		select {
		case <-tk.C:
			deletingShards := map[int]DeletingNode{}
			kv.mu.RLock()
			for k, v := range kv.stateMachine.DeletingShards {
				deletingShards[k] = v
			}
			kv.mu.RUnlock()

			cfg := kv.mck.Query(-1)

			var wg sync.WaitGroup
			for s, n := range deletingShards {
				wg.Add(1)
				go func(s int, n DeletingNode) {
					defer wg.Done()
					for _, server := range cfg.Groups[n.Gid] {
						srv := kv.make_end(server)
						reply := &CanGcResponse{}
						args := &CanGcRequest{}
						args.Shard = s
						args.ConfigNum = n.ConfigNum
						ok := srv.Call("ShardKV.CanGc", args, reply)
						if ok && reply.Err == OK && reply.Can {
							kv.rf.Start(reply)
							return
						}
					}
				}(s, n)
			}
			wg.Wait()
		}
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*0.95
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var sm *MemoryKV = &MemoryKV{}
	err := d.Decode(sm)
	if err == nil {
		kv.stateMachine = sm
	} else {
		panic(err)
	}
	// TODO
}

func (kv *ShardKV) applyLogToStateMachine(command *CommandRequest) *CommandResponse {
	rsp := &CommandResponse{}
	rsp.ClientId = command.ClientId
	rsp.CommandId = command.CommandId

	is, r := kv.stateMachine.IsDuplicateRequest(command.ClientId, command.CommandId)
	if is {
		DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(), command, command.ClientId, command.ClientId)
		return r
	}

	switch command.Op {
	case "Get":
		res, err := kv.stateMachine.Get(command.Key)
		rsp.Value, rsp.Err = res, string(err)
	case "Put":
		err := kv.stateMachine.Put(command.Key, command.Value, command.ClientId, command.CommandId)
		rsp.Value, rsp.Err = "", string(err)
	case "Append":
		err := kv.stateMachine.Append(command.Key, command.Value, command.ClientId, command.CommandId)
		rsp.Value, rsp.Err = "", string(err)
	default:
		panic("")
	}
	return rsp
}
