package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

type Clerk struct {
	lk                  sync.Mutex
	sm                  *shardctrler.Clerk
	config              shardctrler.Config
	make_end            func(string) *labrpc.ClientEnd
	clientId, commandId int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	return ck.Command(key, "", "Get")
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(key, value, "Append")
}

func (ck *Clerk) Command(key string, value string, op string) string {
	ck.lk.Lock()
	defer ck.lk.Unlock()

	args := CommandRequest{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.CommandId = ck.commandId
	args.ClientId = ck.clientId
	shard := key2shard(key)

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				DPrintf("[%v] group: %v, svr: %v, shard: %v", op, gid, si, key2shard(key))
				srv := ck.make_end(servers[si])
				var reply *CommandResponse
				ok := srv.Call("ShardKV.Command", &args, &reply)
				DPrintf("[%v] res: %v", op, reply)
				if ok && reply.Err == OK {
					// fmt.Println("done: ", gid, servers[si], " -- ", key, value)
					ck.commandId++
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
