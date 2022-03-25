package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/shardctrler"
)

// type KVStateMachine interface {
// 	Get(key string) (string, Err)
// 	Put(key, value string, clientId, commandId int64) Err
// 	Append(key, value string, clientId, commandId int64) Err
// 	IsDuplicateRequest(clientId, CommandId int64) (bool, *CommandResponse)
// 	GetConfigNum() int
// 	Contains(key string) bool
// }

type DeletingNode struct {
	Gid       int
	ConfigNum int
}

type HistoryNode struct {
	Gid  int
	Svrs []string
}

type MemoryKV struct {
	KV             map[string]string
	LastOperations map[int64]int64
	OldCfg         *shardctrler.Config
	Cfg            *shardctrler.Config
	Shards         map[int]struct{}
	Gid            int
	ShardsHistiry  map[int]HistoryNode
	NeedPullShards map[int]HistoryNode
	DeletingShards map[int]DeletingNode
}

func NewMemoryKV(gid int) *MemoryKV {
	cfg := shardctrler.Config{}
	cfg.Num = -1
	return &MemoryKV{
		make(map[string]string),
		make(map[int64]int64),
		&shardctrler.Config{},
		&cfg,
		make(map[int]struct{}),
		gid,
		make(map[int]HistoryNode),
		make(map[int]HistoryNode),
		make(map[int]DeletingNode),
	}
}

func (kv *MemoryKV) Get(key string) (string, Err) {
	if !kv.Contains(key) {
		return "", ErrWrongGroup
	}
	if value, ok := kv.KV[key]; ok {
		// DPrintf("[SM - %v] Get[%v] = %v", kv.Gid, key, value)
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *MemoryKV) DeleteShard(s int) bool {
	if _, ok := kv.Shards[s]; ok {
		return false
	}

	DPrintf("deleting %v", s)

	for k, _ := range kv.KV {
		if key2shard(k) == s {
			delete(kv.KV, k)
		}
	}

	delete(kv.DeletingShards, s)

	return true
}

func (kv *MemoryKV) Put(key, value string, clientId, commandId int64) Err {
	if !kv.Contains(key) {
		return ErrWrongGroup
	}
	res := &CommandResponse{}
	kv.KV[key] = value
	res.Err = OK
	kv.LastOperations[clientId] = commandId
	return Err(res.Err)
}

func (kv *MemoryKV) Append(key, value string, clientId, commandId int64) Err {
	if !kv.Contains(key) {
		return ErrWrongGroup
	}
	res := &CommandResponse{}
	if _, ok := kv.KV[key]; ok {
		kv.KV[key] += value
		res.Err = OK
	} else {
		res.Err = ErrNoKey
	}
	kv.LastOperations[clientId] = commandId
	// DPrintf("[SM - %v] Append[%v] = %v", kv.Gid, key, kv.KV[key])
	return Err(res.Err)
}

func (kv *MemoryKV) GetConfigNum() int {
	return kv.Cfg.Num

}

func (kv *MemoryKV) IsDuplicateRequest(clientId, CommandId int64) (bool, *CommandResponse) {
	maxCommandId, ok := kv.LastOperations[clientId]
	if ok && maxCommandId >= CommandId {
		return true, &CommandResponse{
			ClientId:  clientId,
			CommandId: CommandId,
			Value:     "",
			Err:       OK,
		}
	}
	return false, nil
}

func (kv *MemoryKV) Contains(key string) bool {
	s := key2shard(key)
	_, ok1 := kv.Shards[s]
	_, ok2 := kv.NeedPullShards[s]
	DPrintf("!!! %v %v %v", key2shard(key), kv.Shards, kv.NeedPullShards)
	return ok1 && !ok2
}

// cfg -> shards -> newShards
func (kv *MemoryKV) ApplyConfig(cfg *shardctrler.Config) bool {
	if len(kv.NeedPullShards) != 0 {
		DPrintf("waiting pulling: %v | %v", kv.Cfg, kv.NeedPullShards)
		return false
	}
	if cfg.Num != kv.Cfg.Num+1 {
		DPrintf("error config: %v => %v", cfg.Num, kv.Cfg.Num)
		return false
	}

	kv.OldCfg = kv.Cfg
	kv.Cfg = cfg
	oldShards := kv.Shards
	kv.Shards = make(map[int]struct{})
	for s, g := range cfg.Shards {
		if g == kv.Gid {
			kv.Shards[s] = struct{}{}
		}
		oldNode, ok := kv.ShardsHistiry[s]
		kv.ShardsHistiry[s] = HistoryNode{g, cfg.Groups[g]}
		if g == kv.Gid && ok && oldNode.Gid != kv.Gid && oldNode.Gid != 0 {
			kv.NeedPullShards[s] = oldNode
		}
	}

	DPrintf("[SM - %v] Config(%v) -> Config(%v) -- [%v => %v] with %v", kv.Gid, kv.OldCfg.Num, cfg.Num, oldShards, kv.Shards, kv.NeedPullShards)
	return true
}

func (kv *MemoryKV) ApplyPullShards(reply *PullShardsResponse) bool {
	if _, ok := kv.NeedPullShards[reply.Shards[0]]; !ok {
		return true
	}

	data := reply.StateMachine
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var sm *MemoryKV = &MemoryKV{}
	if d.Decode(sm) != nil {
		return false
	}

	for k, v := range sm.KV {
		kv.KV[k] = v
	}
	// TODO: delete shards
	for k, v := range sm.LastOperations {
		if v_, ok := kv.LastOperations[k]; ok && v > v_ || !ok {
			kv.LastOperations[k] = v
		}
	}
	for _, s := range reply.Shards {
		delete(kv.NeedPullShards, s)
		DPrintf("[SM - %v] Config(%v) [get_shard: %v]", kv.Gid, kv.Cfg.Num, s)
	}

	return true
}

func (kv *MemoryKV) PullShards(shards []int, configNum, gid int) *MemoryKV {
	res := &MemoryKV{}
	res.KV = map[string]string{}
	res.LastOperations = map[int64]int64{}

	for k, v := range kv.KV {
		if in(key2shard(k), shards) {
			res.KV[k] = v
		}
	}
	for k, v := range kv.LastOperations {
		res.LastOperations[k] = v
	}
	for _, s := range shards {
		kv.DeletingShards[s] = DeletingNode{gid, configNum}
	}
	return res
}

func in(num int, nums []int) bool {
	for _, i := range nums {
		if i == num {
			return true
		}
	}
	return false
}
