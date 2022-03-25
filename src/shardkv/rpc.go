package shardkv

type CommandRequest struct {
	ClientId, CommandId int64
	Key, Value, Op      string
}

type CommandResponse struct {
	ClientId, CommandId int64
	Value, Err          string
}

type PullShardsRequest struct {
	Shards    []int
	ConfigNum int
	Gid       int
}

type PullShardsResponse struct {
	Err          Err
	StateMachine []byte
	Shards       []int
}

type CanGcRequest struct {
	ConfigNum int
	Shard     int
}

type CanGcResponse struct {
	Shard int
	Err   Err
	Can   bool
}
