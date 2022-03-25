package shardctrler

import "fmt"

func NewMemoryShardCtrler() MemoryShardCtrler {
	c := Config{Groups: map[int][]string{}}
	return &memoryShardCtrler{[]Config{c}}
}

type memoryShardCtrler struct {
	Configs []Config // indexed by config num
}

func (cf *memoryShardCtrler) Join(args *JoinArgs) *JoinReply {
	reply := &JoinReply{}
	newConfig := copyConfig(cf.Configs[len(cf.Configs)-1])

	g_ := 0
	for gid, servers := range args.Servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			g_ = gid
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	for i := range newConfig.Shards {
		if newConfig.Shards[i] == 0 {
			newConfig.Shards[i] = g_
		}
	}
	g2s := group2Shards(newConfig)
	for {
		source, target := maxG(g2s), minG(g2s)
		if len(g2s[source])-len(g2s[target]) <= 1 {
			break
		}
		g2s[target] = append(g2s[target], g2s[source][0])
		g2s[source] = g2s[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)

	fmt.Println("[Raft Config]", newConfig.Num-1, "->", newConfig.Num)

	return reply
}

func (sc *memoryShardCtrler) Leave(args *LeaveArgs) *LeaveReply {
	reply := &LeaveReply{}

	newConfig := copyConfig(sc.Configs[len(sc.Configs)-1])

	g2s := group2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range args.GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}
	var newShards [NShards]int
	// load balancing is performed only when raft groups exist
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := minG(g2s)
			g2s[target] = append(g2s[target], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	} else {
		for i := range newConfig.Shards {
			newConfig.Shards[i] = -1
		}
	}
	newConfig.Shards = newShards
	sc.Configs = append(sc.Configs, newConfig)

	fmt.Println("[Raft Config]", newConfig.Num-1, "->", newConfig.Num)

	return reply
}

func (sc *memoryShardCtrler) Move(args *MoveArgs) *MoveReply {
	reply := &MoveReply{}

	newConfig := copyConfig(sc.Configs[len(sc.Configs)-1])
	newConfig.Shards[args.Shard] = args.GID

	sc.Configs = append(sc.Configs, newConfig)

	fmt.Println("[Raft Config]", newConfig.Num-1, "->", newConfig.Num)

	return reply
}

func (sc *memoryShardCtrler) Query(args *QueryArgs) *QueryReply {
	reply := &QueryReply{}
	if args.Num == -1 || args.Num >= len(sc.Configs) {
		reply.Config = sc.Configs[len(sc.Configs)-1]
	} else {
		reply.Config = sc.Configs[args.Num]
	}
	return reply
}

func copyConfig(cfg Config) Config {
	newConfig := Config{}
	newConfig.Num = cfg.Num + 1
	newConfig.Shards = cfg.Shards
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range cfg.Groups {
		newConfig.Groups[gid] = servers
	}
	return newConfig
}

func group2Shards(cfg Config) map[int][]int {
	res := make(map[int][]int)
	for i, g := range cfg.Shards {
		_, ok := res[g]
		if ok {
			res[g] = append(res[g], i)
		} else {
			res[g] = []int{i}
		}
	}
	for g := range cfg.Groups {
		_, ok := res[g]
		if !ok {
			res[g] = nil
		}
	}
	return res
}

func minG(g2s map[int][]int) int {
	res := 0
	cnt := 0
	for g := range g2s {
		if res == 0 || cnt > len(g2s[g]) || (cnt == len(g2s[g]) && res > g) {
			res = g
			cnt = len(g2s[g])
		}
	}
	return res
}

func maxG(g2s map[int][]int) int {
	res := 0
	cnt := 0
	for g := range g2s {
		if res == 0 || cnt < len(g2s[g]) || (cnt == len(g2s[g]) && res > g) {
			res = g
			cnt = len(g2s[g])
		}
	}
	return res
}
