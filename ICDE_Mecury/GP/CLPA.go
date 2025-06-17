package GP

import (
	"ICDE_Mecury/Params"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func (cs *State) Init_CLPA() {
	cs.WeightPenalty = 0.40
	cs.MaxIterations = 100
	cs.ShardNum = Params.ShardNum
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.NetGraph.AddEdgesFromFile(Params.Filename)
	cs.PartitionMap = make(map[Vertex]int, len(cs.NetGraph.VertexSet))
	for v := range cs.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		cs.PartitionMap[v] = int(num) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	}
}

func (cs *State) CLPA_score(v Vertex, uShard int) float64 {
	var score float64
	// 节点 v 的出度
	TotalEdges := 0
	// uShard 与节点 v 相连的边数
	Edgesto_uShard := 0
	EdgesNotUshard := 0
	for _, item := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += cs.NetGraph.WeightSet[v][item]
		} else {
			EdgesNotUshard += cs.NetGraph.WeightSet[v][item]
		}
		TotalEdges += cs.NetGraph.WeightSet[v][item]
	}

	//add this line
	score = float64(Edgesto_uShard) / float64(TotalEdges) * (1 - cs.WeightPenalty*float64(cs.Shardload[uShard])/float64(cs.MinEdges2Shard))
	return score
}

func (cs *State) CLPA_Partition() (map[string]uint64, int) {
	start := time.Now()
	cs.Init_CLPA()
	cs.ComputeEdges2Shard()
	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		for v := range cs.NetGraph.VertexSet {
			if updateTreshold[v.Addr] >= 50 {
				continue
			}
			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			//Case:1
			for _, u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				// 对于属于 uShard 的邻居，仅需计算一次
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.CLPA_score(v, uShard)
					if max_score < neighborShardScore[uShard] { //小于变成了大于
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && cs.Shardload[vNowShard] > 1 {
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateTreshold[v.Addr]++
				// 重新计算 VertexsNumInShard
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1
				cs.changeShardRecompute(v, vNowShard)
			}
		}
	}
	fmt.Println("----------细化后各分片权重----------")
	for sid, n := range cs.Shardload {
		fmt.Printf("Shard %d has Load: %d\n", sid, n)
	}
	fmt.Println("方差:", Variance(cs.Shardload))
	fmt.Println("最大负载：", MaxLoadShard(cs.Shardload))
	cs.ComputeEdges2Shard()
	cost := time.Since(start)
	fmt.Printf("cost=[%s]\n", cost)
	// add these lines
	file, err := os.Create("graph_clpa.txt") //create a new file
	if err != nil {
		fmt.Println(err)
	}
	fmt.Fprintf(file, "%s\n", cost)
	for key, value := range cs.PartitionMap {
		_, err := fmt.Fprintf(file, "%s: %d\n", key.Addr, value)
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
	}
	defer file.Close()
	//some func or operation
	fmt.Println("CLPA is executedd successfully.")
	//
	return res, cs.CrossShardEdgeNum
}
