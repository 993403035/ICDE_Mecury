package GP

import (
	"ICDE_Mecury/Params"
	"fmt"
	"math"
	"os"
	"time"
)

func (cs *State) Init_TxAllo() {
	cs.MaxIterations = 100
	cs.ShardNum = Params.ShardNum
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.NetGraph.AddEdgesFromFile(Params.Filename)

	//labels := cs.NetGraph.LouvainPartition(1, 0.01)
	//cs.NetGraph.writeCommunitiesToCSV(labels)
	//计算时间太漫长，已处理好
	labels := readCommunitiesFromCSV("3_Louvain.csv")
	community := make(map[int][]Vertex)
	for v, label := range labels {
		community[label] = append(community[label], v)
	}
	cs.PartitionMap = cs.NetGraph.TxAllo_MergeCommunitiesIntoShards(community, labels)
	for v := range cs.NetGraph.VertexSet {
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	}
}

func (cs *State) TxAlloScore(v Vertex, uShard int) float64 {
	var score float64
	// uShard 与节点 v 相连的边数
	vShard := cs.PartitionMap[v]
	Edgesto_uShard := 0
	Edgesto_vShard := 0
	TotalEdges := 0
	for _, item := range cs.NetGraph.EdgeSet[v] {
		TotalEdges += cs.NetGraph.WeightSet[v][item]
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += cs.NetGraph.WeightSet[v][item]
		} else if cs.PartitionMap[item] == vShard {
			Edgesto_vShard += cs.NetGraph.WeightSet[v][item]
		}
	}
	MeanLoad := float64(Params.Edges / Params.ShardNum)
	//移入分片
	Now_uShardload := cs.Shardload[uShard]
	Now_uThrouput := cs.Throuput[uShard]
	Future_uShardload := float64(cs.Shardload[uShard] + Params.Weight*(TotalEdges-Edgesto_uShard) - (Params.Weight-1)*Edgesto_uShard)
	Future_uThrouput := float64(cs.Throuput[uShard]) + 0.5*float64(TotalEdges)
	IncreaseGain := CaculateThrouput(int(Future_uShardload), int(MeanLoad), Future_uThrouput) - CaculateThrouput(Now_uShardload, int(MeanLoad), Now_uThrouput)
	//移出分片
	Now_vShardload := cs.Shardload[vShard]
	Now_vThrouput := cs.Throuput[vShard]
	Future_vShardload := float64(cs.Shardload[vShard] - Params.Weight*(TotalEdges-Edgesto_vShard) + (Params.Weight-1)*Edgesto_vShard)
	Future_vThrouput := float64(cs.Throuput[vShard]) - 0.5*float64(TotalEdges-Edgesto_vShard)
	RemoveGain := CaculateThrouput(int(Future_vShardload), int(MeanLoad), Future_vThrouput) - CaculateThrouput(Now_vShardload, int(MeanLoad), Now_vThrouput)
	score = IncreaseGain + RemoveGain
	return score
}

func (cs *State) TxAllo_Partition() (map[string]uint64, int) {
	start := time.Now()
	cs.Init_TxAllo()
	cs.ComputeEdges2Shard()
	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		for v := range cs.NetGraph.VertexSet {
			neighborShardScore := make(map[int]float64)
			max_score := 0.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			for _, u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				// 对于属于 uShard 的邻居，仅需计算一次
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.TxAlloScore(v, uShard)
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
				// 重新计算WeightSet
				cs.TxAllochangeShardRecompute(v, vNowShard)
			}
		}
	}
	cs.Shardload = cs.TxAlloShardload()
	fmt.Println("----------优化后各分片权重----------")
	for sid, n := range cs.Shardload {
		fmt.Printf("Shard %d has Load: %d\n", sid, n)
	}
	fmt.Println("方差:", Variance(cs.Shardload))
	cs.ComputeEdges2Shard()
	cost := time.Since(start)
	fmt.Printf("cost=[%s]\n", cost)
	file, err := os.Create("graph_txallo.txt") //create a new file
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
	fmt.Println("TxAllo is executedd successfully.")
	return res, cs.CrossShardEdgeNum
}

func (cs *State) TxAlloShardload() []int {
	ShardloadDic := make(map[int]float64)
	Shardload := make([]int, 0)
	for v := range cs.NetGraph.VertexSet {
		vShard := cs.PartitionMap[v]
		for _, u := range cs.NetGraph.EdgeSet[v] {
			uShard := cs.PartitionMap[u]
			if vShard != uShard {
				ShardloadDic[vShard] += 1.0 * float64(cs.NetGraph.WeightSet[v][u])
			} else {
				ShardloadDic[vShard] += 0.5 * float64(cs.NetGraph.WeightSet[v][u])
			}
		}
	}
	for _, load := range ShardloadDic {
		Shardload = append(Shardload, int(load))
	}
	fmt.Println(Shardload)
	return Shardload
}

// TxAllo优化阶段，负载更新
func (cs *State) TxAllochangeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	for _, u := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			cs.Shardload[new] += Params.Weight * cs.NetGraph.WeightSet[v][u] //cross-shard +1
			cs.Shardload[old] -= Params.Weight * cs.NetGraph.WeightSet[v][u] //cross-shard -1
			cs.Throuput[new] += 0.5 * float64(cs.NetGraph.WeightSet[v][u])
			cs.Throuput[old] -= 0.5 * float64(cs.NetGraph.WeightSet[v][u])
		} else if neighborShard == new {
			cs.Shardload[old] -= Params.Weight * cs.NetGraph.WeightSet[v][u]
			cs.Shardload[new] -= (Params.Weight - 1) * cs.NetGraph.WeightSet[v][u]
			cs.Throuput[old] -= 0.5 * float64(cs.NetGraph.WeightSet[v][u])
			cs.Throuput[new] += 0.5 * float64(cs.NetGraph.WeightSet[v][u])
			cs.CrossShardEdgeNum -= cs.NetGraph.WeightSet[v][u]
		} else {
			cs.Shardload[new] += Params.Weight * cs.NetGraph.WeightSet[v][u]
			cs.Shardload[old] += (Params.Weight - 1) * cs.NetGraph.WeightSet[v][u]
			cs.Throuput[new] += 0.5 * float64(cs.NetGraph.WeightSet[v][u])
			cs.Throuput[old] -= 0.5 * float64(cs.NetGraph.WeightSet[v][u])
			cs.CrossShardEdgeNum += cs.NetGraph.WeightSet[v][u]
		}
	}
	cs.MinEdges2Shard = math.MaxInt
	cs.MaxWeightShard = math.MinInt
	// 修改 MinEdges2Shard, CrossShardEdgeNum
	for _, val := range cs.Shardload {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
		if cs.MaxWeightShard < val {
			cs.MaxWeightShard = val
		}
	}
}
