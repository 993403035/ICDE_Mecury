package GP

import (
	"ICDE_Mecury/Params"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

func (cs *State) Init_FG_LPA() {
	cs.MaxIterations = 100
	cs.ShardNum = Params.ShardNum
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.NetGraph.AddEdgesFromFile(Params.Filename)
	cs.PartitionMap = make(map[Vertex]int, len(cs.NetGraph.VertexSet))
	//labels := readCommunitiesFromCSV("data.csv")
	//for v := range cs.NetGraph.VertexSet {
	//	cs.PartitionMap[v] = labels[v]
	//	cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	//}
	//community := make(map[int][]Vertex)
	//for v, label := range labels {
	//	community[label] = append(community[label], v)
	//}
	//cs.PartitionMap = cs.NetGraph.FG_MergeCommunitiesIntoShards(community)
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

func (cs *State) FG_Louvain_score(v Vertex, uShard int) float64 {
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
	MeanLoad := Params.Edges / Params.ShardNum
	OverControl := -0.6 * math.Pow(math.Max(float64(cs.Shardload[uShard]-MeanLoad), 0)/float64(MeanLoad), 1) //去
	UnderControl := 0.4 * math.Max(float64(MeanLoad-cs.Shardload[uShard]), 0) / float64(MeanLoad)            //* math.Exp(float64(MeanLoad-cs.Shardload[uShard])/float64(MeanLoad)) //去
	LBScore := OverControl + UnderControl
	//penalty := 1.0 / (float64(cs.Shardload[uShard]-MeanLoad) * float64(Params.ShardNum))
	//penalty := 1.0 / (float64(cs.Shardload[uShard]-MeanLoad) * float64(Params.ShardNum))
	loadDiff := float64(cs.Shardload[uShard] - MeanLoad)
	penalty := 1.0 / (1.0 + math.Exp(loadDiff/float64(MeanLoad)))
	score = float64(Edgesto_uShard) / float64(TotalEdges) * math.Exp(1/float64(Params.ShardNum)*float64(LBScore)) * penalty
	//score = (float64(Params.ShardNum) * float64(Params.ShardNum)) * float64(Edgesto_uShard) / float64(TotalEdges) * math.Exp(float64(LBScore))
	return score
}

func (cs *State) FG_LPA_score(v Vertex, uShard int) float64 {
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
	MeanLoad := Params.Edges / Params.ShardNum
	OverControl := -0.6 * math.Pow(math.Max(float64(cs.Shardload[uShard]-MeanLoad), 0)/float64(MeanLoad), 1) //去
	UnderControl := 0.4 * math.Max(float64(MeanLoad-cs.Shardload[uShard]), 0) / float64(MeanLoad)            //* math.Exp(float64(MeanLoad-cs.Shardload[uShard])/float64(MeanLoad)) //去
	LBScore := OverControl + UnderControl
	score = float64(Edgesto_uShard) / float64(TotalEdges) * math.Exp(1/(float64(Params.ShardNum)*float64(Params.ShardNum))*float64(LBScore))
	threshold := 2.0 * float64(MeanLoad) // 可以根据实际情况调整阈值
	penalty := 1.0
	if float64(cs.Shardload[uShard]) > threshold {
		penalty = 1.0 / (float64(cs.Shardload[uShard]-MeanLoad) * float64(Params.ShardNum))
	}
	score = float64(Edgesto_uShard) / float64(TotalEdges) * math.Exp(1/(float64(Params.ShardNum)*float64(Params.ShardNum))*float64(LBScore)) * penalty
	return score
}

func (cs *State) FG_LPA_Partition() (map[string]uint64, int) {
	start := time.Now()
	cs.Init_FG_LPA()
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
					neighborShardScore[uShard] = cs.FG_Louvain_score(v, uShard)
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
	cs.ComputeEdges2Shard()
	// add these lines
	community := make(map[int][]Vertex)
	for v, label := range cs.PartitionMap {
		community[label] = append(community[label], v)
	}
	cs.NetGraph.Community = make(map[Vertex]int)
	for v, label := range cs.PartitionMap {
		cs.NetGraph.Community[v] = label
	}
	cs.PartitionMap = cs.NetGraph.FG_Fine()
	cost := time.Since(start)
	fmt.Printf("cost=[%s]\n", cost)
	cs.ComputeEdges2Shard()
	fmt.Println("最大负载：", MaxLoadShard(cs.Shardload))
	file, err := os.Create("graph_FG.txt") //create a new file
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
	fmt.Println("FG_LPA is executedd successfully.")
	//
	return res, cs.CrossShardEdgeNum
}

func (cs *State) fg_score(v Vertex, uShard int) float64 {
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
	//移入分片
	Now_uShardload := cs.Shardload[uShard]
	Future_uShardload := float64(cs.Shardload[uShard] + (TotalEdges - Edgesto_uShard) - Edgesto_uShard)
	Now_vShardload := cs.Shardload[vShard]
	Future_vShardload := float64(cs.Shardload[vShard] - (TotalEdges - Edgesto_vShard) + Edgesto_vShard)
	score = max(float64(Future_uShardload), float64(Future_vShardload)) - max(float64(Now_uShardload), float64(Now_vShardload))
	//score = float64(Edgesto_uShard) / float64(TotalEdges)
	return score
}
