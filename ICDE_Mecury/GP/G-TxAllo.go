package GP

import (
	"ICDE_Mecury/Params"
	"fmt"
	"sort"
)

// TxAllo初始化分配
func (g *Graph) TxAllo_MergeCommunitiesIntoShards(communityMap map[int][]Vertex, labelmap map[Vertex]int) map[Vertex]int {
	shardCount := Params.ShardNum
	//shardWeights := make([]int, shardCount)
	g.Shardload = make(map[int]int, shardCount)
	g.Throuput = make(map[int]float64, Params.ShardNum)
	g.IsShard = make(map[Vertex]bool, len(g.VertexSet))
	g.Shard = make(map[Vertex]int, len(g.VertexSet))
	g.Community = make(map[Vertex]int, len(g.VertexSet))
	// 将社区按权重排序
	type communityInfo struct {
		label  int
		weight int
	}
	for vertex, label := range labelmap {
		g.Community[vertex] = label
	}
	communities := make([]communityInfo, 0, len(communityMap))
	for label, members := range communityMap {
		weight := g.TxAlloCalculateCommunityEdgeWeight(members)
		communities = append(communities, communityInfo{label, weight})
	}
	sort.Slice(communities, func(i, j int) bool {
		return communities[i].weight > communities[j].weight // 按权重从大到小排序
	})
	//区别大型社区和小型社区
	topCommunities := communities[:Params.ShardNum] // Top N communities
	IstopCommunities := make(map[int]bool, len(communities))
	topCommunitiesIndex := make(map[int]int, 0)
	for idx := range len(communities) {
		IstopCommunities[idx] = false
	}
	index := 0
	for _, member := range topCommunities {
		g.Shardload[index] = member.weight
		IstopCommunities[member.label] = true
		topCommunitiesIndex[member.label] = index
		index += 1
	}
	//节点编号重分配
	for v := range g.VertexSet {
		g.IsShard[v] = false
		CommunityLabel := g.Community[v]
		if IstopCommunities[CommunityLabel] {
			g.IsShard[v] = true
			g.Shard[v] = topCommunitiesIndex[CommunityLabel]
		}
	}
	g.InitThrouput()
	nodeToShard := g.TxAllo_Partition()
	g.Community = g.Shard
	// 打印每个分片的权重
	fmt.Println("----------分片初始化后各分片权重----------")
	shardload := make(map[int]float64)
	for v, lst := range g.EdgeSet {
		// 获取节点 v 所属的shard
		vShard := nodeToShard[v]
		for _, u := range lst {
			// 同上，获取节点 u 所属的shard
			uShard := nodeToShard[u]
			if vShard != uShard {
				shardload[vShard] += 1.0 * float64(g.WeightSet[v][u])
			} else {
				shardload[vShard] += 0.5 * float64(g.WeightSet[v][u])
			}
		}
	}
	for i, weight := range shardload {
		fmt.Printf("Shard %d has load: %f\n", i, weight)
	}
	load := make([]int, Params.ShardNum)
	for id := range shardload {
		load[id] = int(shardload[id])
	}
	fmt.Println("方差:", Variance(load))
	fmt.Println("中位数:", Median(load))
	return nodeToShard
}

// TxAllo初始化阶段负载计算
func (g *Graph) TxAlloCalculateCommunityEdgeWeight(community []Vertex) int {
	visited := make(map[Vertex]map[Vertex]bool) // 用于记录已经计算过的边
	totalWeight := 0
	for _, v := range community {
		if visited[v] == nil {
			visited[v] = make(map[Vertex]bool)
		}
		for _, neighbor := range g.EdgeSet[v] {
			// 检查是否已经计算过该点
			if visited[neighbor] == nil {
				visited[neighbor] = make(map[Vertex]bool)
			}
			if !visited[v][neighbor] && !visited[neighbor][v] {
				if g.Community[v] != g.Community[neighbor] {
					totalWeight += Params.Weight * g.WeightSet[v][neighbor]
				}
				if g.Community[v] == g.Community[neighbor] {
					totalWeight += g.WeightSet[v][neighbor]
				}
				visited[v][neighbor] = true
				visited[neighbor][v] = true
			}
		}
	}
	return totalWeight
}

// TxAllo初始化阶段节点加入
func (g *Graph) TxAllo_Partition() map[Vertex]int {
	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)
	for iter := 0; iter < 1; iter += 1 { // 第一层循环控制算法次数，constraint
		for v := range g.VertexSet {
			if g.IsShard[v] {
				continue
			}
			ShardScore := make(map[int]float64)
			max_score := -99999.0
			max_scoreShard := 0
			//Case:1
			var CandidateFlag = false
			for _, u := range g.EdgeSet[v] {
				if g.IsShard[u] {
					CandidateFlag = true
				}
			}
			if CandidateFlag {
				for _, u := range g.EdgeSet[v] {
					uShard := g.Shard[u]
					// 对于属于 uShard 的邻居，仅需计算一次
					if _, computed := ShardScore[uShard]; !computed {
						ShardScore[uShard] = g.TxAlloScore(v, uShard)
						if ShardScore[uShard] > 0 && max_score < ShardScore[uShard] { //小于变成了大于
							max_score = ShardScore[uShard]
							max_scoreShard = uShard
						}
					}
				}
			} else {
				for idx := range Params.ShardNum {
					if _, computed := ShardScore[idx]; !computed {
						ShardScore[idx] = g.TxAlloScore(v, idx)
						if max_score < ShardScore[idx] {
							max_score = ShardScore[idx]
							max_scoreShard = idx
						}
					}
				}
			}
			g.Shard[v] = max_scoreShard
			res[v.Addr] = uint64(max_scoreShard)
			updateTreshold[v.Addr]++
			g.TxAllochangeShardRecompute(v, max_scoreShard)
			g.IsShard[v] = true
		}
	}
	return g.Shard
}

// TxAllo初始化阶段分数
func (g *Graph) TxAlloScore(v Vertex, uShard int) float64 {
	var score float64
	Edgesto_uShard := 0
	TotalEdges := 0
	for _, item := range g.EdgeSet[v] {
		if g.IsShard[item] && g.Shard[item] == uShard {
			Edgesto_uShard += g.WeightSet[v][item]
		}
		TotalEdges += g.WeightSet[v][item]
	}
	MeanLoad := float64(Params.Edges / Params.ShardNum)
	Future_uShardload := float64(g.Shardload[uShard] + Params.Weight*(TotalEdges-Edgesto_uShard) - (Params.Weight-1)*Edgesto_uShard)
	Future_uThrouput := float64(g.Throuput[uShard]) + 0.5*float64(TotalEdges)
	IncreaseGain := CaculateThrouput(int(Future_uShardload), int(MeanLoad), Future_uThrouput)
	score = IncreaseGain
	return score
}

// TxAllo初始化阶段加入后负载重计算
func (g *Graph) TxAllochangeShardRecompute(v Vertex, new int) {
	for _, neigbor := range g.EdgeSet[v] {
		if !g.IsShard[neigbor] {
			g.Shardload[new] += Params.Weight * g.WeightSet[v][neigbor]
			g.Throuput[new] += 0.5 * float64(g.WeightSet[v][neigbor])
		} else if g.IsShard[neigbor] && g.Shard[neigbor] != new {
			g.Shardload[new] += Params.Weight * g.WeightSet[v][neigbor]
			g.Throuput[new] += 0.5 * float64(g.WeightSet[v][neigbor])
		} else if g.IsShard[neigbor] && g.Shard[neigbor] == new {
			g.Shardload[new] -= (Params.Weight - 1) * g.WeightSet[v][neigbor]
			g.Throuput[new] += 0.5 * float64(g.WeightSet[v][neigbor])
		}
	}
}

// TxAllo初始化吞吐量
func (g *Graph) InitThrouput() {
	ProcessCablity := float64(Params.Edges / Params.ShardNum)
	for v := range g.VertexSet {
		v_shard := g.Shard[v]
		if g.IsShard[v] {
			for _, u := range g.EdgeSet[v] {
				if g.IsShard[u] {
					u_shard := g.Shard[u]
					if v_shard != u_shard {
						g.Throuput[v_shard] += 0.5 * float64(g.WeightSet[v][u])
					}
					if v_shard == u_shard {
						g.Throuput[v_shard] += 0.5 * float64(g.WeightSet[v][u])
					}
				} else {
					g.Throuput[v_shard] += 0.5 * float64(g.WeightSet[v][u])
				}
			}
		}
	}
	for idx, load := range g.Shardload {
		if float64(load) <= ProcessCablity {
			g.Throuput[idx] = g.Throuput[idx]
		}
		if float64(load) > ProcessCablity {
			g.Throuput[idx] = ProcessCablity / float64(load) * g.Throuput[idx]
		}
	}
}

// TxAllo吞吐量计算
func CaculateThrouput(Workload int, IdealWorkload int, tps float64) float64 {
	//if Workload <= IdealWorkload {
	//	return float64(Workload)
	//} else {
	//	return float64((IdealWorkload / Workload) * Workload)
	//}
	if Workload <= IdealWorkload {
		return float64(tps)
	} else {
		return float64(IdealWorkload/Workload) * tps
	}
}
