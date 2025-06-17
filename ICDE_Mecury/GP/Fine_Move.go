package GP

import (
	"ICDE_Mecury/Params"
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// LabelPropagation 基于标签传播算法检测社区
func (g *Graph) LabelPropagation() map[Vertex]int {
	labels := make(map[Vertex]int)
	vertices := make([]Vertex, 0, len(g.VertexSet))

	// 初始化标签
	i := 0
	for v := range g.VertexSet {
		labels[v] = i
		vertices = append(vertices, v)
		i++
	}

	rand.Seed(time.Now().UnixNano())
	changed := true
	for iter := 0; iter < 100; iter += 1 {
		// 迭代标签传播
		for changed {
			changed = false

			// 随机遍历所有节点
			rand.Shuffle(len(vertices), func(i, j int) {
				vertices[i], vertices[j] = vertices[j], vertices[i]
			})

			for _, v := range vertices {
				labelCount := make(map[int]int)
				for _, neighbor := range g.EdgeSet[v] {
					labelCount[labels[neighbor]] += g.WeightSet[v][neighbor]
				}

				// 找到频率最高的标签
				maxLabel := labels[v]
				maxCount := 0
				for label, count := range labelCount {
					if count > maxCount || (count == maxCount && label < maxLabel) {
						maxLabel = label
						maxCount = count
					}
				}

				// 如果标签改变，则标记为需要继续传播
				if labels[v] != maxLabel {
					labels[v] = maxLabel
					changed = true
				}
			}
		}
	}
	return labels
}

func (g *Graph) CalculateCommunityEdgeWeight(community []Vertex) int {
	visited := make(map[Vertex]map[Vertex]bool) // 用于记录已经计算过的边
	totalWeight := 0

	for _, v := range community {
		for _, neighbor := range g.EdgeSet[v] {
			// 检查是否已经计算过该边
			if visited[v] == nil {
				visited[v] = make(map[Vertex]bool)
			}
			if visited[neighbor] == nil {
				visited[neighbor] = make(map[Vertex]bool)
			}

			if !visited[v][neighbor] && !visited[neighbor][v] {
				totalWeight += g.WeightSet[v][neighbor]
				visited[v][neighbor] = true
				visited[neighbor][v] = true
			}
		}
	}

	return totalWeight
}

func (g *Graph) FG_Fine() map[Vertex]int {
	//每个包含多少个分片
	fmt.Println("微调阶段开始")
	shardCount := Params.ShardNum
	communityMap := make(map[int][]Vertex)
	shards := make([][]Vertex, shardCount)
	shardWeights := make([]int, shardCount)
	for v, label := range g.Community {
		communityMap[label] = append(communityMap[label], v)
		shards[label] = append(shards[label], v)
	}
	for label, members := range communityMap {
		shardWeights[label] = g.CalculateCommunityEdgeWeight(members)
	}
	// 将社区按权重排序
	ShardSort := g.SortShards(communityMap)
	nodeToShard := make(map[Vertex]int)
	nodeToShard = g.Community
	IsTraverses := make([]int, Params.ShardNum)
	isOverload := true
	round := 0
	for isOverload {
		isOverload = false
		NewShards := make([][]Vertex, Params.ShardNum)
		//indexes := rand.Perm(Params.ShardNum) //打乱分片序
		for _, id := range ShardSort {
			nodes := shards[id]
			if (float64(shardWeights[id]) > float64(Params.Edges/Params.ShardNum)*1.25) && (IsTraverses[id] < 3) {
				isOverload = true
				IsTraverses[id] += 1
				round += 1
				fmt.Println("粗化第几个分片", id)
				NewGraph := g.ShardToGraph(nodes, id)
				NewShards, shardWeights = NewGraph.SubCommunityMove(shardWeights, id)
				for id, nodes := range NewShards {
					for _, node := range nodes {
						nodeToShard[node] = id
					}
				}
				break
			}
		}
		shards = make([][]Vertex, Params.ShardNum)
		for node, idx := range nodeToShard {
			communityMap[idx] = append(communityMap[idx], node)
			shards[idx] = append(shards[idx], node)
		}
		ShardSort = g.SortShards(communityMap)
		if round > Params.ShardNum*2 {
			break
		}
	}
	//计算粗话后的权重
	fmt.Println("----------微调后各分片权重----------")
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

func (g *Graph) SortShards(communityMap map[int][]Vertex) []int {
	type communityInfo struct {
		label  int
		weight int
	}
	communities := make([]communityInfo, 0, len(communityMap))
	for label, members := range communityMap {
		weight := g.CalculateCommunityEdgeWeight(members)
		communities = append(communities, communityInfo{label, weight})
	}
	sort.Slice(communities, func(i, j int) bool {
		return communities[i].weight > communities[j].weight // 按权重从大到小排序
	})
	sortedLabels := make([]int, len(communities))
	for i, community := range communities {
		sortedLabels[i] = community.label
	}
	return sortedLabels
}

// 社区合并
func (g *Graph) MergeCommunitiesIntoShards(communityMap map[int][]Vertex) map[Vertex]int {
	shardCount := Params.ShardNum
	shards := make([][]Vertex, shardCount)
	shardWeights := make([]int, shardCount)
	labelToShard := make(map[int]int) // 映射社区标签到分片编号

	// 将社区按权重排序
	type communityInfo struct {
		label  int
		weight int
	}
	communities := make([]communityInfo, 0, len(communityMap))
	for label, members := range communityMap {
		weight := g.CalculateCommunityEdgeWeight(members)
		communities = append(communities, communityInfo{label, weight})
	}
	sort.Slice(communities, func(i, j int) bool {
		return communities[i].weight > communities[j].weight // 按权重从大到小排序
	})
	// 按贪心策略分配社区到分片
	for _, community := range communities {
		minShard := 0
		for i := 1; i < shardCount; i++ {
			if shardWeights[i] < shardWeights[minShard] {
				minShard = i
			}
		}
		shards[minShard] = append(shards[minShard], communityMap[community.label]...)
		shardWeights[minShard] += community.weight
		labelToShard[community.label] = minShard
	}
	// 更新节点标签为分片编号
	nodeToShard := make(map[Vertex]int)
	for shardID, members := range shards {
		for _, v := range members {
			nodeToShard[v] = shardID
		}
	}
	g.Community = nodeToShard
	// 打印每个分片的权重
	//add these lines
	//IsTraverses := make([]int, Params.ShardNum)
	//isOverload := true
	//round := 0
	//for isOverload {
	//	isOverload = false
	//	NewShards := make([][]Vertex, Params.ShardNum)
	//	//indexes := rand.Perm(Params.ShardNum) //打乱分片序
	//	for id := range Params.ShardNum {
	//		nodes := shards[id]
	//		if (float64(shardWeights[id]) > float64(Params.Edges/Params.ShardNum)*1.5) && (IsTraverses[id] < 5) {
	//			//fmt.Println(shardWeights[id])
	//			isOverload = true
	//			IsTraverses[id] += 1
	//			round += 1
	//			fmt.Println("粗化第几个分片", id)
	//			NewGraph := g.ShardToGraph(nodes, id)
	//			NewShards, shardWeights = NewGraph.SubCommunityMove(shardWeights, id)
	//			for id, nodes := range NewShards {
	//				for _, node := range nodes {
	//					nodeToShard[node] = id
	//				}
	//			}
	//			break
	//		}
	//	}
	//	shards = make([][]Vertex, Params.ShardNum)
	//	for node, idx := range nodeToShard {
	//		shards[idx] = append(shards[idx], node)
	//	}
	//
	//	if round > Params.ShardNum*2 {
	//		break
	//	}
	//}
	//分割线
	//NewShards := make([][]Vertex, Params.ShardNum)
	//for id, nodes := range shards {
	//	//fmt.Println(len(nodes))
	//	if id > 0 {
	//		break
	//	}
	//	fmt.Println("粗化第几个分片", id)
	//	NewGraph := g.ShardToGraph(nodes, id)
	//	NewShards, shardWeights = NewGraph.SubCommunityMove(shardWeights, id)
	//	for id, nodes := range NewShards {
	//		for _, node := range nodes {
	//			nodeToShard[node] = id
	//		}
	//	}
	//}
	//计算粗话后的权重
	fmt.Println("----------初始化后各分片权重----------")
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
