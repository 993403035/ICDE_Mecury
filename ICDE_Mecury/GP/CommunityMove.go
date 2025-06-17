package GP

import (
	"fmt"
	"sort"
)

// 分片重构图
func (g *Graph) ShardToGraph(Shards []Vertex, id int) *Graph {
	fmt.Println("节点个数:", len(Shards))
	NewGraph := new(Graph)
	//初始化图
	for _, node := range Shards {
		for _, neighbor := range g.EdgeSet[node] {
			if g.Community[neighbor] == id {
				NewGraph.AddCommunityEdge(node, neighbor, g.WeightSet[node][neighbor])
			}
		}
	}
	//细粒度分割
	NewGraph.Community = CommunityCovert(NewGraph.LabelPropagation())
	//NewGraph.Community = CommunityCovert(NewGraph.LouvainPartition(1, 0.5))
	fmt.Println("节点个数", len(NewGraph.Community))
	//NewGraph.Community = NewGraph.LouvainPartition(1, 10)
	//计算子社区权重
	return NewGraph
}

func (NewGraph *Graph) SubCommunityMove(ShardWeights []int, id int) ([][]Vertex, []int) {
	//计算子社区权重
	CommunityWeight := make(map[int]float64)
	for node := range NewGraph.VertexSet {
		for _, nbr := range NewGraph.EdgeSet[node] {
			if NewGraph.Community[node] != NewGraph.Community[nbr] {
				CommunityWeight[NewGraph.Community[node]] += 1.0 * float64(NewGraph.WeightSet[node][nbr])
			}
			if NewGraph.Community[node] == NewGraph.Community[nbr] {
				CommunityWeight[NewGraph.Community[node]] += 0.5 * float64(NewGraph.WeightSet[node][nbr])
			}
		}
	}
	//子社区添加元素
	SubCommunity := make([][]Vertex, len(CommunityWeight))
	for node, id := range NewGraph.Community {
		SubCommunity[id] = append(SubCommunity[id], node)
	}
	// 将社区按权重排序
	type communityInfo struct {
		label  int
		weight int
	}
	SubCommunities := make([]communityInfo, 0, len(CommunityWeight))
	for label, Weight := range CommunityWeight {
		SubCommunities = append(SubCommunities, communityInfo{label, int(Weight)})
	}
	// 按权重从大到小排序
	sort.Slice(SubCommunities, func(i, j int) bool {
		return SubCommunities[i].weight > SubCommunities[j].weight
	})
	ShardWeights[id] = 0
	//Shards添加元素
	Shards := make([][]Vertex, len(ShardWeights))
	for _, Sub := range SubCommunities {
		minShard := 0
		for i := 1; i < len(ShardWeights); i++ {
			if ShardWeights[i] < ShardWeights[minShard] {
				minShard = i
			}
		}
		ShardWeights[minShard] += Sub.weight
		Shards[minShard] = append(Shards[minShard], SubCommunity[Sub.label]...)
	}
	return Shards, ShardWeights
}
