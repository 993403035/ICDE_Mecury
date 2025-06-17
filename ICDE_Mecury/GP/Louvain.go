package GP

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"strconv"
)

// 改进鲁汶算法
func (g *Graph) LouvainPartition(resolution float64, threshold float64) map[Vertex]int {
	mod := 0.0                        //增益值
	id := 0                           //社区序号
	Community := make(map[Vertex]int) //节点标签
	//初始化社区和节点权重
	for v := range g.VertexSet {
		Community[v] = id
		id++
	}
	//初始化
	g.NodeDegree = make(map[Vertex]int, len(g.VertexSet))
	for u, neighbors := range g.WeightSet {
		for v := range neighbors {
			g.NodeDegree[u] += g.WeightSet[u][v] //计算节点u的权重
		}
	}
	//计算初始mod值
	mod = g.modularity(Community, resolution)
	fmt.Println("mod:", mod)
	m := float64(g.Edges)
	Partition, Inner_Partiontion, Improvement := g.OneLevel(m)
	g.Community = CommunityCovert(Inner_Partiontion)
	fmt.Println("CommunityNumber:", Partition)
	level := 1
	Improvement = true
	//NewMod := g.modularity(g.Community, resolution)
	//fmt.Println(NewMod)
	//Partition, Inner_Partiontion,Improvement = NetGraph.OnePartition(Inner_Partiontion,m)
	for {
		if Improvement == false || Partition < 10000 {
			break
		}
		NewMod := g.modularity(g.Community, resolution)
		fmt.Println("NewMod:", NewMod)
		if NewMod-mod <= threshold {
			fmt.Println("Level", level)
			return g.Community
		}
		mod = NewMod
		NetGraph := new(Graph)
		Community_to_node := NetGraph.GenGraph(*g, Partition, level)
		Partition, Inner_Partiontion, Improvement = NetGraph.ManyLevel(g.Community, Community_to_node, m)
		g.Community = CommunityCovert(Inner_Partiontion)
		fmt.Println("CommunityNumber:", Partition)
		level += 1
	}
	//	fmt.Println("Caculated threshold:", NewMod-mod)
	//}
	return g.Community
}

func (g *Graph) modularity(community map[Vertex]int, resolution float64) float64 {
	modularity := 0.0
	degree := 0
	for _, wt := range g.NodeDegree {
		degree += wt
	}
	m := float64(degree / 2)
	norm := 1 / math.Pow(float64(degree), 2)
	IntraEdges := make(map[int]float64)
	for u, neighbors := range g.WeightSet {
		for neighbor := range neighbors {
			for Weight := range g.WeightSet[u][neighbor] {
				if community[u] == community[neighbor] {
					IntraEdges[community[u]] += 0.5 * float64(Weight)
				}
			}
		}
	}
	CommunityDegree := make(map[int]float64)
	//计算每个社区的贡献度
	for u, neighbors := range g.WeightSet {
		for v := range neighbors {
			if community[u] != community[v] {
				CommunityDegree[community[u]] += 1.0 * float64(g.WeightSet[u][v]) //计算节点u初始社区的权重
			}
			if community[u] == community[v] {
				CommunityDegree[community[u]] += 0.5 * float64(g.WeightSet[u][v]) //计算节点u初始社区的权重
			}
		}
	}
	for i := range len(CommunityDegree) {
		degree := CommunityDegree[i]
		length := IntraEdges[i]
		CommunityContribution := float64(length/m - resolution*float64(degree)*float64(degree)*norm)
		modularity += CommunityContribution
	}
	return modularity
}

// 一次迭代
func (g *Graph) OneLevel(m float64) (int, map[Vertex]int, bool) {
	id := 0
	node2com := make(map[Vertex]int)
	//自成社区
	for v := range g.VertexSet {
		node2com[v] = id
		id++
	}
	//初始化权重列表
	CommunityDegree := make(map[int]int)
	for u, neighbors := range g.WeightSet {
		for v := range neighbors {
			CommunityDegree[node2com[u]] += g.WeightSet[u][v] //计算节点u初始社区的权重
		}
	}
	improvement := false
	best_community := 0
	moves := 1
	for moves > 0 {
		moves = 0
		for v := range g.VertexSet {
			current_community := node2com[v]
			best_community = g.BestCommunity(node2com, CommunityDegree, v, m)
			if current_community != best_community {
				CommunityDegree[current_community] = CommunityDegree[current_community] - g.NodeDegree[v]
				CommunityDegree[best_community] = CommunityDegree[best_community] + g.NodeDegree[v]
				node2com[v] = best_community
				moves += 1
				improvement = true
				if CommunityDegree[current_community] == 0 {
					delete(CommunityDegree, current_community)
				}
			}
		}
		fmt.Println("Moves:", moves)
	}
	return len(CommunityDegree), node2com, improvement
}

// 多层鲁汶思想
func (g *Graph) ManyLevel(Community map[Vertex]int, CommunityId_to_Node map[int]Vertex, m float64) (int, map[Vertex]int, bool) {
	id := 0
	node2com := make(map[Vertex]int)
	//自成社区
	for v := range g.VertexSet {
		node2com[v] = id
		id++
	}
	//初始化权重列表
	TotalWeight := 0
	CommunityDegree := make(map[int]int)
	for u, neighbors := range g.WeightSet {
		for v := range neighbors {
			CommunityDegree[node2com[u]] += g.WeightSet[u][v] //计算节点u初始社区的权重
			TotalWeight += g.WeightSet[u][v]
		}
	}

	improvement := false
	best_community := 0
	moves := 1
	for moves > 0 {
		moves = 0
		for v := range g.VertexSet {
			current_community := node2com[v]
			best_community = g.BestCommunity(node2com, CommunityDegree, v, m)
			if current_community != best_community {
				CommunityDegree[current_community] = CommunityDegree[current_community] - g.NodeDegree[v]
				CommunityDegree[best_community] = CommunityDegree[best_community] + g.NodeDegree[v]
				node2com[v] = best_community
				moves += 1
				improvement = true
				if CommunityDegree[current_community] == 0 {
					delete(CommunityDegree, current_community)
				}
			}
		}
	}
	for node := range Community {
		//node2com[node] //node社区的标号。node社区和int社区一一对应，应该为赋值号
		CommunityID := Community[node]
		BigNode := CommunityId_to_Node[CommunityID]
		for Node := range node2com {
			if BigNode == Node {
				Community[node] = node2com[BigNode]
			}
		}
	}
	return len(CommunityDegree), Community, improvement
}

func (g *Graph) BestCommunity(communities map[Vertex]int, CommunityDegree map[int]int, v Vertex, m float64) int {
	best_gain := 0.0                                 //最佳增益
	best_community := communities[v]                 //最佳社区初始为本地社区
	v_community_weight_list := make(map[int]float64) //节点v与邻居社区的权重
	a := int(math.Pow(float64(m), 2))                //边数的平方
	resolution := 0.5                                //调节参数,默认为1.0
	for neighbor, _ := range g.WeightSet[v] {
		v_community_weight_list[communities[neighbor]] += 1.0 * float64(g.WeightSet[v][neighbor])
	}
	//从当前社区移除的成本
	//remove_cost = -weights2com[best_com] / m + resolution * ( Stot[best_com] * degree ) / (2 * m ** 2)
	CommunityWithDegree := make(map[int]int)
	for k, v := range CommunityDegree {
		CommunityWithDegree[k] = v
	}
	CommunityWithDegree[communities[v]] -= g.NodeDegree[v]
	RemoveCost := -v_community_weight_list[communities[v]]/m + resolution*float64(CommunityWithDegree[communities[v]])*float64(g.NodeDegree[v])/float64((2*a))
	//迭代邻居节点
	for _, u := range g.EdgeSet[v] {
		gain := RemoveCost + float64(v_community_weight_list[communities[u]])/float64(m) - resolution*float64(CommunityWithDegree[communities[u]])*float64(g.NodeDegree[v])/float64((2*a))
		if gain > best_gain {
			best_community = communities[u]
			best_gain = gain
		}
	}
	return best_community
}

// 社区合并
func (g *Graph) GenGraph(src Graph, Partition int, Level int) map[int]Vertex {
	CommunityId_to_Node := make(map[int]Vertex, Partition)
	NodeDegree := make(map[Vertex]float64, Partition)
	//构建社区节点
	for i := 0; i < Partition; i++ {
		randomNum := new(big.Int)
		randomNum, _ = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 112)) // 112位二进制对应28位十六进制
		// 将随机数转换为28位十六进制字符串
		hexStr := fmt.Sprintf("%028x", randomNum)
		// 拼接上“x”前缀
		resultStr := strconv.Itoa(Level) + "x" + hexStr
		node := Vertex{Addr: resultStr}
		CommunityId_to_Node[i] = node
	}
	//添加边
	SrcCommunity := 0
	TagCommunity := 0
	g.NodeDegree = make(map[Vertex]int, Partition)
	for v, Neighbors := range src.WeightSet {
		SrcCommunity = src.Community[v]
		SrcVertex := CommunityId_to_Node[SrcCommunity]
		for Neighbor, _ := range Neighbors {
			TagCommunity = src.Community[Neighbor]
			TagVertex := CommunityId_to_Node[TagCommunity]
			if SrcCommunity != TagCommunity {
				g.AddWeightEdge(SrcVertex, TagVertex, src.WeightSet[v][Neighbor])
				NodeDegree[SrcVertex] += 1.0 * float64(src.WeightSet[v][Neighbor])
			}
			if SrcCommunity == TagCommunity {
				NodeDegree[SrcVertex] += 0.5 * float64(src.WeightSet[v][Neighbor])
			}
		}
	}
	for v := range NodeDegree {
		g.NodeDegree[v] = int(NodeDegree[v])
	}
	return CommunityId_to_Node
}

// 节点编号重分配
func CommunityCovert(inner_partition map[Vertex]int) map[Vertex]int {
	var values []int
	valueMap := make(map[int]bool)
	for _, value := range inner_partition {
		if _, exists := valueMap[value]; !exists {
			values = append(values, value)
			valueMap[value] = true
		}
	}
	High2Low := make(map[int]int, len(values))
	for i := 0; i < len(values); i++ {
		High2Low[values[i]] = i
	}
	for node, _ := range inner_partition {
		value := inner_partition[node]
		inner_partition[node] = High2Low[value]
	}
	return inner_partition
}
