package GP

import (
	"fmt"
	"math"
	"sort"
)

type State struct {
	NetGraph          Graph          // 需运行CLPA算法的图
	PartitionMap      map[Vertex]int // 记录分片信息的 map，某个节点属于哪个分片
	Shardload         []int          //记录分片负载
	VertexsNumInShard []int          // Shard 内节点的数目
	WeightPenalty     float64        // 权重惩罚，对应论文中的 beta
	MinEdges2Shard    int            // 最少的 Shard 邻接边数，最小的 total weight of edges associated with label k
	MaxWeightShard    int            // 最大的 Shard 权重，最小的 total weight of edges associated with label k
	MaxIterations     int            // 最大迭代次数，constraint，对应论文中的\tau
	CrossShardEdgeNum int            // 跨分片边的总数
	ShardNum          int            // 分片数目
	GraphHash         []byte
	TotalLoad         int
	Throuput          []float64
}

func (cs *State) ComputeEdges2Shard() {
	CrossLoad := make([]int, cs.ShardNum)
	load := make(map[int]float64, cs.ShardNum)
	cs.CrossShardEdgeNum = 0
	cs.Shardload = make([]int, cs.ShardNum)
	cs.Throuput = make([]float64, cs.ShardNum)
	shardload := make(map[int]float64, cs.ShardNum)
	throuput := make(map[int]float64, cs.ShardNum)
	cs.MinEdges2Shard = math.MaxInt
	cs.MaxWeightShard = math.MinInt
	cs.TotalLoad = 0
	for v, lst := range cs.NetGraph.EdgeSet {
		// 获取节点 v 所属的shard
		vShard := cs.PartitionMap[v]
		for _, u := range lst {
			// 同上，获取节点 u 所属的shard
			uShard := cs.PartitionMap[u]
			if vShard != uShard {
				// 判断节点 v, u 不属于同一分片，则对应的 Edges2Shard 加一
				// 仅计算入度，这样不会重复计算
				CrossLoad[vShard] += cs.NetGraph.WeightSet[v][u]
				load[vShard] += 1.0 * float64(cs.NetGraph.WeightSet[v][u])
				shardload[vShard] += 1.0 * float64(cs.NetGraph.WeightSet[v][u])
				throuput[vShard] += 0.5 * float64(cs.NetGraph.WeightSet[v][u])
			} else {
				load[vShard] += 1.0 * float64(cs.NetGraph.WeightSet[v][u])
				shardload[vShard] += 0.5 * float64(cs.NetGraph.WeightSet[v][u])
				throuput[vShard] += 1.0 * float64(cs.NetGraph.WeightSet[v][u])
			}
		}
	}
	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Shardload[idx] = int(shardload[idx])
		cs.Throuput[idx] = throuput[idx]
	}
	//计算跨分片交易数目
	for _, val := range CrossLoad {
		cs.CrossShardEdgeNum += val
	}
	for _, val := range load {
		cs.TotalLoad += int(val)
	}
	//fmt.Println("跨分片交易边数：", cs.CrossShardEdgeNum/2)
	//fmt.Println("总边数：", cs.TotalLoad/2)
	fmt.Println("跨分片交易比例：", float64(cs.CrossShardEdgeNum)/float64(cs.TotalLoad))
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

func (cs *State) changeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	for _, u := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			cs.Shardload[new] += cs.NetGraph.WeightSet[v][u] //cross-shard +1
			cs.Shardload[old] -= cs.NetGraph.WeightSet[v][u] //cross-shard -1
		} else if neighborShard == new {
			cs.Shardload[old] -= cs.NetGraph.WeightSet[v][u]
			cs.CrossShardEdgeNum -= cs.NetGraph.WeightSet[v][u]
		} else {
			cs.Shardload[new] += cs.NetGraph.WeightSet[v][u]
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

func mean(arr []int) float64 {
	sum := 0.0
	for _, v := range arr {
		sum += float64(v)
	}
	return sum / float64(len(arr))
}

func Median(load []int) float64 {
	n := len(load)
	if n == 0 {
		return 0 // 处理空切片的情况
	}

	sort.Ints(load) // 对切片进行排序

	if n%2 == 1 {
		return float64(load[n/2]) // 奇数长度时取中间值
	}

	// 偶数长度时取中间两个数的平均值
	return float64(load[n/2-1]+load[n/2]) / 2.0
}

// 计算数组的方差
func Variance(arr []int) float64 {
	avg := mean(arr)
	sum := 0.0
	for _, v := range arr {
		sum += math.Pow(float64(v)-avg, 2)
	}
	return sum / float64(len(arr))
}

func MaxLoadShard(load []int) int {
	maxWeight := -math.MaxInt
	for _, weight := range load {
		if weight > maxWeight {
			maxWeight = weight
		}
	}
	return maxWeight
}
