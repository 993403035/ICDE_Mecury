package LoadAware

import (
	"ICDE_Mecury/Params"
	"fmt"
	"time"
)

func (tb *TransactionBuffer) MonoxideAllocation(ShardNum int) (map[int]float64, int) {
	CrossEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	for i := 0; i < ShardNum; i++ {
		TotalShardLoad[i] = 0.0
	}
	for _, transactions := range tb.buffer {
		for _, t := range transactions {
			u := t.sender
			v := t.recipient
			v_label := Addr2Shard(v)
			u_label := Addr2Shard(u)
			if u_label == v_label {
				TotalShardLoad[u_label] += 1.0
			} else {
				CrossEdges += 1
				TotalShardLoad[u_label] += 1.0
				TotalShardLoad[v_label] += 1.0
			}
		}
	}
	return TotalShardLoad, CrossEdges
}

func Monoxide() {
	TotalCrossEdges := 0
	TotalShardLoad := make(map[int]float64)
	start := time.Now()
	TransactionSets := AddFromFileExtra(Params.Filename)
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges := transactionBuffer.MonoxideAllocation(ShardNum)
		TotalCrossEdges += Crossedges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
	}
	fmt.Println(time.Since(start))
	fmt.Println("跨分片通信次数", TotalCrossEdges)
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
}
