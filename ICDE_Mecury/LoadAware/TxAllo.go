package LoadAware

import (
	"ICDE_Mecury/Params"
	"fmt"
	"time"
)

func (tb *TransactionBuffer) TxAlloAllocation(AddShard *AccountLocation, ShardNum int) (map[int]float64, int) {
	CrossEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	for i := 0; i < ShardNum; i++ {
		TotalShardLoad[i] = 0.0
	}
	for _, transactions := range tb.buffer {
		for _, t := range transactions {
			u := t.sender
			v := t.recipient
			if AddShard.AddrSet[u] && AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
				}
			}
			if AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				AddShard.label[v] = Addr2Shard(v)
				v_label := AddShard.label[v]
				AddShard.AddrSet[v] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
				}
			}
			if AddShard.AddrSet[v] && !AddShard.AddrSet[u] {
				v_label := AddShard.label[v]
				AddShard.label[u] = Addr2Shard(u)
				u_label := AddShard.label[u]
				AddShard.AddrSet[u] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
				}
			}
			if !AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				AddShard.label[u] = Addr2Shard(u)
				AddShard.label[v] = Addr2Shard(v)
				AddShard.AddrSet[u] = true
				AddShard.AddrSet[v] = true
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				TotalShardLoad[u_label] += 1.0
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
				}
			}
		}
	}
	return TotalShardLoad, CrossEdges
}

func TxAllo() {
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	TotalCrossEdges := 0
	TotalShardLoad := make(map[int]float64)
	parseFile("graph_txallo.txt", AddrShard)
	TransactionSets := AddFromFileExtra(Params.Filename)
	//TransactionSets := AddFromFile(Params.Filename)
	start := time.Now()
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges := transactionBuffer.clpaAllocation(AddrShard, ShardNum)
		TotalCrossEdges += Crossedges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
	}
	filename := fmt.Sprintf("./label.csv")
	WriteLabelMapToFile(AddrShard.label, filename)
	cost := time.Since(start)
	fmt.Println(cost)
	fmt.Println("跨分片通信次数", float64(TotalCrossEdges)/float64(ExtraDatasize))
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
}
