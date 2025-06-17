package LoadAware

import (
	"ICDE_Mecury/Params"
	"fmt"
	"os"
)

func (tb *TransactionBuffer) SchedulerAllocation(AddShard *AccountLocation, ShardNum int) (map[int]float64, int) {
	BuAc := &BufferAccount{
		isContain: make(map[string]bool),
		label:     make(map[string]int),
	}
	CrossEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	for i := 0; i < ShardNum; i++ {
		TotalShardLoad[i] = 0.0
	}
	BlockShardLoad := make(map[int]float64, ShardNum)
	for _, transactions := range tb.buffer {
		ShardAccount := make(map[int][]string)
		AccountLoad := make(map[string]map[int]int, ShardNum)
		AccountFreq := make(map[string]int)
		for i := 0; i < ShardNum; i++ {
			BlockShardLoad[i] = 0.0
		}
		//threshod:=float64(2*Params.BatchSize/Params.ShardNum)
		for _, t := range transactions {
			u := t.sender
			v := t.recipient
			AccountFreq[u] += 1
			AccountFreq[v] += 1
			if !BuAc.isContain[u] {
				BuAc.isContain[u] = true
				AccountLoad[u] = make(map[int]int, ShardNum)
			}
			if !BuAc.isContain[v] {
				BuAc.isContain[v] = true
				AccountLoad[v] = make(map[int]int, ShardNum)
			}
			if AccountLoad[u] == nil {
				AccountLoad[u] = make(map[int]int, ShardNum)
			}
			if AccountLoad[v] == nil {
				AccountLoad[v] = make(map[int]int, ShardNum)
			}
			if AddShard.AddrSet[u] && AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[v] = u_label //Tem_label
				v_label := AddShard.label[v]
				AddShard.AddrSet[v] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[v] && !AddShard.AddrSet[u] {
				v_label := AddShard.label[v]
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[u] = v_label //Tem_label
				u_label := AddShard.label[u]
				AddShard.AddrSet[u] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if !AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				minLabel := MinLoadShard(BlockShardLoad)
				AddShard.label[u] = minLabel
				AddShard.label[v] = minLabel
				AddShard.AddrSet[u] = true
				AddShard.AddrSet[v] = true
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				TotalShardLoad[u_label] += 1.0
				BlockShardLoad[u_label] += 1.0
				AccountLoad[u][u_label] += 1
				AccountLoad[v][v_label] += 1
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
		}
		AccountScheduler(AddShard.label, AccountLoad, BlockShardLoad)
		//OverShard := IsOverload(BlockShardLoad)
		////fmt.Println("OverShard:", OverShard)
		//superAccountList := SuperAccountConstruct(AccountFreq)
		////fmt.Println("superAccountList:", superAccountList)
		//TotalShardLoad, CrossEdges = AgentConstruct(OverShard, superAccountList, AccountLoad, BlockShardLoad, CrossEdges, AddShard.label)
	}
	return TotalShardLoad, CrossEdges
}

func (tb *TransactionBuffer) SchedulerAllocation2(AddShard *AccountLocation, ShardNum int) (map[int]float64, int) {
	BuAc := &BufferAccount{
		isContain: make(map[string]bool),
		label:     make(map[string]int),
	}
	CrossEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	for i := 0; i < ShardNum; i++ {
		TotalShardLoad[i] = 0.0
	}
	BlockShardLoad := make(map[int]float64, ShardNum)
	for _, transactions := range tb.buffer {
		ShardAccount := make(map[int][]string)
		AccountLoad := make(map[string]map[int]int, ShardNum)
		AccountFreq := make(map[string]int)
		for i := 0; i < ShardNum; i++ {
			BlockShardLoad[i] = 0.0
		}
		//threshod:=float64(2*Params.BatchSize/Params.ShardNum)
		for _, t := range transactions {
			u := t.sender
			v := t.recipient
			AccountFreq[u] += 1
			AccountFreq[v] += 1
			if !BuAc.isContain[u] {
				BuAc.isContain[u] = true
				AccountLoad[u] = make(map[int]int, ShardNum)
			}
			if !BuAc.isContain[v] {
				BuAc.isContain[v] = true
				AccountLoad[v] = make(map[int]int, ShardNum)
			}
			if AccountLoad[u] == nil {
				AccountLoad[u] = make(map[int]int, ShardNum)
			}
			if AccountLoad[v] == nil {
				AccountLoad[v] = make(map[int]int, ShardNum)
			}
			if AddShard.AddrSet[u] && AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[v] = u_label //Tem_label
				v_label := AddShard.label[v]
				AddShard.AddrSet[v] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[v] && !AddShard.AddrSet[u] {
				v_label := AddShard.label[v]
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[u] = v_label //Tem_label
				u_label := AddShard.label[u]
				AddShard.AddrSet[u] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if !AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				minLabel := MinLoadShard(BlockShardLoad)
				AddShard.label[u] = minLabel
				AddShard.label[v] = minLabel
				AddShard.AddrSet[u] = true
				AddShard.AddrSet[v] = true
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				TotalShardLoad[u_label] += 1.0
				BlockShardLoad[u_label] += 1.0
				AccountLoad[u][u_label] += 1
				AccountLoad[v][v_label] += 1
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
		}
		AccountScheduler(AddShard.label, AccountLoad, BlockShardLoad)
		//OverShard := IsOverload(BlockShardLoad)
		////fmt.Println("OverShard:", OverShard)
		//superAccountList := SuperAccountConstruct(AccountFreq)
		////fmt.Println("superAccountList:", superAccountList)
		//TotalShardLoad, CrossEdges = AgentConstruct(OverShard, superAccountList, AccountLoad, BlockShardLoad, CrossEdges, AddShard.label)
	}
	return TotalShardLoad, CrossEdges
}

func AccountScheduler(AddrShard map[string]int, AccountLoad map[string]map[int]int, BlockShardLoad map[int]float64) {
	ShardLoad := make(map[int]float64)
	for index, Load := range BlockShardLoad {
		ShardLoad[index] = Load
	}
	for Account, Location := range AddrShard {
		LoadNotInLocation := 0
		LoadInLocation := AccountLoad[Account][Location]
		for index := range Params.ShardNum {
			if index != Location {
				LoadNotInLocation += AccountLoad[Account][index]
			}
		}
		if LoadNotInLocation >= Params.MigrationWeight*LoadInLocation {
			MinShard := MinLoadShard(ShardLoad)
			AddrShard[Account] = MinShard
			ShardLoad[Location] -= float64(LoadNotInLocation)
			ShardLoad[MinShard] += float64(LoadNotInLocation - AccountLoad[Account][MinShard] + LoadInLocation)
		}
	}
}

func ShardScheduler() {
	// 直接使用TransactionBuffer，而不需要TransactionPool
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	TotalCrossEdges := 0
	TotalShardLoad := make(map[int]float64)
	parseFile("graph.txt", AddrShard)
	//TransactionSets := AddFromFileExtra(Params.Filename)
	TransactionSets := AddFromFile(Params.Filename)
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges := transactionBuffer.SchedulerAllocation(AddrShard, ShardNum)
		TotalCrossEdges += Crossedges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
	}
	fmt.Println("跨分片通信次数", TotalCrossEdges)
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "优化负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
	file, err := os.Create("graph_ss.txt") //create a new file
	if err != nil {
		fmt.Println(err)
	}
	for key, value := range AddrShard.label {
		_, err := fmt.Fprintf(file, "%s: %d\n", key, value)
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
	}
}

func ShardScheduler2() {
	// 直接使用TransactionBuffer，而不需要TransactionPool
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	TotalCrossEdges := 0
	TotalShardLoad := make(map[int]float64)
	parseFile("graph_ss.txt", AddrShard)
	TransactionSets := AddFromFileExtra(Params.Filename)
	//TransactionSets := AddFromFile(Params.Filename)
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges := transactionBuffer.SchedulerAllocation2(AddrShard, ShardNum)
		TotalCrossEdges += Crossedges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
	}
	filename := fmt.Sprintf("./label.csv")
	WriteLabelMapToFile(AddrShard.label, filename)
	fmt.Println("跨分片通信次数", TotalCrossEdges)
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "优化负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
}
