package LoadAware

import (
	"ICDE_Mecury/Params"
	"bufio"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

func (tb *TransactionBuffer) AgentAllocation(AddShard *AccountLocation, ShardNum int) (map[int]float64, int, int, map[string]int, map[string]map[int]int) {
	BuAc := &BufferAccount{
		isContain: make(map[string]bool),
		label:     make(map[string]int),
	}
	CrossEdges := 0
	InnerEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	TotalAccountLoad := make(map[string]map[int]int, ShardNum)
	TotalAccountFreq := make(map[string]int)
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
			//maxLabel := MaxLoadShard(BlockShardLoad)
			u := t.sender
			v := t.recipient
			AccountFreq[u] += 1
			AccountFreq[v] += 1
			TotalAccountFreq[u] += 1
			TotalAccountFreq[v] += 1
			if !BuAc.isContain[u] {
				BuAc.isContain[u] = true
				AccountLoad[u] = make(map[int]int, ShardNum)
				TotalAccountLoad[u] = make(map[int]int, ShardNum)
			}
			if !BuAc.isContain[v] {
				BuAc.isContain[v] = true
				AccountLoad[v] = make(map[int]int, ShardNum)
				TotalAccountLoad[v] = make(map[int]int, ShardNum)
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
					InnerEdges += 1
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
					TotalAccountLoad[u][u_label] += 1
					TotalAccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
					TotalAccountLoad[v][u_label] += 1
					TotalAccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				Tem_label := AddShard.label[u]
				if BlockShardLoad[u_label] > float64(1.25*float64(Params.BlockSize/Params.ShardNum)) {
					Tem_label = MinLoadShard(BlockShardLoad)
				}
				//if Tem_label == MaxLoadShard(BlockShardLoad) {
				//	Tem_label = MinLoadShard(BlockShardLoad)
				//}
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[v] = Tem_label
				v_label := AddShard.label[v]
				AddShard.AddrSet[v] = true
				if u_label == v_label {
					InnerEdges += 1
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
					TotalAccountLoad[v][v_label] += 1
					TotalAccountLoad[u][u_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
					TotalAccountLoad[v][u_label] += 1
					TotalAccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[v] && !AddShard.AddrSet[u] {
				v_label := AddShard.label[v]
				Tem_label := AddShard.label[v]
				if BlockShardLoad[v_label] > float64(1.25*float64(Params.BlockSize/Params.ShardNum)) {
					Tem_label = MinLoadShard(BlockShardLoad)
				}
				//if Tem_label == MaxLoadShard(BlockShardLoad) {
				//	Tem_label = MinLoadShard(BlockShardLoad)
				//}
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[u] = Tem_label
				u_label := AddShard.label[u]
				AddShard.AddrSet[u] = true
				if u_label == v_label {
					InnerEdges += 1
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[v][v_label] += 1
					AccountLoad[u][u_label] += 1
					TotalAccountLoad[u][u_label] += 1
					TotalAccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
					TotalAccountLoad[v][u_label] += 1
					TotalAccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if !AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				InnerEdges += 1
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
				TotalAccountLoad[u][u_label] += 1
				TotalAccountLoad[v][v_label] += 1
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
		}
		OverShard := IsOverload(BlockShardLoad)
		//fmt.Println("OverShard:", OverShard)
		superAccountList := SuperAccountConstruct(AccountFreq)
		//fmt.Println("superAccountList:", superAccountList)
		TotalShardLoad, CrossEdges = AgentConstruct(OverShard, superAccountList, AccountLoad, BlockShardLoad, CrossEdges, AddShard.label)
	}
	return TotalShardLoad, CrossEdges, InnerEdges, TotalAccountFreq, TotalAccountLoad
}

func (tb *TransactionBuffer) HermesAllocation(AddShard *AccountLocation, ShardNum int) (map[int]float64, int, int, map[string]int, map[string]map[int]int) {
	BuAc := &BufferAccount{
		isContain: make(map[string]bool),
		label:     make(map[string]int),
	}
	CrossEdges := 0
	InnerEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	TotalAccountLoad := make(map[string]map[int]int, ShardNum)
	TotalAccountFreq := make(map[string]int)
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
			//maxLabel := MaxLoadShard(BlockShardLoad)
			u := t.sender
			v := t.recipient
			AccountFreq[u] += 1
			AccountFreq[v] += 1
			TotalAccountFreq[u] += 1
			TotalAccountFreq[v] += 1
			if !BuAc.isContain[u] {
				BuAc.isContain[u] = true
				AccountLoad[u] = make(map[int]int, ShardNum)
				TotalAccountLoad[u] = make(map[int]int, ShardNum)
			}
			if !BuAc.isContain[v] {
				BuAc.isContain[v] = true
				AccountLoad[v] = make(map[int]int, ShardNum)
				TotalAccountLoad[v] = make(map[int]int, ShardNum)
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
					InnerEdges += 1
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
					TotalAccountLoad[u][u_label] += 1
					TotalAccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
					TotalAccountLoad[v][u_label] += 1
					TotalAccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				Tem_label := AddShard.label[u]
				if BlockShardLoad[u_label] > float64(1.25*float64(Params.BlockSize/Params.ShardNum)) {
					Tem_label = MinLoadShard(BlockShardLoad)
				}
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[v] = Tem_label
				v_label := AddShard.label[v]
				AddShard.AddrSet[v] = true
				if u_label == v_label {
					InnerEdges += 1
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
					TotalAccountLoad[v][v_label] += 1
					TotalAccountLoad[u][u_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
					TotalAccountLoad[v][u_label] += 1
					TotalAccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[v] && !AddShard.AddrSet[u] {
				v_label := AddShard.label[v]
				Tem_label := AddShard.label[v]
				if BlockShardLoad[v_label] > float64(1.25*float64(Params.BlockSize/Params.ShardNum)) {
					Tem_label = MinLoadShard(BlockShardLoad)
				}
				//Tem_label := MinLoadShard(BlockShardLoad)
				AddShard.label[u] = Tem_label
				u_label := AddShard.label[u]
				AddShard.AddrSet[u] = true
				if u_label == v_label {
					InnerEdges += 1
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					AccountLoad[v][v_label] += 1
					AccountLoad[u][u_label] += 1
					TotalAccountLoad[u][u_label] += 1
					TotalAccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
					TotalAccountLoad[v][u_label] += 1
					TotalAccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if !AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				InnerEdges += 1
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
				TotalAccountLoad[u][u_label] += 1
				TotalAccountLoad[v][v_label] += 1
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
		}
		//OverShard := IsOverload(BlockShardLoad)
		////fmt.Println("OverShard:", OverShard)
		//superAccountList := SuperAccountConstruct(AccountFreq)
		////fmt.Println("superAccountList:", superAccountList)
		//TotalShardLoad, CrossEdges = AgentConstruct(OverShard, superAccountList, AccountLoad, BlockShardLoad, CrossEdges, AddShard.label)
	}
	return TotalShardLoad, CrossEdges, InnerEdges, TotalAccountFreq, TotalAccountLoad
}

func MaxLoadShard(ShardLoad map[int]float64) int {
	maxLabel := -1
	maxWeight := -math.MaxFloat64
	for label, weight := range ShardLoad {
		if weight > maxWeight {
			maxWeight = weight
			maxLabel = label
		}
	}
	return maxLabel
}

func MinLoadShard(ShardLoad map[int]float64) int {
	minLabel := -1
	minWeight := math.MaxFloat64
	for label, weight := range ShardLoad {
		if weight < minWeight {
			minWeight = weight
			minLabel = label
		}
	}
	return minLabel
}

func IsOverload(ShardLoad map[int]float64) map[int]bool {
	OverloadShard := make(map[int]bool)
	threshod := 1.25 * float64(Params.BatchSize/Params.ShardNum)
	for shard, load := range ShardLoad {
		if load >= threshod {
			OverloadShard[shard] = true
		}
	}
	return OverloadShard
}

func SuperAccountConstruct(AccountFreq map[string]int) []string {
	superAccount := make([]string, 0)
	for Account, frequency := range AccountFreq {
		if frequency > Params.Activity {
			superAccount = append(superAccount, Account)
		}
	}
	return superAccount
}

func AgentConstruct(OverloadShard map[int]bool, superAccount []string, AccountLoad map[string]map[int]int, BlockShardLoad map[int]float64, Edges int, AddShard map[string]int) (map[int]float64, int) {
	Lowthreshod := 1.25 * float64(Params.BatchSize/Params.ShardNum)
	ShardLoad := make(map[int]float64)
	CrossEdges := Edges
	for shard, load := range BlockShardLoad {
		ShardLoad[shard] = load
	}
	for _, Account := range superAccount {
		location := AddShard[Account]
		if OverloadShard[location] && ShardLoad[location] > Lowthreshod {
			for idx := range ShardNum {
				if AccountLoad[Account][idx] > Params.HotFrequency && location != idx {
					CrossEdges -= AccountLoad[Account][idx]
					ShardLoad[location] -= float64(AccountLoad[Account][idx])
				}
			}
		}
	}
	return ShardLoad, CrossEdges
}

func Hermes_Agent() {
	// 直接使用TransactionBuffer，而不需要TransactionPool
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	//fmt.Println(AddrShard)
	TotalCrossEdges := 0
	TotalInnerEdges := 0
	TotalShardLoad := make(map[int]float64)
	TotalAccountLoad := make(map[string]map[int]int, ShardNum)
	TotalAccountFreq := make(map[string]int)
	superAccount := make(map[string]map[int]string)
	//parseFile("graph_FG.txt", AddrShard)
	//TransactionSets := AddFromFileExtra(Params.Filename)
	TransactionSets := AddFromFile(Params.Filename)
	start := time.Now()
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges, InnerEdges, AccountFrq, AccountLoad := transactionBuffer.AgentAllocation(AddrShard, ShardNum)
		TotalCrossEdges += Crossedges
		TotalInnerEdges += InnerEdges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
		for account, frq := range AccountFrq {
			TotalAccountFreq[account] += frq
		}
		for account, Load := range AccountLoad {
			if TotalAccountLoad[account] == nil {
				TotalAccountLoad[account] = make(map[int]int)
			}
			for ShardIndx, Shardload := range Load {
				TotalAccountLoad[account][ShardIndx] += Shardload
			}
		}
	}
	//superAccount = MapConstruct(AddrShard, TotalAccountFreq, TotalAccountLoad, AddrShard.label)
	filename := fmt.Sprintf("D:\\block-emulato_2025\\label.csv")
	WriteLabelMapToFile(AddrShard.label, filename)
	WriteSuAccountToFile(superAccount)
	writeCommunitiesToCSV(AddrShard.label)
	cost := time.Since(start)
	fmt.Println(cost)
	fmt.Println("跨分片通信次数", TotalCrossEdges)
	fmt.Println("分片内通信次数", TotalInnerEdges)
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "优化负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
	file, err := os.Create("graph_FG.txt") //create a new file
	if err != nil {
		fmt.Println(err)
	}
	fmt.Fprintf(file, "%s\n", cost)
	for key, value := range AddrShard.label {
		_, err := fmt.Fprintf(file, "%s: %d\n", value, key)
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
	}
	defer file.Close()
}

func parseFile(filename string, addrShard *AccountLocation) {
	file, _ := os.Open(filename)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ": ")
		if len(parts) != 2 {
			continue // 跳过格式不对的行
		}

		address := parts[0]
		var value int
		_, err := fmt.Sscanf(parts[1], "%d", &value)
		if err != nil {
			continue // 跳过无效数值行
		}

		// 更新 AddrShard 结构
		addrShard.AddrSet[address] = true
		addrShard.label[address] = value
	}
}

func WriteSuAccountToFile(suAccountLabel map[string]map[int]string) error {
	// Create or open the file
	file, _ := os.Create("./superAccount.csv")
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header row (account, shard, agent)
	writer.Write([]string{"Account", "Shard", "Agent"})

	// Iterate over the suAccount.Label map and write each entry to the CSV
	for account, shardMap := range suAccountLabel {
		for shard, agent := range shardMap {
			// Write account, shard, and agent to the CSV
			err := writer.Write([]string{account, fmt.Sprintf("%d", shard), agent})
			if err != nil {
				return fmt.Errorf("failed to write row: %v", err)
			}
		}
	}
	return nil
}

func MapConstruct(AddrShard *AccountLocation, AccountFrq map[string]int, AccountLoad map[string]map[int]int, location map[string]int, Activity int) map[string]map[int]string {
	AccountInShard := make(map[string]map[int]string, 0)
	for Account, Frq := range AccountFrq {
		addressPrefix := Account
		if Frq > Activity {
			for shard, load := range AccountLoad[Account] {
				if load > 10 && shard != location[Account] {
					hexValue := fmt.Sprintf("%x", shard)
					NewAccount := addressPrefix + hexValue
					if AccountInShard[Account] == nil {
						AccountInShard[Account] = make(map[int]string)
					}
					AccountInShard[Account][shard] = NewAccount
					AddrShard.AddrSet[NewAccount] = true
					AddrShard.label[NewAccount] = shard
				}
			}
		}
	}
	return AccountInShard
}

func Hermes_Agent_2(Activity int) {
	// 直接使用TransactionBuffer，而不需要TransactionPool
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	//fmt.Println(AddrShard)
	TotalCrossEdges := 0
	TotalInnerEdges := 0
	TotalShardLoad := make(map[int]float64)
	TotalAccountLoad := make(map[string]map[int]int, ShardNum)
	TotalAccountFreq := make(map[string]int)
	superAccount := make(map[string]map[int]string)
	parseFile("graph_FG.txt", AddrShard)
	TransactionSets := AddFromFileExtra(Params.Filename)
	start := time.Now()
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges, InnerEdges, AccountFrq, AccountLoad := transactionBuffer.HermesAllocation(AddrShard, ShardNum)
		TotalCrossEdges += Crossedges
		TotalInnerEdges += InnerEdges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
		for account, frq := range AccountFrq {
			TotalAccountFreq[account] += frq
		}
		for account, Load := range AccountLoad {
			if TotalAccountLoad[account] == nil {
				TotalAccountLoad[account] = make(map[int]int)
			}
			for ShardIndx, Shardload := range Load {
				TotalAccountLoad[account][ShardIndx] += Shardload
			}
		}
	}
	superAccount = MapConstruct(AddrShard, TotalAccountFreq, TotalAccountLoad, AddrShard.label, Activity)
	filename := fmt.Sprintf("D:\\Mecury_Platform\\label.csv")
	WriteLabelMapToFile(AddrShard.label, filename)
	WriteSuAccountToFile(superAccount)
	//writeCommunitiesToCSV(AddrShard.label)
	cost := time.Since(start)
	fmt.Println(cost)
	fmt.Println("跨分片通信比例", float64(TotalCrossEdges)/float64(TotalCrossEdges+TotalInnerEdges))
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "优化前负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
}
