package LoadAware

import (
	"ICDE_Mecury/Params"
	"fmt"
	"time"
)

func Mecury_test(Activity int) {
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
	allSuperAccounts := make(map[string]map[int]string)
	superAccount := make(map[string]map[int]string)
	proxy := make(map[string]map[int]bool)
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
		superAccount, proxy = ConstructProxy(AddrShard, AccountFrq, AccountLoad, AddrShard.label, Activity, proxy)
		for account, shardMap := range superAccount {
			if allSuperAccounts[account] == nil {
				allSuperAccounts[account] = make(map[int]string)
			}
			for shard, value := range shardMap {
				allSuperAccounts[account][shard] = value
			}
		}
	}
	//superAccount = MapConstruct(AddrShard, TotalAccountFreq, TotalAccountLoad, AddrShard.label, Activity)
	filename := fmt.Sprintf("./label.csv")
	WriteLabelMapToFile(AddrShard.label, filename)
	WriteSuAccountToFile(allSuperAccounts)
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

func ConstructProxy(AddrShard *AccountLocation, AccountFrq map[string]int, AccountLoad map[string]map[int]int, location map[string]int, Activity int, proxy map[string]map[int]bool) (map[string]map[int]string, map[string]map[int]bool) {
	AccountInShard := make(map[string]map[int]string)
	for Account, Frq := range AccountFrq {
		addressPrefix := Account
		if Frq > Activity {
			if AccountLoad[Account] != nil {
				for shard, load := range AccountLoad[Account] {
					if load > 10 && shard != location[Account] {
						hexValue := fmt.Sprintf("%x", shard)
						NewAccount := addressPrefix + hexValue
						if AccountInShard[Account] == nil {
							AccountInShard[Account] = make(map[int]string)
						}
						if proxy[Account] == nil {
							proxy[Account] = make(map[int]bool, ShardNum)
						}
						if !proxy[Account][shard] {
							proxy[Account][shard] = true
							AccountInShard[Account][shard] = NewAccount
							if AddrShard.AddrSet == nil {
								AddrShard.AddrSet = make(map[string]bool)
							}
							if AddrShard.label == nil {
								AddrShard.label = make(map[string]int)
							}
							AddrShard.AddrSet[NewAccount] = true
							AddrShard.label[NewAccount] = shard
						}
					}
				}
			}
		}
	}
	return AccountInShard, proxy
}
