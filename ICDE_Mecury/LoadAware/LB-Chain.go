package LoadAware

import (
	"ICDE_Mecury/Params"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	BatchSize     = Params.BatchSize
	ShardNum      = Params.ShardNum
	TimeGap       = Params.ReconfigTimeGap
	Datasize      = Params.TotalDataSize
	ExtraDatasize = Params.ExtraDataSize
)

// 交易结构体
type transaction struct {
	sender    string
	recipient string
}

// 交易池内账户结构体
type BufferAccount struct {
	isContain map[string]bool
	label     map[string]int
}

// 账户结构体
type AccountLocation struct {
	AddrSet map[string]bool
	label   map[string]int
}

// TransactionBuffer 结构体
type TransactionBuffer struct {
	buffer     map[int][]transaction
	bufferSize int
}

type TransactionSet struct {
	Sets map[int]TransactionBuffer
}

type Agent struct {
	AddrSet map[string]bool
	label   map[string]int
	primary map[string]string
}

type SuperAccount struct {
	AddrSet map[string]bool
	Label   map[string]map[int]string
}

type SortData struct {
	Key   string
	Value int
}

type SortData2 struct {
	Key   int
	Value float64
}

// 新建TransactionBuffer
func NewTransactionBuffer(size int) *TransactionBuffer {
	return &TransactionBuffer{
		buffer:     make(map[int][]transaction),
		bufferSize: size,
	}
}

// 向缓冲区添加一个事务
func (tb *TransactionBuffer) Add(idx int, t transaction) {
	tb.buffer[idx] = append(tb.buffer[idx], t)
}

func AddFromFile(filename string) map[int]TransactionBuffer {
	// 打开文件
	file, _ := os.Open(filename)
	defer file.Close()
	// 创建CSV读取器
	reader := csv.NewReader(file)
	rows, _ := reader.ReadAll()
	TransSet := make(map[int]TransactionBuffer)
	TotalBufferSize := math.Ceil(float64(Datasize) / float64(TimeGap))
	fmt.Println("Total buffer size: ", TotalBufferSize)
	// 创建TransactionBuffer
	for epoch := 0; epoch < int(TotalBufferSize); epoch++ {
		transactionbuffer := NewTransactionBuffer(Params.BlockSize)
		index := 0
		Begin := epoch * TimeGap
		End := (epoch + 1) * TimeGap
		if End > Params.TotalDataSize {
			End = Params.TotalDataSize
		}
		transNumber := 0
		for _, row := range rows[Begin:End] {
			if len(row) < 5 {
				// Skip rows that don't have enough columns
				continue
			}
			// 获取第四列和第五列的账户地址
			addr1 := strings.TrimSpace(row[3][2:]) // Remove any leading/trailing spaces and skip the '0x'
			addr2 := strings.TrimSpace(row[4][2:]) // Same for addr2
			// 创建新的 trans 事务
			trans := transaction{sender: addr1, recipient: addr2}
			// 添加事务到缓冲区
			transactionbuffer.buffer[index] = append(transactionbuffer.buffer[index], trans)
			transNumber++
			// Check if buffer is full, if so increment index
			if transNumber%transactionbuffer.bufferSize == 0 {
				index++
			}
		}
		TransSet[epoch] = *transactionbuffer
	}
	return TransSet
}

func (tb *TransactionBuffer) LBChainAllocation(AddShard *AccountLocation, ShardNum int) (map[int]float64, int) {
	BuAc := &BufferAccount{
		isContain: make(map[string]bool),
		label:     make(map[string]int),
	}
	CrossEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	ShardAccount := make(map[int][]string)
	AccountLoad := make(map[string]map[int]int, ShardNum)
	AccountFreq := make(map[string]int)
	for i := 0; i < ShardNum; i++ {
		TotalShardLoad[i] = 0.0
	}
	for _, transactions := range tb.buffer {
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
			if AddShard.AddrSet[u] && AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				v_label := Addr2Shard(v)
				AddShard.label[v] = v_label
				AddShard.AddrSet[v] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if AddShard.AddrSet[v] && !AddShard.AddrSet[u] {
				v_label := AddShard.label[v]
				u_label := Addr2Shard(u)
				AddShard.label[u] = u_label
				AddShard.AddrSet[u] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
			if !AddShard.AddrSet[u] && !AddShard.AddrSet[v] {
				v_label := Addr2Shard(v)
				u_label := Addr2Shard(u)
				AddShard.label[v] = v_label
				AddShard.label[u] = u_label
				AddShard.AddrSet[u] = true
				AddShard.AddrSet[v] = true
				if u_label == v_label {
					TotalShardLoad[u_label] += 1.0
					AccountLoad[u][u_label] += 1
					AccountLoad[v][v_label] += 1
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					AccountLoad[v][u_label] += 1
					AccountLoad[u][v_label] += 1
				}
				ShardAccount[u_label] = append(ShardAccount[u_label], u)
				ShardAccount[v_label] = append(ShardAccount[v_label], v)
			}
		}
	}
	//for shard, load := range TotalShardLoad {
	//	fmt.Println("分片", shard, "优化前负载", load)
	//}
	OptimizedShardLoad := make(map[int]float64)
	for index, workload := range TotalShardLoad {
		OptimizedShardLoad[index] = workload
	}
	OptimizedShardLoad = LBChainOptimization(AddShard, OptimizedShardLoad, ShardAccount, AccountLoad, AccountFreq)
	//for shard, load := range OptimizedShardLoad {
	//	fmt.Println("分片", shard, "优化后负载", load)
	//}
	return TotalShardLoad, CrossEdges
}

func LBChainOptimization(AddShard *AccountLocation, TotalShardLoad map[int]float64, ShardAccount map[int][]string, AccountLoad map[string]map[int]int, AccountFreq map[string]int) map[int]float64 {
	isTransfer := make(map[string]bool)
	maxiters := 100
	for iters := 0; iters < maxiters; iters++ {
		TotalShardLoad, ShardAccount, isTransfer = AccountMove(AddShard, TotalShardLoad, ShardAccount, isTransfer, AccountLoad, AccountFreq)
	}
	return TotalShardLoad
}

func AccountMove(AddShard *AccountLocation, TotalShardLoad map[int]float64, ShardAccount map[int][]string, isTransfer map[string]bool, AccountLoad map[string]map[int]int, AccountFreq map[string]int) (map[int]float64, map[int][]string, map[string]bool) {
	OverShard, LeastShard := SelectShard(TotalShardLoad)
	ShardLoad := make(map[int]float64)
	AccountList := make(map[int][]string)
	for index, workload := range TotalShardLoad {
		ShardLoad[index] = workload
	}
	for index, shard := range ShardAccount {
		AccountList[index] = shard
	}
	for ShardIndex, _ := range AccountList {
		if ShardIndex == OverShard {
			var freqSlice []SortData
			for _, v := range AccountList[ShardIndex] {
				freqSlice = append(freqSlice, SortData{v, AccountFreq[v]})
			}
			sort.Slice(freqSlice, func(i, j int) bool {
				return freqSlice[i].Value > freqSlice[j].Value // 降序排序
			})
			AccountIndex := 0
			for {
				Account := freqSlice[AccountIndex].Key
				AccountShardFreqeuncy := AccountLoad[Account]
				Freqeuncy := AccountFreq[Account]
				Move := IsAccountTransfer(ShardLoad, AccountShardFreqeuncy, Freqeuncy, OverShard, LeastShard)
				if Move {
					//fmt.Println("Shard", OverShard, "--->", "Shard", LeastShard)
					AddShard.label[Account] = LeastShard
					ShardLoad[LeastShard] += float64(Freqeuncy - AccountShardFreqeuncy[LeastShard])
					ShardLoad[OverShard] -= float64(Freqeuncy - AccountShardFreqeuncy[OverShard])
					AccountList[LeastShard] = append(AccountList[LeastShard], Account)
					AccountList[OverShard] = removeElement(AccountList[OverShard], Account)
					isTransfer[Account] = true
					break
				} else {
					AccountIndex += 1
				}
				if AccountIndex == len(freqSlice) {
					break
				}
			}
			//分割线
			//for AccountIndex < len(freqSlice) && isTransfer[freqSlice[AccountIndex].Key] {
			//	AccountIndex += 1
			//}
			//if AccountIndex >= len(freqSlice) {
			//	continue
			//}
			//Account := freqSlice[AccountIndex].Key
			//AccountShardFreqeuncy := AccountLoad[Account]
			//Freqeuncy := AccountFreq[Account]
			//Move := IsAccountTransfer(ShardLoad, AccountShardFreqeuncy, Freqeuncy, OverShard, LeastShard)
			//if Move {
			//	//fmt.Println("Shard", OverShard, "--->", "Shard", LeastShard)
			//	AddShard.label[Account] = LeastShard
			//	ShardLoad[LeastShard] += float64(Freqeuncy - AccountShardFreqeuncy[LeastShard])
			//	ShardLoad[OverShard] -= float64(Freqeuncy - AccountShardFreqeuncy[OverShard])
			//	AccountList[LeastShard] = append(AccountList[LeastShard], Account)
			//	AccountList[OverShard] = removeElement(AccountList[OverShard], Account)
			//	isTransfer[Account] = true
			//	break
			//}
		}
	}
	return ShardLoad, AccountList, isTransfer
}

func SelectShard(TotalShardLoad map[int]float64) (int, int) {
	var shardSlice []SortData2
	for k, v := range TotalShardLoad {
		shardSlice = append(shardSlice, SortData2{k, v})
	}

	// 根据负载排序（降序和升序）
	sort.Slice(shardSlice, func(i, j int) bool {
		return shardSlice[i].Value > shardSlice[j].Value // 负载降序排序
	})

	// 获取负载最重和最轻的分片
	overShard := shardSlice[0].Key                  // 负载最重的分片
	lightShard := shardSlice[len(shardSlice)-1].Key // 负载最轻的分片
	return overShard, lightShard
}

func IsAccountTransfer(TotalShardLoad map[int]float64, AccountShardFreqeuncy map[int]int, Frq int, OverShard int, LeastShard int) bool {
	ShardLoad := make(map[int]float64)
	for index, workload := range TotalShardLoad {
		ShardLoad[index] = workload
	}
	Old := calculateVariance(ShardLoad)
	ShardLoad[OverShard] -= float64(Frq - AccountShardFreqeuncy[OverShard])
	ShardLoad[LeastShard] += float64(Frq - AccountShardFreqeuncy[LeastShard])
	New := calculateVariance(ShardLoad)
	if New < Old {
		return true
	}
	return false
}

// calculateVariance 计算 TotalShardLoad 的方差
func calculateVariance(ShardLoad map[int]float64) float64 {
	// 计算均值 (mean)
	var sum float64
	for _, load := range ShardLoad {
		sum += load
	}
	mean := sum / float64(len(ShardLoad))
	// 计算方差
	var variance float64
	for _, load := range ShardLoad {
		diff := load - mean
		variance += diff * diff
	}
	variance /= float64(len(ShardLoad)) // 样本方差 (除以 N)
	return variance
}

func Addr2Shard(addr string) int {
	last8_addr := addr
	if last8_addr == "" {
		log.Println("Error: last8_addr is empty")
		return 1
	}
	if len(last8_addr) > 8 {
		last8_addr = last8_addr[len(last8_addr)-8:]
	}
	num, err := strconv.ParseUint(last8_addr, 16, 64)
	if err != nil {
		log.Panic(err)
	}
	return int(num) % Params.ShardNum
}

//func Addr2Shard(addr string) int {
//	last8_addr := addr
//	if last8_addr == "" {
//		log.Println("Error: last8_addr is empty")
//		return 1
//	}
//	if len(last8_addr) > 8 {
//		last8_addr = last8_addr[len(last8_addr)-8:]
//	}
//
//	// 尝试解析为十六进制数字
//	num, err := strconv.ParseUint(last8_addr, 16, 64)
//	if err != nil {
//		log.Printf("Error parsing address: %s, error: %v\n", last8_addr, err)
//		return 0 // 返回默认值或错误处理逻辑
//	}
//	// 计算分片编号
//	return int(num) % Params.ShardNum
//}

//func Addr2Shard(addr string) int {
//	last8_addr := addr
//	if len(last8_addr) > 8 {
//		last8_addr = last8_addr[len(last8_addr)-8:]
//	}
//	// 检查 last8_addr 是否为有效的十六进制字符串
//	_, err := strconv.ParseUint(last8_addr, 16, 64)
//	if err != nil {
//		log.Printf("Error: parsing %s as uint: %v", last8_addr, err)
//		return -1
//	}
//	num, _ := strconv.ParseUint(last8_addr, 16, 64)
//	return int(num) % Params.ShardNum
//}

func removeElement(arr []string, value string) []string {
	var newArr []string
	for _, v := range arr {
		if v != value {
			newArr = append(newArr, v)
		}
	}
	return newArr
}

func LB_Chain() {
	// 直接使用TransactionBuffer，而不需要TransactionPool
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	TotalCrossEdges := 0
	TotalShardLoad := make(map[int]float64)
	//TransactionSets := AddFromFileExtra(Params.Filename)
	TransactionSets := AddFromFile(Params.Filename)
	start := time.Now()
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges := transactionBuffer.LBChainAllocation(AddrShard, ShardNum)
		TotalCrossEdges += Crossedges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
	}
	cost := time.Since(start)
	fmt.Println(cost)
	fmt.Println("跨分片通信次数", TotalCrossEdges)
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "优化后负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
	file, err := os.Create("graph_lb.txt") //create a new file
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

func LB_Chain2() {
	// 直接使用TransactionBuffer，而不需要TransactionPool
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	TotalCrossEdges := 0
	TotalShardLoad := make(map[int]float64)
	parseFile("graph_lb.txt", AddrShard)
	TransactionSets := AddFromFileExtra(Params.Filename)
	//TransactionSets := AddFromFile(Params.Filename)
	start := time.Now()
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges := transactionBuffer.LBChainAllocation(AddrShard, ShardNum)
		TotalCrossEdges += Crossedges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
	}
	cost := time.Since(start)
	fmt.Println(cost)
	filename := fmt.Sprintf("./label.csv")
	WriteLabelMapToFile(AddrShard.label, filename)
	fmt.Println("跨分片通信次数", TotalCrossEdges)
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "优化后负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
}

func AddFromFileExtra(filename string) map[int]TransactionBuffer {
	// 打开文件
	file, _ := os.Open(filename)
	defer file.Close()
	// 创建CSV读取器
	reader := csv.NewReader(file)
	rows, _ := reader.ReadAll()
	TransSet := make(map[int]TransactionBuffer)
	TotalBufferSize := math.Ceil(float64(ExtraDatasize) / float64(TimeGap))
	fmt.Println("Total buffer size: ", TotalBufferSize)
	// 创建TransactionBuffer
	Total := Params.TotalDataSize + Params.ExtraDataSize
	for epoch := 0; epoch < int(TotalBufferSize); epoch++ {
		transactionbuffer := NewTransactionBuffer(Params.BlockSize)
		index := 0
		Begin := Datasize + epoch*TimeGap
		End := Datasize + (epoch+1)*TimeGap
		if End > Total {
			End = Total
		}
		transNumber := 0
		for _, row := range rows[Begin:End] {
			if len(row) < 5 {
				// Skip rows that don't have enough columns
				continue
			}
			// 获取第四列和第五列的账户地址
			addr1 := strings.TrimSpace(row[3][2:]) // Remove any leading/trailing spaces and skip the '0x'
			addr2 := strings.TrimSpace(row[4][2:]) // Same for addr2
			// 创建新的 trans 事务
			trans := transaction{sender: addr1, recipient: addr2}
			// 添加事务到缓冲区
			transactionbuffer.buffer[index] = append(transactionbuffer.buffer[index], trans)
			transNumber++
			// Check if buffer is full, if so increment index
			if transNumber%transactionbuffer.bufferSize == 0 {
				index++
			}
		}
		TransSet[epoch] = *transactionbuffer
	}
	return TransSet
}
