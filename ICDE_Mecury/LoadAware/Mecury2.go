package LoadAware

import (
	"ICDE_Mecury/Params"
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func Mecury() {
	// 直接使用TransactionBuffer，而不需要TransactionPool
	AddrShard := &AccountLocation{
		AddrSet: make(map[string]bool),
		label:   make(map[string]int),
	}
	//fmt.Println(AddrShard)
	TotalCrossEdges := 0
	TotalInnerEdges := 0
	TotalShardLoad := make(map[int]float64)
	superAccount, Exits := ReadSuAccountFromFile("D:\\Mecury_Platform\\superAccount.csv")
	MecuryparseFile("D:\\Mecury_Platform\\label.csv", AddrShard)
	TransactionSets := AddFromFileExtra(Params.Filename)
	start := time.Now()
	for _, transactionBuffer := range TransactionSets {
		ShardLoad, Crossedges, InnerEdges := transactionBuffer.MecuryAllocation(AddrShard, superAccount, Exits, ShardNum)
		TotalCrossEdges += Crossedges
		TotalInnerEdges += InnerEdges
		for shard, workload := range ShardLoad {
			TotalShardLoad[shard] += workload
		}
	}
	cost := time.Since(start)
	fmt.Println(cost)
	fmt.Println("跨分片通信比例", float64(TotalCrossEdges)/float64(TotalCrossEdges+TotalInnerEdges)*100)
	for shard, workload := range TotalShardLoad {
		fmt.Println("分片", shard, "优化负载", workload)
	}
	Variance := calculateVariance(TotalShardLoad)
	fmt.Println(Variance)
}

func (tb *TransactionBuffer) MecuryAllocation(AddShard *AccountLocation, superAccount map[string]map[int]string, Exists map[string]bool, ShardNum int) (map[int]float64, int, int) {
	BuAc := &BufferAccount{
		isContain: make(map[string]bool),
		label:     make(map[string]int),
	}
	CrossEdges := 0
	InnerEdges := 0
	TotalShardLoad := make(map[int]float64, ShardNum)
	for i := 0; i < ShardNum; i++ {
		TotalShardLoad[i] = 0.0
	}
	BlockShardLoad := make(map[int]float64, ShardNum)
	for _, transactions := range tb.buffer {
		for i := 0; i < ShardNum; i++ {
			BlockShardLoad[i] = 0.0
		}
		//threshod:=float64(2*Params.BatchSize/Params.ShardNum)
		for _, t := range transactions {
			//maxLabel := MaxLoadShard(BlockShardLoad)
			u := t.sender
			v := t.recipient
			if Exists[u] && !Exists[v] && AddShard.label[u] != AddShard.label[v] {
				CurrentSender := u
				R_Shard := AddShard.label[v]
				u = superAccount[CurrentSender][R_Shard]
			}
			if Exists[v] && !Exists[u] && AddShard.label[u] != AddShard.label[v] {
				CurrentReciver := v
				S_Shard := AddShard.label[u]
				v = superAccount[CurrentReciver][S_Shard]
				//S_Shard := utils.AddrMap[tx.Sender]
				//tx.Recipient = superAccount[tx.Recipient][S_Shard]
			}
			if Exists[v] && Exists[u] && AddShard.label[u] != AddShard.label[v] {
				CurrentReciver := v
				S_Shard := AddShard.label[u]
				v = superAccount[CurrentReciver][S_Shard]
				//S_Shard := utils.AddrMap[tx.Sender]
				//tx.Recipient = superAccount[tx.Recipient][S_Shard]
			}
			if !BuAc.isContain[u] {
				BuAc.isContain[u] = true
			}
			if !BuAc.isContain[v] {
				BuAc.isContain[v] = true
			}
			if AddShard.AddrSet[u] && AddShard.AddrSet[v] {
				u_label := AddShard.label[u]
				v_label := AddShard.label[v]
				if u_label == v_label {
					InnerEdges += 1
					TotalShardLoad[u_label] += 1.0
					BlockShardLoad[u_label] += 1.0
				} else {
					CrossEdges += 1
					TotalShardLoad[u_label] += 1.0
					TotalShardLoad[v_label] += 1.0
					BlockShardLoad[u_label] += 1.0
					BlockShardLoad[v_label] += 1.0
				}
			}
		}
	}
	return TotalShardLoad, CrossEdges, InnerEdges
}

func ReadSuAccountFromFile(filename string) (map[string]map[int]string, map[string]bool) {
	// Open the CSV file
	file, _ := os.Open(filename)
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read all rows from the CSV
	rows, _ := reader.ReadAll()

	// Skip the header row
	if len(rows) > 0 {
		rows = rows[1:]
	}

	// Create the map to hold the data
	suAccountLabel := make(map[string]map[int]string)
	suAccountExesists := make(map[string]bool)
	// Parse each row and populate the suAccountLabel map
	for _, row := range rows {
		if len(row) < 3 {
			continue // Skip any malformed rows
		}

		account := row[0]
		shard, _ := strconv.Atoi(row[1])

		agent := row[2]

		// Initialize the map for this account if it doesn't exist
		if _, exists := suAccountLabel[account]; !exists {
			suAccountLabel[account] = make(map[int]string)
			suAccountExesists[account] = true
		}

		// Add the shard-agent mapping for the account
		suAccountLabel[account][shard] = agent
	}
	// Return the constructed map
	return suAccountLabel, suAccountExesists
}

func ReadLabelMapFromCSV(filename string) (map[string]int, error) {
	labelMap := make(map[string]int)
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	// 创建 CSV 读取器
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // 允许可变列数
	// 读取所有行
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}
	// 解析每一行
	for _, row := range rows {
		if len(row) == 0 {
			continue // 跳过空行
		}
		parts := strings.Split(row[0], ": ")
		if len(parts) != 2 {
			continue // 跳过格式不正确的行
		}
		addr := strings.TrimSpace(parts[0])
		label, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			continue // 跳过无法解析的行
		}

		labelMap[addr] = label
	}

	return labelMap, nil
}

func MecuryparseFile(filename string, addrShard *AccountLocation) {
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
