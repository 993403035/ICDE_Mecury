package GP

import (
	"ICDE_Mecury/Params"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

type Vertex struct {
	Addr string // 账户地址
}

// Graph 表示区块链交易集合的图
type Graph struct {
	VertexSet  map[Vertex]bool           //节点集合，其实是 set
	EdgeSet    map[Vertex][]Vertex       //记录节点与节点间是否存在交易，邻接表
	WeightSet  map[Vertex]map[Vertex]int //记录边的权重
	Community  map[Vertex]int            //节点标签
	Shard      map[Vertex]int
	IsShard    map[Vertex]bool
	Edges      int            //边数
	NodeDegree map[Vertex]int //每个节点的度数
	Shardload  map[int]int
	Throuput   map[int]float64
}

// 创建节点
func (v *Vertex) ConstructVertex(s string) {
	v.Addr = s
}

// 增加图中的点
func (g *Graph) AddVertex(v Vertex) {
	if g.VertexSet == nil {
		g.VertexSet = make(map[Vertex]bool)
	}
	g.VertexSet[v] = true
}

// 增加图中的边
func (g *Graph) AddEdge(u, v Vertex) {
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}
	//g.EdgeSet[u] = append(g.EdgeSet[u], v)
	//g.EdgeSet[v] = append(g.EdgeSet[v], u)
	if !contains(g.EdgeSet[u], v) {
		g.EdgeSet[u] = append(g.EdgeSet[u], v)
	}
	if !contains(g.EdgeSet[v], u) {
		g.EdgeSet[v] = append(g.EdgeSet[v], u)
	}
	if g.WeightSet == nil {
		g.WeightSet = make(map[Vertex]map[Vertex]int)
	}
	if _, ok := g.WeightSet[u]; !ok {
		g.WeightSet[u] = make(map[Vertex]int)
	}
	if _, ok := g.WeightSet[v]; !ok {
		g.WeightSet[v] = make(map[Vertex]int)
	}
	g.WeightSet[u][v] += 1
	g.WeightSet[v][u] += 1
	g.Edges += 1
}

// 增加超图中的权重边
func (g *Graph) AddWeightEdge(u, v Vertex, weight int) {
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}
	if !contains(g.EdgeSet[u], v) {
		g.EdgeSet[u] = append(g.EdgeSet[u], v)
	}
	if !contains(g.EdgeSet[v], u) {
		g.EdgeSet[v] = append(g.EdgeSet[v], u)
	}
	if g.WeightSet == nil {
		g.WeightSet = make(map[Vertex]map[Vertex]int)
	}
	if _, ok := g.WeightSet[u]; !ok {
		g.WeightSet[u] = make(map[Vertex]int)
	}
	if _, ok := g.WeightSet[v]; !ok {
		g.WeightSet[v] = make(map[Vertex]int)
	}
	g.WeightSet[u][v] += weight
	g.WeightSet[v][u] += weight
	g.Edges += 1
}

// 增加超图中的权重边
func (g *Graph) AddCommunityEdge(u, v Vertex, weight int) {
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}
	if !contains(g.EdgeSet[u], v) {
		g.EdgeSet[u] = append(g.EdgeSet[u], v)
	}
	if !contains(g.EdgeSet[v], u) {
		g.EdgeSet[v] = append(g.EdgeSet[v], u)
	}
	if g.WeightSet == nil {
		g.WeightSet = make(map[Vertex]map[Vertex]int)
	}
	if _, ok := g.WeightSet[u]; !ok {
		g.WeightSet[u] = make(map[Vertex]int)
	}
	if _, ok := g.WeightSet[v]; !ok {
		g.WeightSet[v] = make(map[Vertex]int)
	}
	g.WeightSet[u][v] += weight
}

// 检查节点是否存在
func contains(edges []Vertex, target Vertex) bool {
	for _, v := range edges {
		if v == target {
			return true
		}
	}
	return false
}

func (g *Graph) AddEdgesFromFile(filename string) {
	// 打开CSV文件
	file, _ := os.Open(filename)
	defer file.Close()

	// 创建CSV读取器
	reader := csv.NewReader(file)
	// 读取所有行
	rows, _ := reader.ReadAll()

	// 遍历CSV文件中的每一行，从第2行到第10000行（索引从0开始）
	for _, row := range rows[1:Params.Edges] {
		if len(row) < 5 {
			continue // 如果行数据不足5列，跳过此行
		}
		if row[6] == "0" && row[7] == "0" && len(row[3]) > 16 && len(row[4]) > 16 && row[3] != row[4] {
			// 获取第四列和第五列的账户地址
			addr1 := row[3]
			addr2 := row[4]

			// 创建两个Vertex对象
			v1 := Vertex{Addr: addr1[2:]}
			v2 := Vertex{Addr: addr2[2:]}

			// 增加边
			g.AddEdge(v1, v2)
		}
	}
}

func (g *Graph) writeCommunitiesToCSV(community map[Vertex]int) {
	file, _ := os.Create("144_million_TxAllo")
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	// Write header
	writer.Write([]string{"Vertex Addr", "Community ID"})
	// Write community data
	for v, communityID := range community {
		writer.Write([]string{v.Addr, strconv.Itoa(communityID)})
	}
	fmt.Println("Communities written to InitLouvain.csv")
}

func readCommunitiesFromCSV(filePath string) map[Vertex]int {
	file, _ := os.Open(filePath)
	defer file.Close()
	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()
	records = records[1:]
	community := make(map[Vertex]int)
	for _, record := range records {
		addr := record[0]
		communityID, _ := strconv.Atoi(record[1])
		community[Vertex{Addr: addr}] = communityID
	}
	return community
}
