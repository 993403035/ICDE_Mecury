package Params

var (
	NodesInShard = 4  // # of Nodes in a shard.
	ShardNum     = 12 // # of shards.
)

var (
	BlockSize       = 20000
	BatchSize       = 20000 // The supervisor read a batch of txs then send them. The size of a batch is 'BatchSize'
	Weight          = 2
	MigrationWeight = 2
	OverloadWeight  = 1.5
	TotalDataSize   = 2700000
	ExtraDataSize   = 300000
	ReconfigTimeGap = 20000
	Activity        = 200
	HotFrequency    = Activity / 15
	//Filename        = "./3million.csv"
	Filename      = "D:\\Mecury_Platform\\5million.csv"
	TotalActivity = ""
	TotalFrq      = ""
)

var Edges = TotalDataSize
