package main

import (
	"ICDE_Mecury/GP"
	"ICDE_Mecury/LoadAware"
)

//Provides Mecury and other transaction allocation methods

func main() {
	//case-Mecury
	gp := new(GP.State)
	gp.FG_LPA_Partition()
	LoadAware.Mecury_test(200)
	LoadAware.Mecury()

	//case-CLPA:
	//cp := new(GP.State)
	//cp.CLPA_Partition()
	//LoadAware.CLPA()

	//case-TxAllo:
	//ta := new(GP.State)
	//ta.TxAllo_Partition()
	//LoadAware.TxAllo()

	//case-Plouvain:
	//pl := new(GP.State)
	//pl.PLouvain_Partition()
	//LoadAware.P_louvain()

	//case-Scheduler:
	//LoadAware.ShardScheduler()
	//LoadAware.ShardScheduler2()

	//case-Monoxide:
	//LoadAware.Monoxide()

	//case-LBchain:
	//LoadAware.LB_Chain()
	//LoadAware.LB_Chain2()
}
