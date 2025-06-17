Our source code implements Mercury's transaction allocation logic and reproduces the transaction allocation logic of CLPA, PLouvain, Txallo, ShardScheduler, LB-Chain, Monoxide, and Metis. Additionally, we simplified and modified the transaction allocation logic of the BlockEmulator platform, setting the number of shards to 12. Reviewers and readers can quickly run simulation experiments (approximately 3 minutes) on low-specification Windows machines to compare Mercury’s performance against various other schemes.

The data is sourced from Ethereum transactions collected by Xblock [https://www.xblock.pro]. The system prototype is based on the BlockEmulator provided by the Huang Lab. 
We gratefully acknowledge their outstanding contribution.

Note: Datasets can be downloaded from [https://www.xblock.pro/#/dataset/14].