# Mercury Transaction Allocation System

## Overview

This repository contains the source code for Mercury's transaction allocation logic, alongside implementations of CLPA, PLouvain, Txallo, ShardScheduler, LB-Chain, Monoxide, and Metis. We have also simplified and modified the transaction allocation logic of the BlockEmulator platform.

## Features

- Reproduces transaction allocation logic for multiple schemes: Mercury, CLPA, PLouvain, Txallo, ShardScheduler, LB-Chain, Monoxide, and Metis.
- Modified BlockEmulator with a fixed configuration of 12 shards.
- Designed for quick simulation experiments (~3 minutes) on low-specification Windows machines.
- Enables performance comparison of Mercury against other schemes such as CLPA, Txallo.

## Data Source

- The data used in this project is sourced from Ethereum transactions collected by Xblock, available at [https://www.xblock.pro/#/dataset/14](https://www.xblock.pro/#/dataset/14). 
- To ensure the dataset size exceeds 6 million for the Mercury transaction allocation system

## System Prototype

The system prototype is based on a fork of the BlockEmulator https://github.com/HuangLab-SYSU/block-emulator provided by the Huang Lab. We gratefully acknowledge their outstanding contribution to this work.

## Usage

1. Ensure your system meets the minimum requirements for running simulations (low-specification Windows machines are supported).
2. Download the required Ethereum transaction dataset from [https://www.xblock.pro/#/dataset/14](https://www.xblock.pro/#/dataset/14).
3. Run the simulation experiments to compare Mercury’s performance against CLPA, PLouvain, Txallo, ShardScheduler, LB-Chain, Monoxide, and Metis.

## Acknowledgments

We extend our gratitude to the Huang Lab for providing the BlockEmulator platform, which forms the basis of our system prototype.
