# TieredIndexing
Tiered Indexing is a general way to improve the memory utilization of buffer-managed data structures including B+tree, Hashing, Heap, and Log-Structured-Merge Tree. It is generalization of [2-Tree](https://www.cidrdb.org/cidr2023/papers/p57-zhou.pdf). The philosophy of Tiered Indexing is to maintain a hierarchy of homogeneous index structures with different hotness that share a buffer pool. Tiered Indexing actively performs efficient inter-tier record migration based on record hotness. 

Pointers to tiered data structures:
* 2B+tree: `backend/twotree/ConcurrentTwoBTree.hpp`
* 2Hash: `backend/twohash/TwoHash.hpp`
* 2Heap: `backend/twoheap/TwoIHeap.hpp`
* BiLSM-tree: `backend/lsmt/bidirectional_migration_rocksdb_adapter.hpp`

## Cite

```
@article{zhou2025tiered,
  title={Tiered-Indexing: Optimizing Access Methods for Skew},
  author={Zhou, Xinjing and Hao, Xiangpeng and Yu, Xiangyao and Stonebraker, Michael},
  journal={The VLDB Journal},
  volume={34},
  number={4},
  pages={1--24},
  year={2025},
  publisher={Springer}
}
@inproceedings{2-tree,
  author       = {Zhou, Xinjing and Yu, Xiangyao and Graefe, Goetz and Stonebraker, Michael},
  title        = {Two is Better Than One: The Case for 2-Tree for Skewed Data Sets},
  booktitle    = {13th Conference on Innovative Data Systems Research, {CIDR} 2023,
                  Amsterdam, Online Proceedings},
  year         = {2023}
}
```
# LeanStore
[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling
Install dependencies:

`sudo apt-get install cmake libaio-dev libtbb-dev libbz2-dev liblz4-dev libsnappy-dev zlib1g-dev libwiredtiger-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

## TPC-C Example
`build/frontend/tpcc --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=240 --tpcc_warehouse_count=100 --notpcc_warehouse_affinity --csv_path=./log --cool_pct=40 --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60`

check `build/frontend/tpcc --help` for other options

## Cite
The code we used for our CIDR 2021 paper is in a different (and outdated) [branch](https://github.com/leanstore/leanstore/tree/cidr).

```
@inproceedings{alhomssi21,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Contention and Space Management in B-Trees},
    booktitle = {CIDR},
    year      = {2021}
}
```
