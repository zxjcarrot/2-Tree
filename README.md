# 2-Tree
[2-Tree](https://www.cidrdb.org/cidr2023/papers/p57-zhou.pdf) explores the idea of seprating hot records from cold ones in tree structures so as to improve the main memory utilization of buffer pool. The architecture breaks a single B-tree structure into two and migrates records between them.  The migration protocol ensures hot records are clusterted tightly in the hot tree so that it gets majority of the accesses. This results in increased memory utilization.  The two tree structures logically expose the same interface as a single tree structure. We also generalized 2-Tree to a N-tree using LSM-tree by adding upward data migration to a LSM-tree. 
## Cite
```
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
