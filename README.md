# TieredIndexing
Tiered Indexing is a general way to improve the memory utilization of buffer-managed data structures including B+tree, Hashing, Heap, and Log-Structured-Merge Tree. The philosophy of Tiered Indexing is to maintain a hierarchy of homogeneous index structures with different hotness that share a buffer pool. Tiered Indexing actively performs efficient inter-tier record migration based on record hotness. 

Pointers to tiered data structures:
* 2B+tree: `backend/twotree/ConcurrentTwoBTree.hpp`
* 2Hash: `backend/twohash/TwoHash.hpp`
* 2Heap: `backend/twoheap/TwoIHeap.hpp`
* BiLSM-tree: `backend/lsmt/bidirectional_migration_rocksdb_adapter.hpp`
