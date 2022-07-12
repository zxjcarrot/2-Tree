#sh scripts/anticache_ycsb_hotspot.sh 
#sh scripts/cached_trie_ycsb_hotspot.sh 

sh scripts/concurrent_partitioned_leanstore_ycsb_hotspot.sh
sh scripts/concurrent_btree_ycsb_hotspot.sh
sh scripts/concurrent_twobtree_ycsb_hotspot.sh

#sh scripts/btree_ycsb_hotspot.sh
#sh scripts/cached_btree_ycsb_hotspot_lazy.sh 


#sh scripts/rocksdb_ycsb_hotspot.sh
#sh scripts/cached_btree_ycsb.sh


# sh scripts/rocksdb_ycsb.sh
# sh scripts/btree_ycsb.sh
# sh scripts/cached_btree_ycsb.sh
# sh scripts/noninline_cached_btree_ycsb.sh
# sh scripts/cached_btree_ycsb_lazy.sh
# sh scripts/noninline_cached_btree_ycsb_lazy.sh
# sh scripts/writeonly_varying_dataset_size.sh
