
for cache_dram_ratio in 0.94
do
log_file="cached_btree_ycsb_hotspot_lazy_${cache_dram_ratio}.log"
# empty log file
cat /dev/null > $log_file
for access_ratio in 0.001 0.01 0.1 0.2 0.3 0.4 0.5
do
build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.3 --worker_threads=1 --cached_btree=2 --ycsb_keyspace_access_ratio=$access_ratio --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=80 --ssd_path=/mnt/disks/nvme/leanstore --cache_lazy_migration=0 --run_for_seconds=160 >> $log_file 2>&1
done
done