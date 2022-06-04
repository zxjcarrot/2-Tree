for update_or_put in 0 1
do
for inclusive_cache in 0 1
do
for cache_dram_ratio in 0.95
do
log_file="noninline_cached_btree_ycsb_${cache_dram_ratio}_inclusive_${inclusive_cache}_put_${update_or_put}.log"
# empty log file
cat /dev/null > $log_file
for rw_ratio in 0 20 40 60 80 100
do
build/frontend/ycsb --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.7 --worker_threads=1 --cached_btree=2 --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$rw_ratio --ssd_path=/mnt/disks/nvme/leanstore --inclusive_cache=$inclusive_cache --run_for_seconds=300 --update_or_put=$update_or_put >> $log_file 2>&1
done
done
done
done