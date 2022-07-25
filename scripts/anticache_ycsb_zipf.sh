
for cache_dram_ratio in 0.94
do
log_file="anticache_ycsb_zipf.log"
# empty log file
cat /dev/null > $log_file

for zipf_factor in 0.75 0.9 0.95 0.96 0.97
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.6 --worker_threads=1 --index_type=AntiCache  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=80 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=480 --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
done
done