cache_dram_ratio=0.94

log_file="lsmt_ycsb_zipf.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
for zipf_factor in 0.75 0.9 0.95 0.96 0.97
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.6 --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=480 --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
done
done

log_file="btree_ycsb_zipf.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
for zipf_factor in 0.75 0.9 0.95 0.96 0.97
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.6 --worker_threads=1 --index_type=BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=480 --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
done
done

