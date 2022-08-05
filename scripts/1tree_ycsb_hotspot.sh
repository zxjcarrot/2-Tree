cache_dram_ratio=0.9

log_file="lsmt_ycsb_hotspot.log"
# empty log file
cat /dev/null > $log_file

for read_ratio in 100 0
do
for hotspot_ratio in 0.001 0.01 0.1 0.2 0.3 0.4 0.5
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.3 --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=480 --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=$hotspot_ratio  --ycsb_request_hotspot_op_fraction=1 --zipf_factor=0 --xmerge >> $log_file 2>&1
done
done

log_file="btree_ycsb_hotspot.log"
# empty log file
cat /dev/null > $log_file

for read_ratio in 100 0
do
for hotspot_ratio in 0.001 0.01 0.1 0.2 0.3 0.4 0.5
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.3 --worker_threads=1 --index_type=BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=480 --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=$hotspot_ratio  --ycsb_request_hotspot_op_fraction=1 --zipf_factor=0 --xmerge >> $log_file 2>&1
done
done

