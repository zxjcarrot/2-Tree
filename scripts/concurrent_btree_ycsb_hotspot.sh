
log_file="concurrent_leanstore_ycsb_hotspot.log"
# empty log file
cat /dev/null > $log_file
for access_ratio in 0.001 0.01 0.1 0.2 0.3 0.4 0.5
do
build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.3 --worker_threads=14 --cached_btree=0 --ycsb_keyspace_access_ratio=$access_ratio   --ycsb_read_ratio=80 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=500 --cached_btree_ram_ratio=0.95 --xmerge >> $log_file 2>&1
done
