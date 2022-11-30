cache_dram_ratio=0.9
EXEC=/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf
RUNTIME=2000
TUPLE_COUNT=8388608
SLEEP_COUNT=100

/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=20000000 --dram_gib=1 --worker_threads=1 --index_type=BTree  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --ycsb_request_dist=zipfian --zipf_factor=0.9 --ycsb_request_hotspot_op_fraction=1 --inclusive_cache --cache_lazy_migration=100 --xmerge


for dram_budget in 2
do
run_time=$RUNTIME
log_file="2btree_ycsb_zipf_dram_${dram_budget}gib_inclusive_eager.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=2BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=1 --ycsb_request_hotspot_op_fraction=1 --inclusive_cache --cache_lazy_migration=100 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done

for dram_budget in 1
do
run_time=$RUNTIME
log_file="2btree_ycsb_zipf_dram_${dram_budget}gib_exclusive_eager.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 # 0
do
update_ratio=$((100-read_ratio))
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=2BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=1 --ycsb_request_hotspot_op_fraction=1  --cache_lazy_migration=100 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
