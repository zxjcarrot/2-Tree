top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=3000
SLEEP_COUNT=20
TUPLE_COUNT=100000000
WORKERS=20

for dram_budget in 3.125 6.25 12.5 25
do
run_time=$RUNTIME
if (( $(echo "$dram_budget > 12.5" |bc -l) )); then
    run_time=800
fi
log_file="2heap_ycsb_zipf_dram_${dram_budget}_heap_scan.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9 # 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=2IHeap  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30 --ycsb_2heap_eviction_index_scan=0 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

for dram_budget in 3.125 6.25 12.5 25
do
run_time=$RUNTIME
if (( $(echo "$dram_budget > 12.5" |bc -l) )); then
    run_time=800
fi
log_file="2heap_ycsb_zipf_dram_${dram_budget}_indexed_heap_scan.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9 # 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=2IHeap  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --xmerge --cache_lazy_migration=30 --ycsb_2heap_eviction_index_scan=1 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
