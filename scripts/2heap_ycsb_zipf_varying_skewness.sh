top_component_dram_ratio=0.9
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=3600
SLEEP_COUNT=20
TUPLE_COUNT=100000000
WORKERS=20
RESULT_DIR_PREFIX=results/skewness/

log_file="${RESULT_DIR_PREFIX}/2heap_ycsb_zipf_dram_skewness.log"
# empty log file
cat /dev/null > $log_file
run_time=$RUNTIME
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.1 0.3 0.5 0.7 0.9 1.1
do
csv_file="${RESULT_DIR_PREFIX}/2heap_ycsb_zipf_dram_read_ratio_${read_ratio}_skewness_${zipf_factor}.csv"
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=5 --worker_threads=$WORKERS --index_type=2IHeap  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian2 --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30 --ycsb_2heap_eviction_index_scan=1 --results_csv_file_path=$csv_file >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
