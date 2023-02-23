top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=800
SLEEP_COUNT=20
TUPLE_COUNT=20000000

for dram_budget in  1 2 4 8
do
run_time=$RUNTIME
log_file="1btree_ycsb_zipf_dram_${dram_budget}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=10 --index_type=BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
