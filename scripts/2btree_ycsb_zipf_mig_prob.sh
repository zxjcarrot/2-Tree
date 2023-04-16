top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=4000
SLEEP_COUNT=20
TUPLE_COUNT=100000000

for zipf_factor in 0.8
do
for dram_budget in 5
do
run_time=$RUNTIME
log_file="2btree_ycsb_zipf_dram_${dram_budget}_migration_probability_${zipf_factor}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for mig_prob in 1 10 20 30 40 50 60 70 80 90 100
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=10 --index_type=C2BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=$mig_prob >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
done
