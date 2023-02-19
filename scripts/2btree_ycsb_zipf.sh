top_component_dram_ratio=0.8
EXEC=/home/zxjcarrot/Workspace/2-Tree/build/frontend/ycsb_zipf
RUNTIME=2400
SLEEP_COUNT=100
TUPLE_COUNT=150000000

for dram_budget in 7.5 15 30
do
run_time=$RUNTIME
log_file="2btree_ycsb_zipf_dram_${dram_budget}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=10 --index_type=C2BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/home/zxjcarrot/Workspace/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/home/zxjcarrot/Workspace/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done


/home/zxjcarrot/Workspace/2-Tree/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=1500000 --dram_gib=0.75 --worker_threads=10 --index_type=C2BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/home/zxjcarrot/Workspace/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/home/zxjcarrot/Workspace/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30


/home/zxjcarrot/Workspace/2-Tree/build/frontend/ycsb_zipf_debug --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=10 --worker_threads=10 --index_type=C2BTree  --top_component_dram_ratio=0.8 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/home/zxjcarrot/Workspace/2-Tree/build/leanstore_data/leanstore --run_for_seconds=3200 --ycsb_request_dist=zipfian --zipf_factor=0.7 --lsmt_db_path=/home/zxjcarrot/Workspace/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30