top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=800
SLEEP_COUNT=20
TUPLE_COUNT=100000000
WORKERS=20



# /ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=100000000 --dram_gib=3.125 --worker_threads=20 --index_type=UpLSMT --top_component_dram_ratio=0.8 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=800 --ycsb_request_dist=zipfian --zipf_factor=0.9 --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30


RUNTIME=3000

for prepop_block_cache in 1 0
do
for dram_budget in 3.125 6.25 12.5 25
do
run_time=$RUNTIME
log_file="uplsmt_ycsb_zipf_dram_${dram_budget}_prepop_cache_${prepop_block_cache}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=UpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_populate_block_cache_for_compaction_and_flush=$prepop_block_cache --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
done


RUNTIME=3000

for prepop_block_cache in 1 0
do
for dram_budget in 3.125 6.25 12.5 25
do
run_time=$RUNTIME
log_file="lsmtrc_ycsb_zipf_dram_${dram_budget}_prepop_cache_${prepop_block_cache}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=LSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=true --lsmt_populate_block_cache_for_compaction_and_flush=$prepop_block_cache --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
done



RUNTIME=800
for prepop_block_cache in 1 0
do
for dram_budget in 3.125 6.25 12.5 25
do
run_time=$RUNTIME
log_file="lsmt_ycsb_zipf_dram_${dram_budget}_prepop_cache_${prepop_block_cache}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=LSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_populate_block_cache_for_compaction_and_flush=$prepop_block_cache --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
done

