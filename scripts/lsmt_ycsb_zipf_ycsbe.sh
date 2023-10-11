top_component_dram_ratio=0.9
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=400
SLEEP_COUNT=20
WORKERS=20
TUPLE_COUNT=100000000
RESULT_DIR_PREFIX=results/ycsbe/


for dram_budget in 5 #2.5 10 5 20
do
run_time=$RUNTIME
for zipf_factor in 0.9 #0.7 0.9
do
log_file="${RESULT_DIR_PREFIX}/bilsmt_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.log"
# empty log file
cat /dev/null > $log_file
csv_file="${RESULT_DIR_PREFIX}/bilsmt_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.csv"
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=0 --ycsb_update_ratio=5 --ycsb_scan_ratio=95 --update_or_put=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_adaptive_migration=1 --results_csv_file_path=$csv_file --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done


# for dram_budget in 2.5 10 5 20
# do
# run_time=$RUNTIME
# for zipf_factor in 0.7 0.9
# do
# log_file="${RESULT_DIR_PREFIX}/lsmtrc_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/lsmtrc_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=LSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=0 --ycsb_update_ratio=5 --ycsb_scan_ratio=95 --update_or_put=0  --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=true --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --results_csv_file_path=$csv_file >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done


# for dram_budget in 2.5 5 #2.5 10 5 20
# do
# run_time=$RUNTIME
# for zipf_factor in 0.9 #0.7 0.9
# do
# log_file="${RESULT_DIR_PREFIX}/lsmt_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/lsmt_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=LSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=0 --ycsb_update_ratio=5 --ycsb_scan_ratio=95 --update_or_put=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --results_csv_file_path=$csv_file >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done


# for dram_budget in 10 5 2.5 20
# do
# run_time=$RUNTIME
# for zipf_factor in 0.7 0.9
# do
# log_file="${RESULT_DIR_PREFIX}/2btree_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/2btree_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=C2BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=0 --ycsb_update_ratio=5 --ycsb_scan_ratio=95 --update_or_put=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_adaptive_migration=1 --results_csv_file_path=$csv_file --cache_lazy_migration=30 --xmerge=1 >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done

# for dram_budget in  10 5 2.5 20
# do
# run_time=$RUNTIME
# for zipf_factor in 0.7 0.9
# do
# log_file="${RESULT_DIR_PREFIX}/treeline_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/treeline_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=Treeline  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=0 --ycsb_update_ratio=5 --ycsb_scan_ratio=95 --update_or_put=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_adaptive_migration=1 --results_csv_file_path=$csv_file --cache_lazy_migration=30 >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done



# for dram_budget in 10 5 2.5 20
# do
# run_time=$RUNTIME
# for zipf_factor in 0.7 0.9
# do
# log_file="${RESULT_DIR_PREFIX}/btree_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/btree_ycsb_zipf_${zipf_factor}_dram_${dram_budget}_ycsbe.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=0 --ycsb_update_ratio=5 --ycsb_scan_ratio=95 --update_or_put=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_adaptive_migration=1 --results_csv_file_path=$csv_file --cache_lazy_migration=30 --xmerge=1 >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
