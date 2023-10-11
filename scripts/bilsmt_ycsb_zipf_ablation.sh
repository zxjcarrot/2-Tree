top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=2500
SLEEP_COUNT=20
WORKERS=20
TUPLE_COUNT=100000000
RESULT_DIR_PREFIX=results/bilsmt_ablation/

RUNTIME=3000
dram_budget=5

run_time=$RUNTIME
zipf_factor=0.9
read_ratio=100
update_ratio=$((100-read_ratio))
# log_file="${RESULT_DIR_PREFIX}/bilsmt_unopt_ycsb_zipf.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/bilsmt_unopt_ycsb_zipf_dram.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=0 --lsmt_bulk_migration_at_flush=0 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --cache_lazy_migration=100 >> $log_file 2>&1
# 



# log_file="${RESULT_DIR_PREFIX}/bilsmt_constprobmig_im_ycsb_zipf.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/bilsmt_constprobmig_im_ycsb_zipf.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=0 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_constant_mig_prob=true --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --cache_lazy_migration=30 >> $log_file 2>&1



# log_file="${RESULT_DIR_PREFIX}/bilsmt_expprobmig_im_ycsb_zipf.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/bilsmt_expprobmig_im_ycsb_zipf.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=0 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_constant_mig_prob=false --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --cache_lazy_migration=30 >> $log_file 2>&1






log_file="${RESULT_DIR_PREFIX}/bilsmt_expprobmig_im_bulkm_ycsb_zipf.log"
# empty log file
cat /dev/null > $log_file
csv_file="${RESULT_DIR_PREFIX}/bilsmt_expprobmig_im_bulkm_ycsb_zipf.csv"
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_constant_mig_prob=false --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --cache_lazy_migration=30 >> $log_file 2>&1




# log_file="${RESULT_DIR_PREFIX}/bilsmt_expprobmig_im_bulkm_am_ycsb_zipf.log"
# # empty log file
# cat /dev/null > $log_file
# csv_file="${RESULT_DIR_PREFIX}/bilsmt_expprobmig_im_bulkm_am_ycsb_zipf.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_constant_mig_prob=false --lsmt_adaptive_migration=1 --results_csv_file_path=$csv_file --cache_lazy_migration=30 >> $log_file 2>&1


