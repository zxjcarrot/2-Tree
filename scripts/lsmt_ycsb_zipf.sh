top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=2500
SLEEP_COUNT=20
WORKERS=20
TUPLE_COUNT=100000000
RESULT_DIR_PREFIX=results/steady_state/

#/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=200000000 --dram_gib=5 --worker_threads=20 --index_type=C2BTree --top_component_dram_ratio=0.8 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=2400 --ycsb_request_dist=zipfian --zipf_factor=0.9 --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=true --lsmt_in_memory_migration=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --lsmt_adaptive_migration=0 --xmerge=1 --lsmt_bulk_migration_at_flush=1


# /ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=100000000 --dram_gib=5 --worker_threads=20 --index_type=Treeline --top_component_dram_ratio=0.8 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=2000 --ycsb_request_dist=zipfian --zipf_factor=0.9 --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=true --lsmt_in_memory_migration=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --lsmt_adaptive_migration=0 --xmerge=1 --lsmt_bulk_migration_at_flush=1
 

# /ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=100000000 --dram_gib=2.5 --worker_threads=20 --index_type=LSMT --top_component_dram_ratio=0.8 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=1600 --ycsb_request_dist=zipfian --zipf_factor=0.9 --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --lsmt_adaptive_migration=0 --lsmt_bulk_migration_at_flush=1

# /ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=100000000 --dram_gib=2.5 --worker_threads=20 --index_type=BiUpLSMT --top_component_dram_ratio=0.8 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=600 --ycsb_request_dist=zipfian --zipf_factor=0.9 --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --lsmt_in_memory_migration=1 --inclusive_cache --lsmt_use_record_cache=false --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=50 --lsmt_adaptive_migration=1 --lsmt_bulk_migration_at_flush=1


# for dram_budget in 2.5 5 10 20
# do
# run_time=$RUNTIME
# log_file="${RESULT_DIR_PREFIX}/lsmtrc_ycsb_zipf_dram_${dram_budget}.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 100 50
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/lsmtrc_ycsb_zipf_dram_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=LSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=true --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --results_csv_file_path=$csv_file >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
# done


# RUNTIME=2500
# for dram_budget in 2.5 5 10 20
# do
# run_time=$RUNTIME
# log_file="${RESULT_DIR_PREFIX}/lsmt_ycsb_zipf_dram_${dram_budget}.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 100 50
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/lsmt_ycsb_zipf_dram_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=LSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --results_csv_file_path=$csv_file >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
# done

# RUNTIME=2500

# for adaptive_migration in 0
# do
# for bulk_migration_at_flush in 1
# do
# for in_memory_migration in 1
# do
# for dram_budget in 2.5 5 10 20
# do
# run_time=$RUNTIME
# log_file="${RESULT_DIR_PREFIX}/uplsmt_ycsb_zipf_dram_${dram_budget}_adaptive_migration_${adaptive_migration}_im_migration_${in_memory_migration}_bf_migration_${bulk_migration_at_flush}.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 100 50
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/uplsmt_ycsb_zipf_dram_${dram_budget}_a_migration_${adaptive_migration}_im_migration_${in_memory_migration}_bf_migration_${bulk_migration_at_flush}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=$in_memory_migration --lsmt_bulk_migration_at_flush=$bulk_migration_at_flush --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_adaptive_migration=$adaptive_migration --results_csv_file_path=$csv_file --cache_lazy_migration=30 >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
# done
# done
# done
# done


RUNTIME=2500

for dram_budget in 2.5 5 10 20
do
run_time=$RUNTIME
log_file="${RESULT_DIR_PREFIX}/uplsmt_unopt_ycsb_zipf_dram_${dram_budget}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9
do
csv_file="${RESULT_DIR_PREFIX}/uplsmt_unopt_ycsb_zipf_dram_${dram_budget}_read_ratio_${read_ratio}.csv"
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=0 --lsmt_bulk_migration_at_flush=0 --lsmt_populate_block_cache_for_compaction_and_flush=1 --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --cache_lazy_migration=100 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done