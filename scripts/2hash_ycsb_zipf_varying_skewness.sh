top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=4000
SLEEP_COUNT=20
TUPLE_COUNT=100000000
WORKERS=20
RESULT_DIR_PREFIX=results/skewness/

log_file="${RESULT_DIR_PREFIX}/2hash_ycsb_zipf_dram_skewness2.log"
# empty log file
cat /dev/null > $log_file
run_time=$RUNTIME
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 1.1 #0.1 0.3 0.5 0.7 0.9 1.1
do
csv_file="${RESULT_DIR_PREFIX}/2hash_ycsb_zipf_dram_read_ratio_${read_ratio}_skewness2_${zipf_factor}.csv"
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=5 --worker_threads=$WORKERS --index_type=2Hash  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian2 --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30 --ycsb_2hash_use_different_hash=false --ycsb_2hash_eviction_by_record=false --results_csv_file_path=$csv_file >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done



# log_file="2hash_ycsb_zipf_dram_skewness_eager.log"
# # empty log file
# cat /dev/null > $log_file
# run_time=$RUNTIME
# for read_ratio in 100 50
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.1 0.3 0.5 0.7 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=6.25 --worker_threads=$WORKERS --index_type=2Hash  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=100 --ycsb_2hash_use_different_hash=false --ycsb_2hash_eviction_by_record=false>> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done


# log_file="2hash_ycsb_zipf_dram_skewness_exclusive_migration.log"
# # empty log file
# cat /dev/null > $log_file
# run_time=$RUNTIME
# for read_ratio in 100 50
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.1 0.3 0.5 0.7 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=6.25 --worker_threads=$WORKERS --index_type=2Hash  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --cache_lazy_migration=30 --ycsb_2hash_use_different_hash=false --ycsb_2hash_eviction_by_record=false>> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
