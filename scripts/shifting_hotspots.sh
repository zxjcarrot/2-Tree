top_component_dram_ratio=0.9
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=9000
SLEEP_COUNT=1
TUPLE_COUNT=100000000
WORKERS=20
DRAM_BUDGET=5
HOTSPOT_SHIFTING_PHASES=3
RESULT_DIR_PREFIX=results/shifting_hotspots/
run_time=$RUNTIME


for dram_budget in 5
do
for read_ratio in 100
do
log_file="${RESULT_DIR_PREFIX}/biuplsmt_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}_${dram_budget}.log"
rm $log_file
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9
do
csv_file="${RESULT_DIR_PREFIX}/biuplsmt_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=40 --lsmt_adaptive_migration=1 --results_csv_file_path=$csv_file --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES >> $log_file 2>&1

sleep $SLEEP_COUNT
done
done
done

# for dram_budget in 5
# do
# for read_ratio in 100
# do
# log_file="${RESULT_DIR_PREFIX}/biuplsmt_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}_${dram_budget}_noam.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/biuplsmt_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}_noam.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=BiUpLSMT --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_in_memory_migration=1 --lsmt_bulk_migration_at_flush=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=30 --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES >> $log_file 2>&1

# sleep $SLEEP_COUNT
# done
# done
# done

# for dram_budget in 5
# do
# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/lsmtrc_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}_${dram_budget}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/lsmtrc_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=LSMT --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=true --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=20 --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
# done


# for dram_budget in 5
# do
# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/2btree_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}_${dram_budget}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/2btree_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=$WORKERS --index_type=C2BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --results_csv_file_path=$csv_file --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=20 --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES > $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
# done



# for read_ratio in 100
# do
# log_file="${RESULT_DIR_PREFIX}/lsmt_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/lsmt_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=LSMT --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=false --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=20 --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done


# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/2heap_indexscan_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/2heap_indexscan_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=2IHeap  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=20 --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES --results_csv_file_path=$csv_file --ycsb_2heap_eviction_index_scan=1 >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done



# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/treeline_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/treeline_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=Treeline --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=12000 --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --lsmt_use_record_cache=true --lsmt_in_memory_migration=1 --lsmt_populate_block_cache_for_compaction_and_flush=1 --cache_lazy_migration=20 --lsmt_adaptive_migration=0 --results_csv_file_path=$csv_file --hotspot_shift_phases=4 >> $log_file 2>&1

# sleep $SLEEP_COUNT
# done
# done


# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/2hash_samehash_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/2hash_samehash_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=2Hash  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES --results_csv_file_path=$csv_file --inclusive_cache --cache_lazy_migration=20 --ycsb_2hash_use_different_hash=false --ycsb_2hash_eviction_by_record=false >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done



# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/2hash_diffhash_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/2hash_diffhash_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=2Hash  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES --results_csv_file_path=$csv_file --inclusive_cache --cache_lazy_migration=20 --ycsb_2hash_use_different_hash=true --ycsb_2hash_eviction_by_record=false >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done


# for read_ratio in 50
# do

# log_file="${RESULT_DIR_PREFIX}/2heap_heapscan_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/2heap_heapscan_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=2IHeap  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=20 --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES --results_csv_file_path=$csv_file --ycsb_2heap_eviction_index_scan=0 >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done





# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/btree_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/btree_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES --results_csv_file_path=$csv_file >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done


# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/hash_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/hash_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=Hash  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES --results_csv_file_path=$csv_file >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done



# for read_ratio in 50
# do
# log_file="${RESULT_DIR_PREFIX}/heap_ycsb_zipf_shifting_hotspots_read_ratio_${read_ratio}.log"
# rm $log_file
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.9
# do
# csv_file="${RESULT_DIR_PREFIX}/heap_ycsb_zipf_shifting_hotspots_${dram_budget}_read_ratio_${read_ratio}.csv"
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=IHeap  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --hotspot_shift_phases=$HOTSPOT_SHIFTING_PHASES --results_csv_file_path=$csv_file >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
