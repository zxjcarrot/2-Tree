top_component_dram_ratio=0.8
EXEC=/ssd1/xinjing/2-Tree/build/frontend/ycsb_zipf
RUNTIME=1500
SLEEP_COUNT=20
DATA=12800000000
WORKERS=20
DRAM_BUDGET=12.5


log_file="2btree_ycsb_zipf_record.log"
# empty log file
cat /dev/null > $log_file

log_file="btree_ycsb_zipf_record.log"
# empty log file
cat /dev/null > $log_file


for record_size in 56 120 248 504 1016 2040
do
TUPLE_COUNT="$(( $DATA / ($record_size + 8) ))"
echo $TUPLE_COUNT
cat <<EOT >> greetings.txt
#pragma once
#define RECORD_SIZE $record_size
EOT
cd build
make -j 10
cd ..


log_file="2btree_ycsb_zipf_record.log"
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=C2BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done

log_file="btree_ycsb_zipf_record.log"
for read_ratio in 100 50
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$DRAM_BUDGET --worker_threads=$WORKERS --index_type=BTree  --top_component_dram_ratio=$top_component_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/flash1/xinjing/2-Tree/build/leanstore_data/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --lsmt_db_path=/flash1/xinjing/2-Tree/build/rocksdb --inclusive_cache --cache_lazy_migration=30 >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done


done


