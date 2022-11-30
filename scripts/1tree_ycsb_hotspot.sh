cache_dram_ratio=0.9
EXEC=/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf
RUNTIMES=("1200" "1200" "2000" "4000" "4000")
HOTSPOT_RATIOS=("0.001" "0.01" "0.1" "0.2" "0.3")
SLEEP_COUNT=100
TUPLE_COUNT=20000000
dram_budget=1
log_file="lsmt_ycsb_hotspot_record_cache.log"
# empty log file
cat /dev/null > $log_file

for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for hotspot_ratio_idx in "${!HOTSPOT_RATIOS[@]}"
do
hotspot_ratio=${HOTSPOT_RATIOS[$hotspot_ratio_idx]}
RUNTIME=${RUNTIMES[$hotspot_ratio_idx]}
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --lsmt_use_record_cache=true --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$RUNTIME --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=$hotspot_ratio  --ycsb_request_hotspot_op_fraction=1 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done


log_file="lsmt_ycsb_hotspot.log"
# empty log file
cat /dev/null > $log_file

for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for hotspot_ratio_idx in "${!HOTSPOT_RATIOS[@]}"
do
hotspot_ratio=${HOTSPOT_RATIOS[$hotspot_ratio_idx]}
RUNTIME=${RUNTIMES[$hotspot_ratio_idx]}
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --lsmt_use_record_cache=false --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$RUNTIME --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=$hotspot_ratio  --ycsb_request_hotspot_op_fraction=1 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done

# log_file="btree_ycsb_hotspot.log"
# # empty log file
# cat /dev/null > $log_file

# for read_ratio in 100 0
# do
# update_ratio=$((100-read_ratio))
# for hotspot_ratio_idx in "${!HOTSPOT_RATIOS[@]}"
# do
# hotspot_ratio=${HOTSPOT_RATIOS[$hotspot_ratio_idx]}
# RUNTIME=${RUNTIMES[$hotspot_ratio_idx]}
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$RUNTIME --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=$hotspot_ratio  --ycsb_request_hotspot_op_fraction=1 --xmerge >> $log_file 2>&1
# done
# done

