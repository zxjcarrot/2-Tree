cache_dram_ratio=0.9
EXEC=/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf
RUNTIME=600
SLEEP_COUNT=100
TUPLE_COUNT=20000000

for dram_budget in 1
do
run_time=$RUNTIME
log_file="btree_ycsb_zipf_dram_${dram_budget}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

for dram_budget in 1
do
run_time=$RUNTIME
log_file="lsmt_ycsb_zipf_dram_${dram_budget}_record_cache.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --lsmt_use_record_cache=true --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

# for dram_budget in 0.3
# do
# run_time=$RUNTIME
# log_file="lsmt_ycsb_zipf_dram_${dram_budget}_record_cache_update.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 0
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.7 #0.8 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --lsmt_use_record_cache=true --index_type=LSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio  --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
# done
# done
# done

# for dram_budget in 0.3
# do
# run_time=$RUNTIME
# log_file="lsmt_ycsb_zipf_dram_${dram_budget}_update.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 0
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.7 # 0.8 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio  --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
# done
# done
# done


# for dram_budget in 0.3
# do
# run_time=$RUNTIME
# log_file="lsmt_ycsb_zipf_dram_${dram_budget}_record_cache_lookup_blind_write.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 25 50 75
# do
# blind_write_ratio=$((100-read_ratio))
# for zipf_factor in 0.7 0.8 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --lsmt_use_record_cache=true --index_type=LSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio  --ycsb_blind_write_ratio=$blind_write_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
# done
# done
# done

# for dram_budget in 0.3
# do
# run_time=$RUNTIME
# log_file="lsmt_ycsb_zipf_dram_${dram_budget}_lookup_blind_write.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 25 50 75
# do
# blind_write_ratio=$((100-read_ratio))
# for zipf_factor in 0.7 0.8 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio  --ycsb_blind_write_ratio=$blind_write_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
# done
# done
# done



for dram_budget in 1
do
run_time=$RUNTIME
log_file="lsmt_ycsb_zipf_dram_${dram_budget}.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

for dram_budget in 1
do
run_time=$RUNTIME
log_file="btree_ycsb_zipf_dram_${dram_budget}_scan.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

for dram_budget in 1
do
run_time=$RUNTIME
log_file="lsmt_ycsb_zipf_dram_${dram_budget}_scan.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

for dram_budget in 1
do
run_time=$RUNTIME
log_file="lsmt_ycsb_zipf_dram_${dram_budget}_scan_record_cache.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do 
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --lsmt_use_record_cache=true --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
