cache_dram_ratio=0.9
EXEC=/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf
RUNTIME=600
TUPLE_COUNT=20000000
SLEEP_COUNT=100
# for dram_budget in 0.3 0.6 0.9 1.2
# do
# run_time=300
# log_file="anticache_ycsb_zipf_dram_${dram_budget}gib.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 100 0
# do
# for zipf_factor in 0.7 0.8 0.9
# do
# build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=AntiCache  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
# done
# done
# done

# for dram_budget in 1
# do
# run_time=$RUNTIME
# log_file="anticacheb_ycsb_zipf_dram_${dram_budget}gib.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 100 0
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.7 0.8 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=AntiCacheB  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
# done


for dram_budget in 1
do
run_time=$RUNTIME
log_file="anticacheb_ycsb_zipf_dram_${dram_budget}gib_scan.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=AntiCacheB --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_keyspace_count=10000000 --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

