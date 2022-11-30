cache_dram_ratio=0.9
EXEC=/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf
RUNTIME=1200
TUPLE_COUNT=20000000
SLEEP_COUNT=100

for dram_budget in 1
do
run_time=$RUNTIME
log_file="2lsmt_ycsb_zipf_dram_${dram_budget}gib_scan_inclusive_lazy.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=2LSMT-CF  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

for dram_budget in 1 #0.3 #1.5 #0.6 0.9 1.2 #2 
do
run_time=$RUNTIME
log_file="upmigration_lsmt_ycsb_zipf_dram_${dram_budget}gib_lazy.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=UpLSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=20000000 --dram_gib=1 --worker_threads=1 --index_type=UpLSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=0 --ycsb_update_ratio=100 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=12000 --ycsb_request_dist=zipfian --zipf_factor=0.9 --inclusive_cache --cache_lazy_migration=50 --xmerge

for dram_budget in 1
do
run_time=$RUNTIME
log_file="2lsmt_ycsb_zipf_dram_${dram_budget}gib_inclusive_lazy.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=2LSMT-CF  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done


# /mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.25 --worker_threads=1 --index_type=BTree  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --ycsb_request_dist=zipfian --zipf_factor=0.7 --inclusive_cache --cache_lazy_migration=50 --xmerge 


# /mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=20000000 --dram_gib=1 --worker_threads=1 --index_type=2BTree  --cached_btree_ram_ratio=0.85 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --ycsb_request_dist=zipfian --zipf_factor=0.7 --inclusive_cache --cache_lazy_migration=50 --xmerge 



# /mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=20000000 --dram_gib=1 --worker_threads=1 --index_type=UpLSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --ycsb_request_dist=zipfian --zipf_factor=0.7 --inclusive_cache --cache_lazy_migration=50 --xmerge 

# /mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=20000000 --dram_gib=1 --worker_threads=1 --index_type=UpLSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=1200 --ycsb_request_dist=zipfian --zipf_factor=0.9 --inclusive_cache --cache_lazy_migration=50 --xmerge 



# for dram_budget in 1 
# do
# run_time=$RUNTIME
# log_file="2lsmt_ycsb_zipf_dram_${dram_budget}gib_lazy.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 100 0
# do
# update_ratio=$((100-read_ratio))
# for zipf_factor in 0.7 0.8 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=2LSMT-CF  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
# sleep $SLEEP_COUNT
# done
# done
# done    


for dram_budget in 1
do
run_time=$RUNTIME
log_file="upmigration_lsmt_ycsb_zipf_dram_${dram_budget}_scan_lazy.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=UpLSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done

for dram_budget in 1
do
run_time=$RUNTIME
log_file="2btree_ycsb_zipf_dram_${dram_budget}_scan_inclusive_lazy.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=2BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done




for dram_budget in 1
do
run_time=$RUNTIME
log_file="im2btree_ycsb_zipf_dram_${dram_budget}gib_scan_inclusive_lazy.log"
# empty log file
cat /dev/null > $log_file
for scan_ratio in 25 50 75 100
do
read_ratio=$((100-scan_ratio))
for zipf_factor in 0.8
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=IM2BTree --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_scan_ratio=$scan_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --ycsb_keyspace_count=10000000 --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done


# for dram_budget in 0.6 0.9 1.2
# do
# run_time=300
# log_file="trie_lsmt_ycsb_zipf_dram_${dram_budget}gib_inclusive_lazy.log"
# # empty log file
# cat /dev/null > $log_file
# for read_ratio in 100 0
# do
# for zipf_factor in 0.7 0.8 0.9
# do
# $EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=Trie-LSMT  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
# done
# done
# done


for dram_budget in 1
do
run_time=$RUNTIME
log_file="im2btree_ycsb_zipf_dram_${dram_budget}gib_inclusive_lazy.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=IM2BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done


for dram_budget in 1
do
run_time=$RUNTIME
log_file="2btree_ycsb_zipf_dram_${dram_budget}gib_inclusive_lazy.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=2BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done


/mnt/disks/nvme/leanstore-src/build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=20000000 --dram_gib=1 --worker_threads=1 --index_type=2BTree  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=100 --ycsb_update_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=1200 --ycsb_request_dist=zipfian --zipf_factor=0.9 --inclusive_cache --cache_lazy_migration=50 --xmerge

for dram_budget in 1
do
run_time=$RUNTIME
log_file="trie_btree_ycsb_zipf_dram_${dram_budget}gib_inclusive_lazy.log"
# empty log file
cat /dev/null > $log_file
for read_ratio in 100 0
do
update_ratio=$((100-read_ratio))
for zipf_factor in 0.7 0.8 0.9
do
$EXEC --trunc=1 --ycsb_tuple_count=$TUPLE_COUNT --dram_gib=$dram_budget --worker_threads=1 --index_type=Trie-BTree  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=$read_ratio --ycsb_update_ratio=$update_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --inclusive_cache --cache_lazy_migration=50 --xmerge >> $log_file 2>&1
sleep $SLEEP_COUNT
done
done
done
