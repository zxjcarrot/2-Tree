# for read_ratio in 100
# do
# log_file="leanstore_ycsb_in_memory_read_ratio_${read_ratio}.log"
# # empty log file
# cat /dev/null > $log_file
# for access_ratio in 0.001 0.01 0.05 0.1 0.15 0.2 0.3 0.4 0.5
# do
# build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=7 --worker_threads=1 --cached_btree=0 --ycsb_keyspace_access_ratio=$access_ratio  --cached_btree_ram_ratio=0.95 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=240 --xmerge >> $log_file 2>&1
# done
# done

# for read_ratio in 100
# do
# log_file="two_leanstore_ycsb_in_memory_read_ratio_${read_ratio}.log"
# # empty log file
# cat /dev/null > $log_file
# for access_ratio in 0.001 0.01 0.05 0.1 0.15 0.2 0.3 0.4 0.5
# do
# build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=25000000 --dram_gib=7 --worker_threads=1 --cached_btree=12 --ycsb_keyspace_access_ratio=$access_ratio  --cached_btree_ram_ratio=0.95 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=240 --xmerge >> $log_file 2>&1
# done
# done



for read_ratio in 100 0
do
log_file="rocksdb_ycsb_in_memory_read_ratio_${read_ratio}.log"
# empty log file
cat /dev/null > $log_file
for access_ratio in 0.001 0.01 0.05 0.1 0.15 0.2 0.3 0.4 0.5
do
build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=25000000 --dram_gib=5 --worker_threads=1 --cached_btree=4 --ycsb_keyspace_access_ratio=$access_ratio  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --xmerge  >> $log_file 2>&1
done
done

for read_ratio in 100 0
do
log_file="two_rocksdb_ycsb_in_memory_read_ratio_${read_ratio}.log"
# empty log file
cat /dev/null > $log_file
for access_ratio in 0.001 0.01 0.05 0.1 0.15 0.2 0.3 0.4 0.5
do
build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=25000000 --dram_gib=5 --worker_threads=1 --cached_btree=10 --ycsb_keyspace_access_ratio=$access_ratio  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --xmerge >> $log_file 2>&1
done
done