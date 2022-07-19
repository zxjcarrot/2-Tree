# for read_ratio in 100 80
# do
# log_file="rocksdb_ycsb_hotspot_read_ratio_${read_ratio}_inclusive.log"
# # empty log file
# cat /dev/null > $log_file
# for access_ratio in 0.001 0.01 0.05 0.1 0.15 0.2 0.25 0.3
# do
# build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.3 --worker_threads=1 --cached_btree=4 --ycsb_keyspace_access_ratio=$access_ratio  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --inclusive_cache=true >> $log_file 2>&1
# done
# done

for read_ratio in 100 80
do
log_file="two_rocksdb_ycsb_hotspot_read_ratio_${read_ratio}_inclusive.log"
# empty log file
cat /dev/null > $log_file
for access_ratio in 0.001 0.01 0.05 0.1 0.15 0.2 0.25 0.3
do
build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.3 --worker_threads=1 --cached_btree=10 --ycsb_keyspace_access_ratio=$access_ratio  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600  --inclusive_cache=true >> $log_file 2>&1
done
done


for read_ratio in 100 80
do
log_file="trie_rocksdb_ycsb_hotspot_read_ratio_${read_ratio}_inclusive.log"
# empty log file
cat /dev/null > $log_file
for access_ratio in 0.001 0.01 0.05 0.1 0.15 0.2 0.25 0.3
do
build/frontend/ycsb_hotspot --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.3 --worker_threads=1 --cached_btree=11 --ycsb_keyspace_access_ratio=$access_ratio  --cached_btree_ram_ratio=0.9 --ycsb_read_ratio=$read_ratio --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=600 --inclusive_cache=true >> $log_file 2>&1
done
done


