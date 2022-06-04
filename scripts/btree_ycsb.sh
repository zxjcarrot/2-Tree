# for update_or_put in 0 1
# do
# for inclusive_cache in 0 1
# do
# log_file="partitioned_btree_ycsb_inclusive_${inclusive_cache}_put_${update_or_put}.log"
# cat /dev/null > $log_file
# for rw_ratio in 0 20 40 60 80 100
# do
# build/frontend/ycsb --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.7 --worker_threads=1 --cached_btree=3 --ycsb_read_ratio=$rw_ratio --ssd_path=/mnt/disks/nvme/leanstore --cached_btree_ram_ratio=0.95 --run_for_seconds=300 --inclusive_cache=$inclusive_cache --update_or_put=$update_or_put >> $log_file 2>&1
# done
# done
# done

log_file="btree_ycsb.log"
cat /dev/null > $log_file
for rw_ratio in 0 20 40 60 80 100
do
build/frontend/ycsb --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=0.7 --worker_threads=1 --cached_btree=0 --ycsb_read_ratio=$rw_ratio --ssd_path=/mnt/disks/nvme/leanstore --cached_btree_ram_ratio=0.95 --run_for_seconds=300 >> $log_file 2>&1
done

