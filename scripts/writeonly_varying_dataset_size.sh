for inclusivity in 1 0
do
for update_or_put in 1
do
for cached_btree_type in 4 1 3
do
log_file="writeonly_varying_dataset_size_cache_type_${cached_btree_type}_put_${update_or_put}_inclusivity_${inclusivity}.log"
cat /dev/null > $log_file
for tuple_count in 10000000 20000000 30000000 40000000
do
build/frontend/ycsb --trunc=1 --ycsb_tuple_count=$tuple_count --dram_gib=0.7 --worker_threads=1 --cached_btree=$cached_btree_type --ycsb_read_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --cached_btree_ram_ratio=0.95 --run_for_seconds=300 --inclusive_cache=$inclusivity --update_or_put=$update_or_put >> $log_file 2>&1
done
done
done
done