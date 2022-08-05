cache_dram_ratio=0.7

run_time=300
for dram_budget in 2 0.3
do
log_file="lsmt_ycsb_put_only_zipf_dram_${dram_budget}gib.log"
# empty log file
cat /dev/null > $log_file
for zipf_factor in 0.7 0.8 0.9
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=$dram_budget --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --zipf_factor=$zipf_factor --xmerge --update_or_put=1 >> $log_file 2>&1
done
done

for dram_budget in 2 0.3
do
log_file="lsmt_ycsb_put_only_hotspot_dram_${dram_budget}gib.log"
# empty log file
cat /dev/null > $log_file
for hotspot_key_fraction in 0.1 0.05 0.01
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=$dram_budget --worker_threads=1 --index_type=LSMT  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=$hotspot_key_fraction --ycsb_request_hotspot_op_fraction=0.9 --xmerge --update_or_put=1 >> $log_file 2>&1
done
done

for dram_budget in 2 0.3
do
log_file="2lsmt_ycsb_put_only_zipf_dram_${dram_budget}gib.log"
# empty log file
cat /dev/null > $log_file
for zipf_factor in 0.7 0.8 0.9
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=$dram_budget --worker_threads=1 --index_type=2LSMT-CF  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=zipfian --cache_lazy_migration=50 --inclusive_cache --zipf_factor=$zipf_factor --xmerge --update_or_put=1 >> $log_file 2>&1
done
done

for dram_budget in 2 0.3
do
log_file="2lsmt_ycsb_put_only_hotspot_dram_${dram_budget}gib.log"
# empty log file
cat /dev/null > $log_file
for hotspot_key_fraction in 0.1 0.05 0.01
do
build/frontend/ycsb_zipf --trunc=1 --ycsb_tuple_count=10000000 --dram_gib=$dram_budget --worker_threads=1 --index_type=2LSMT-CF  --cached_btree_ram_ratio=$cache_dram_ratio --ycsb_read_ratio=0 --ssd_path=/mnt/disks/nvme/leanstore --run_for_seconds=$run_time --ycsb_request_dist=hotspot --ycsb_request_hotspot_keyspace_fraction=$hotspot_key_fraction --ycsb_request_hotspot_op_fraction=0.9 --xmerge --update_or_put=1 >> $log_file 2>&1
done
done
