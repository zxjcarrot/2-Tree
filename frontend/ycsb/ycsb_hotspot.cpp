#include "Units.hpp"
#include "interface/StorageInterface.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
#include "rocksdb_adapter.hpp"
#include "anti-caching/AntiCache.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_tx_rounds, 1, "");
DEFINE_uint32(ycsb_tx_count, 0, "default = tuples");
DEFINE_bool(verify, false, "");
DEFINE_uint32(cached_btree, 0, "");
DEFINE_uint32(cached_btree_node_size_type, 0, "");
DEFINE_double(cached_btree_ram_ratio, 0.0, "");
DEFINE_bool(cache_lazy_migration, false, "");
DEFINE_bool(ycsb_scan, false, "");
DEFINE_bool(ycsb_tx, true, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
DEFINE_double(ycsb_keyspace_access_ratio, 0.01, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<120>;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}

void zipf_stats(utils::ScrambledZipfGenerator * zipf_gen) {
   std::vector<YCSBKey> generated_keys;
   std::unordered_set<YCSBKey> unique_keys;
   for (size_t i = 0; i < FLAGS_ycsb_tuple_count; ++i) {
      YCSBKey key = zipf_gen->zipf_generator.rand() % FLAGS_ycsb_tuple_count;
      generated_keys.push_back(key);
      unique_keys.insert(key);
   }
   sort(generated_keys.begin(), generated_keys.end());
   std::cout << "Zipfian Stats: " << std::endl;
   std::cout << "Skew factor: " << FLAGS_zipf_factor << std::endl;
   std::cout << "# unique keys: " << unique_keys.size() << std::endl;
   std::cout << "p50: " << generated_keys[0.5*generated_keys.size()] << ", covering " << generated_keys[0.5*generated_keys.size()] / (0.0 + FLAGS_ycsb_tuple_count) * 100 << "% of the keys"  << std::endl;
   std::cout << "p75: " << generated_keys[0.75*generated_keys.size()] << ", covering " << generated_keys[0.75*generated_keys.size()] / (0.0 + FLAGS_ycsb_tuple_count) * 100 << "% of the keys"  << std::endl;
   std::cout << "p90: " << generated_keys[0.90*generated_keys.size()] << ", covering " << generated_keys[0.90*generated_keys.size()] / (0.0 + FLAGS_ycsb_tuple_count) * 100 << "% of the keys"  << std::endl;
   std::cout << "p99: " << generated_keys[0.99*generated_keys.size()] << ", covering " << generated_keys[0.99*generated_keys.size()] / (0.0 + FLAGS_ycsb_tuple_count) * 100 << "% of the keys"  << std::endl;
}

// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
   // -------------------------------------------------------------------------------------
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   double cached_btree_size_gib = 0;
   if (FLAGS_cached_btree == 3) {
      cached_btree_size_gib = FLAGS_dram_gib * FLAGS_cached_btree_ram_ratio;
   } else if (FLAGS_cached_btree == 1 || FLAGS_cached_btree == 2 || FLAGS_cached_btree == 5 || FLAGS_cached_btree == 7){
      cached_btree_size_gib = FLAGS_dram_gib * FLAGS_cached_btree_ram_ratio;
      FLAGS_dram_gib = FLAGS_dram_gib * (1 - FLAGS_cached_btree_ram_ratio);
   } else if (FLAGS_cached_btree == 4 || FLAGS_cached_btree == 6) { // rocksdb with row cache
      cached_btree_size_gib = FLAGS_dram_gib * FLAGS_cached_btree_ram_ratio; // row cache size
      FLAGS_dram_gib = FLAGS_dram_gib * (1 - FLAGS_cached_btree_ram_ratio); // block cache size
   }
   cout << "cached_btree_size_gib " << cached_btree_size_gib << std::endl;
   cout << "wal=" << FLAGS_wal << std::endl;
   cout << "zipf_factor=" << FLAGS_zipf_factor << std::endl;
   cout << "ycsb_read_ratio=" << FLAGS_ycsb_read_ratio << std::endl;
   cout << "run_for_seconds=" << FLAGS_run_for_seconds << std::endl;
   cout << "cache_btree_node_size_type=" << FLAGS_cached_btree_node_size_type << std::endl;
   LeanStore db;
   unique_ptr<BTreeInterface<YCSBKey, YCSBPayload>> adapter;
   leanstore::storage::btree::BTreeLL* btree_ptr = nullptr;
   if (FLAGS_recover) {
      btree_ptr = &db.retrieveBTreeLL("ycsb");
   } else {
      btree_ptr = &db.registerBTreeLL("ycsb");
   }
   if (FLAGS_cached_btree == 0) {
      adapter.reset(new BTreeVSAdapter<YCSBKey, YCSBPayload>(*btree_ptr, btree_ptr->dt_id));
   } else if (FLAGS_cached_btree == 1) {
      if (FLAGS_cached_btree_node_size_type == 0) {
         adapter.reset(new BTreeCachedVSAdapter<YCSBKey, YCSBPayload, 1024>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
      } else if (FLAGS_cached_btree_node_size_type == 1) {
         adapter.reset(new BTreeCachedVSAdapter<YCSBKey, YCSBPayload, 2048>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
      } else if (FLAGS_cached_btree_node_size_type == 2) {
         adapter.reset(new BTreeCachedVSAdapter<YCSBKey, YCSBPayload, 4096>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
      } else if (FLAGS_cached_btree_node_size_type == 3) {
         adapter.reset(new BTreeCachedVSAdapter<YCSBKey, YCSBPayload, 8192>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
      } else if (FLAGS_cached_btree_node_size_type == 4) {
         adapter.reset(new BTreeCachedVSAdapter<YCSBKey, YCSBPayload, 16384>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
      }
   } else if (FLAGS_cached_btree == 2) {
      adapter.reset(new BTreeCachedNoninlineVSAdapter<YCSBKey, YCSBPayload>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
   } else if (FLAGS_cached_btree == 3) {
      if (FLAGS_cached_btree) {
         cached_btree_size_gib = FLAGS_dram_gib * FLAGS_cached_btree_ram_ratio;
         FLAGS_dram_gib = FLAGS_dram_gib * (1 - FLAGS_cached_btree_ram_ratio);
      }
      adapter.reset(new BTreeVSHotColdPartitionedAdapter<YCSBKey, YCSBPayload>(*btree_ptr, btree_ptr->dt_id, cached_btree_size_gib));
   } else if (FLAGS_cached_btree == 4) {
      adapter.reset(new RocksDBAdapter<YCSBKey, YCSBPayload>("/mnt/disks/nvme/rocksdb", cached_btree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration));
   } else if (FLAGS_cached_btree == 5) {
      adapter.reset(new BTreeTrieCachedVSAdapter<YCSBKey, YCSBPayload>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
   } else if (FLAGS_cached_btree == 6) {
      adapter.reset(new AntiCacheAdapter<YCSBKey, YCSBPayload>("/mnt/disks/nvme/rocksdb", cached_btree_size_gib, FLAGS_dram_gib));
   } else if (FLAGS_cached_btree == 7) {
      adapter.reset(new BTreeCachedCompressedVSAdapter<YCSBKey, YCSBPayload, 4096>(*btree_ptr, cached_btree_size_gib, FLAGS_cache_lazy_migration));
   }

   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   db.registerConfigEntry("ycsb_target_gib", FLAGS_target_gib);
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   auto& table = *adapter;
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   // Insert values
   if (FLAGS_recover) {
      // Warmup
      const u64 n = ycsb_tuple_count;
      cout << "Warmup: Scanning..." << endl;
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
         for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
            YCSBPayload result;
            table.lookup(t_i, result);
         }
      });
      // -------------------------------------------------------------------------------------
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   } else {
      const u64 n = ycsb_tuple_count;
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Inserting values" << endl;
      begin = chrono::high_resolution_clock::now();
      {
         tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
            vector<u64> keys(range.size());
            std::iota(keys.begin(), keys.end(), range.begin());
            std::random_shuffle(keys.begin(), keys.end());
            for (u64 t_i = 0; t_i < keys.size(); t_i++) {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               auto& key = keys[t_i];
               table.insert(key, payload);
            }
         });
      }
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      // -------------------------------------------------------------------------------------
      const u64 written_pages = db.getBufferManager().consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   
   auto optimal_entries_per_leaf = leanstore::storage::PAGE_SIZE / (sizeof(YCSBPayload) + sizeof(YCSBKey));
   auto minimal_num_pages = FLAGS_ycsb_tuple_count * (sizeof(YCSBPayload) + sizeof(YCSBKey)) / leanstore::storage::PAGE_SIZE;
   auto fill_factor = minimal_num_pages / (db.getBufferManager().consumedPages() + 0.0);
   auto entries_per_leaf = fill_factor * optimal_entries_per_leaf;
   auto num_keys_to_access = std::max(1, (int)(FLAGS_ycsb_tuple_count * FLAGS_ycsb_keyspace_access_ratio));
   vector<YCSBKey> all_query_keys;
   for (YCSBKey i = 0; i < FLAGS_ycsb_tuple_count; i += 1) {
      all_query_keys.push_back(i);
   }
   std::random_shuffle(all_query_keys.begin(), all_query_keys.end());
   vector<YCSBKey> query_keys(all_query_keys.begin(), all_query_keys.begin() + num_keys_to_access);
   // -------------------------------------------------------------------------------------
   auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   cout << "-------------------------------------------------------------------------------------" << endl;
   cout << "~Transactions" << endl;
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   atomic<u64> txs = 0;
   adapter->evict_all();

   cout << "All evicted" << endl;
   begin = chrono::high_resolution_clock::now();
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      db.getCRManager().scheduleJobAsync(t_i, [&]() {
         adapter->clear_stats();
         running_threads_counter++;
         u64 tx = 0;
         while (keep_running) {
            u64 key_idx = utils::RandomGenerator::getRandU64() % query_keys.size();
            YCSBKey key = query_keys[key_idx];
            assert(key < ycsb_tuple_count);
            YCSBPayload result;
            if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
               table.lookup(key, result);
            } else {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               table.update(key, payload);
            }
            WorkerCounters::myCounters().tx++;
            ++tx;
         }
         txs += tx;
         running_threads_counter--;
      });
   }
   {
      // Shutdown threads
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         MYPAUSE();
      }
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      // -------------------------------------------------------------------------------------
      cout << "Total commit: " << calculateMTPS(begin, end, txs.load()) << " M tps" << endl;
      cout << "entries_per_leaf: " << entries_per_leaf << std::endl;
      cout << "query_keys size " << query_keys.size() << std::endl;
      cout << "keyspace_access_ratio: " << FLAGS_ycsb_keyspace_access_ratio << std::endl;
      zipf_stats(zipf_random.get());
      adapter->report(FLAGS_ycsb_tuple_count, db.getBufferManager().consumedPages());
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   // -------------------------------------------------------------------------------------
   return 0;
}
