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
#include "leanstore/utils/HotspotGenerator.hpp"
#include "lsmt/rocksdb_adapter.hpp"
#include "twotree/PartitionedBTree.hpp"
#include "twotree/TrieBTree.hpp"
#include "twotree/TwoBTree.hpp"
#include "twotree/TwoLSMT.hpp"
#include "twotree/TrieLSMT.hpp"
#include "twotree/ConcurrentTwoBTree.hpp"
#include "twotree/ConcurrentPartitionedBTree.hpp"
#include "anti-caching/AntiCache.hpp"

//#include "rocksdb_adapter.hpp"
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
DEFINE_string(index_type, "BTree", "");
DEFINE_uint32(cached_btree, 0, "");
DEFINE_uint32(cached_btree_node_size_type, 0, "");
DEFINE_bool(inclusive_cache, false, "");
DEFINE_double(cached_btree_ram_ratio, 0.0, "");
DEFINE_uint32(update_or_put, 0, "");
DEFINE_uint32(cache_lazy_migration, 100, "lazy upward migration sampling rate(%)");
DEFINE_bool(ycsb_scan, false, "");
DEFINE_bool(ycsb_tx, true, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");

DEFINE_string(ycsb_request_dist, "hotspot", "");
DEFINE_double(ycsb_request_hotspot_keyspace_fraction, 0.2, "");
DEFINE_double(ycsb_request_hotspot_op_fraction, 1.0, "");

const std::string kRequestDistributionZipfian = "zipfian";
const std::string kRequestDistributionHotspot = "hotspot";

const std::string kIndexTypeBTree = "BTree";
const std::string kIndexTypeLSMT = "LSMT";

const std::string kIndexTypeAntiCache = "AntiCache";

const std::string kIndexType2BTree = "2BTree";
const std::string kIndexType2LSMT = "2LSMT";
const std::string kIndexType2LSMT_CF = "2LSMT-CF";
const std::string kIndexTypeTrieLSMT = "Trie-LSMT";
const std::string kIndexTypeTrieBTree = "Trie-BTree";


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

void keyspace_stats(utils::ScrambledZipfGenerator * zipf_gen) {
   std::vector<YCSBKey> generated_keys;
   std::unordered_map<YCSBKey, int> unique_keys;
   for (size_t i = 0; i < FLAGS_ycsb_tuple_count * 2; ++i) {
      YCSBKey key = zipf_gen->zipf_generator.rand() % FLAGS_ycsb_tuple_count;
      generated_keys.push_back(key);
      unique_keys[key]++;
   }
   sort(generated_keys.begin(), generated_keys.end());
   std::cout << "Keyspace Stats: " << std::endl;
   std::cout << "Skew factor: " << FLAGS_zipf_factor << std::endl;
   std::cout << "# total keys: " << generated_keys.size() << std::endl;
   std::cout << "# unique keys: " << unique_keys.size() << std::endl;
   std::cout << "Most frequent key occurrence: " << unique_keys[generated_keys[0]] << std::endl;
   auto print_percentile_info = [&](int p) {
      double pd = p / 100.0;
      std::cout << "p" << p  << ": " << generated_keys[pd*generated_keys.size()] << ", covering " << generated_keys[pd*generated_keys.size()] / (0.0 + FLAGS_ycsb_tuple_count) * 100 << "% of the keys, occurrence :" << unique_keys[generated_keys[pd*generated_keys.size()]] << std::endl;
   };
   for (size_t i = 5; i <= 95; i += 5) {
      print_percentile_info(i);
   }
   print_percentile_info(99);
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
   double top_tree_size_gib = 0;
   if (FLAGS_index_type == kIndexTypeBTree || FLAGS_index_type == kIndexType2BTree || FLAGS_index_type == kIndexType2LSMT_CF) {
      top_tree_size_gib = FLAGS_dram_gib * FLAGS_cached_btree_ram_ratio;
   } else if (FLAGS_index_type == kIndexTypeLSMT || 
              FLAGS_index_type == kIndexTypeAntiCache || 
              FLAGS_index_type == kIndexTypeTrieBTree || 
              FLAGS_index_type == kIndexType2LSMT || 
              FLAGS_index_type == kIndexTypeTrieLSMT) {
      top_tree_size_gib = FLAGS_dram_gib * FLAGS_cached_btree_ram_ratio;
      FLAGS_dram_gib = FLAGS_dram_gib * (1 - FLAGS_cached_btree_ram_ratio);
   } else {
      cout << "Unknown index type " << FLAGS_index_type << std::endl;
      assert(false);
      exit(1);
   }

   cout << "index type " << FLAGS_index_type << std::endl; 
   cout << "request distribution " << FLAGS_ycsb_request_dist << std::endl;
   cout << "dram_gib " << FLAGS_dram_gib << std::endl;
   cout << "top_tree_size_gib " << top_tree_size_gib << std::endl;
   cout << "wal=" << FLAGS_wal << std::endl;
   cout << "zipf_factor=" << FLAGS_zipf_factor << std::endl;
   cout << "ycsb_read_ratio=" << FLAGS_ycsb_read_ratio << std::endl;
   cout << "run_for_seconds=" << FLAGS_run_for_seconds << std::endl;
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   std::unique_ptr<leanstore::utils::Generator> random_generator;
   if (FLAGS_ycsb_request_dist == kRequestDistributionZipfian) {
      random_generator.reset(new utils::ScrambledZipfGenerator(0, ycsb_tuple_count, FLAGS_zipf_factor));
   } else {
      cout << "hotspot_keyspace_fraction=" << FLAGS_ycsb_request_hotspot_keyspace_fraction << std::endl;
      cout << "hotspot_op_fraction=" << FLAGS_ycsb_request_hotspot_op_fraction << std::endl;
      random_generator.reset(new utils::HotspotGenerator(0, ycsb_tuple_count, FLAGS_ycsb_request_hotspot_keyspace_fraction, FLAGS_ycsb_request_hotspot_op_fraction));
   }
   LeanStore db;
   unique_ptr<BTreeInterface<YCSBKey, YCSBPayload>> adapter;
   leanstore::storage::btree::BTreeLL* btree_ptr = nullptr;
   leanstore::storage::btree::BTreeLL* btree2_ptr = nullptr;
   db.getCRManager().scheduleJobSync(0, [&](){
      if (FLAGS_recover) {
         btree_ptr = &db.retrieveBTreeLL("ycsb");
         btree2_ptr = &db.retrieveBTreeLL("ycsb_cold");
      } else {
         btree_ptr = &db.registerBTreeLL("ycsb");
         btree2_ptr = &db.registerBTreeLL("ycsb_cold");
      }
   });
   
   if (FLAGS_index_type == kIndexTypeBTree) {
      adapter.reset(new BTreeVSAdapter<YCSBKey, YCSBPayload>(*btree_ptr, btree_ptr->dt_id));
   } else if (FLAGS_index_type == kIndexType2BTree) {
      adapter.reset(new TwoBTreeAdapter<YCSBKey, YCSBPayload>(*btree_ptr, *btree2_ptr, top_tree_size_gib, FLAGS_inclusive_cache, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexTypeTrieBTree) {
      adapter.reset(new BTreeTrieCachedVSAdapter<YCSBKey, YCSBPayload>(*btree_ptr, top_tree_size_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexTypeLSMT) {
      adapter.reset(new RocksDBAdapter<YCSBKey, YCSBPayload>("/mnt/disks/nvme/rocksdb", top_tree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexType2LSMT) {
      adapter.reset(new TwoRocksDBAdapter<YCSBKey, YCSBPayload>("/mnt/disks/nvme/rocksdb", top_tree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexType2LSMT_CF){
      adapter.reset(new RocksDBTwoCFAdapter<YCSBKey, YCSBPayload>("/mnt/disks/nvme/rocksdb", top_tree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexTypeTrieLSMT) {
      adapter.reset(new TrieRocksDBAdapter<YCSBKey, YCSBPayload>("/mnt/disks/nvme/rocksdb", top_tree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else { // 
      assert(FLAGS_index_type == kIndexTypeAntiCache);
      adapter.reset(new AntiCacheAdapter<YCSBKey, YCSBPayload>("/mnt/disks/nvme/rocksdb", top_tree_size_gib, FLAGS_dram_gib));
   }

   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   db.registerConfigEntry("ycsb_target_gib", FLAGS_target_gib);
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   auto& table = *adapter;
   // Insert values
   const u64 n = ycsb_tuple_count;
   vector<u64> keys(n);
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
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Inserting values" << endl;
      
      std::iota(keys.begin(), keys.end(), 0);
      std::random_shuffle(keys.begin(), keys.end());
      begin = chrono::high_resolution_clock::now();
      {
         if (FLAGS_index_type == kIndexType2LSMT || FLAGS_index_type == kIndexTypeLSMT || FLAGS_index_type == kIndexType2LSMT_CF || FLAGS_index_type == kIndexTypeLSMT) {
            tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
               for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
                  YCSBPayload payload;
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  YCSBKey key = keys[t_i];
                  table.insert(key, payload);
               }
            });
         } else {
            tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
               vector<u64> range_keys(range.end() - range.begin());
               std::iota(range_keys.begin(), range_keys.end(), range.begin());
               std::random_shuffle(range_keys.begin(), range_keys.end());
               for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
                  YCSBPayload payload;
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  YCSBKey key = range_keys[t_i - range.begin()];
                  table.insert(key, payload);
               }
            });
         }
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
   // -------------------------------------------------------------------------------------
   
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   // Scan
   if (FLAGS_ycsb_scan) {
      const u64 n = ycsb_tuple_count;
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Scan" << endl;
      {
         begin = chrono::high_resolution_clock::now();
         tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
            for (u64 i = range.begin(); i < range.end(); i++) {
               YCSBPayload result;
               table.lookup(i, result);
            }
         });
         end = chrono::high_resolution_clock::now();
      }
      // -------------------------------------------------------------------------------------
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      // -------------------------------------------------------------------------------------
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   // -------------------------------------------------------------------------------------
   cout << "-------------------------------------------------------------------------------------" << endl;
   cout << "~Transactions" << endl;

   
   // if (FLAGS_index_type == kIndexType2LSMT || FLAGS_index_type == kIndexTypeLSMT || FLAGS_index_type == kIndexType2LSMT_CF || FLAGS_index_type == kIndexTypeLSMT)
   {
      cout << "Warming up" << endl;
      auto t_start = std::chrono::high_resolution_clock::now();
      // warm up for 100 seconds
      // scramble the database
      while (std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now()- t_start).count() < 200000) {
         YCSBKey key = keys[random_generator->rand() % ycsb_tuple_count];

         assert(key < ycsb_tuple_count);
         YCSBPayload result;
         if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
            table.lookup(key, result);
         } else {
            YCSBPayload payload;
            utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
            if (FLAGS_update_or_put == 0) {
               table.update(key, payload);
            } else {
               table.put(key, payload);
            }
         }
      }
      cout << "Warmed up" << endl;
   }

   //adapter->evict_all();

   cout << "All evicted" << endl;

   adapter->report(FLAGS_ycsb_tuple_count, db.getBufferManager().consumedPages());
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   atomic<u64> txs = 0;
   vector<thread> threads;
   begin = chrono::high_resolution_clock::now();
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      db.getCRManager().scheduleJobAsync(t_i, [&]() {
         adapter->clear_stats();
         running_threads_counter++;
         u64 tx = 0;
         
         while (keep_running) {
            YCSBKey key = keys[random_generator->rand() % ycsb_tuple_count];
            
            assert(key < ycsb_tuple_count);
            YCSBPayload result;
            if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
               table.lookup(key, result);
            } else {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               if (FLAGS_update_or_put == 0) {
                  table.update(key, payload);
               } else {
                  table.put(key, payload);
               }
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
      if (FLAGS_ycsb_request_dist == kRequestDistributionZipfian) {
         keyspace_stats((utils::ScrambledZipfGenerator*)random_generator.get());
      }
      
      adapter->report(FLAGS_ycsb_tuple_count, db.getBufferManager().consumedPages());
      cout << "-------------------------------------------------------------------------------------" << endl;
      db.getCRManager().joinAll();
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   // -------------------------------------------------------------------------------------
   return 0;
}
