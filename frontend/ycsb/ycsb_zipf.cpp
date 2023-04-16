#include "Units.hpp"
#include "RecordSize.h"
#include "interface/StorageInterface.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/storage/hashing/LinearHashing.hpp"
#include "leanstore/storage/hashing/LinearHashingWithOverflowHeap.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
#include "leanstore/utils/HotspotGenerator.hpp"
#include "leanstore/utils/HotspotZipfGenerator.hpp"
#include "leanstore/utils/SelfSimilarGenerator.hpp"
#include "lsmt/rocksdb_adapter.hpp"
#include "lsmt/upmigration_rocksdb_adapter.hpp"
#include "twotree/PartitionedBTree.hpp"
#include "twotree/TrieBTree.hpp"
#include "twotree/TwoBTree.hpp"
#include "twotree/TwoLSMT.hpp"
#include "twotree/TrieLSMT.hpp"
#include "twotree/ConcurrentTwoBTree.hpp"
#include "twotree/ConcurrentPartitionedBTree.hpp"
#include "twohash/TwoHash.hpp"
#include "anti-caching/AntiCache.hpp"
#include "anti-caching/AntiCacheBTree.hpp"
#include "hash/HashAdapter.hpp"
#include "heap/HeapFileAdapter.hpp"
#include "heap/IndexedHeapAdapter.hpp"
#include "twoheap/TwoIHeap.hpp"
//#include "rocksdb_adapter.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <shared_mutex>
#include <set>
#include <ctime>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint32(ycsb_update_ratio, 0, "");
DEFINE_uint32(ycsb_blind_write_ratio, 0, "");
DEFINE_uint32(ycsb_scan_ratio, 0, "");
DEFINE_uint32(ycsb_delete_ratio, 0, "");
DEFINE_uint32(ycsb_insert_ratio, 0, "");
DEFINE_uint32(ycsb_scan_length, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint64(ycsb_keyspace_count, 500000000, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_tx_rounds, 1, "");
DEFINE_uint32(ycsb_tx_count, 0, "default = tuples");
DEFINE_bool(verify, false, "");
DEFINE_string(index_type, "BTree", "");
DEFINE_bool(lsmt_use_record_cache, false, "use record cache?");
DEFINE_string(lsmt_db_path, "./", "directory for storing lsm-tree files");
DEFINE_uint32(cached_btree, 0, "");
DEFINE_uint32(cached_btree_node_size_type, 0, "");
DEFINE_bool(inclusive_cache, false, "");
DEFINE_uint32(update_or_put, 0, "");
DEFINE_uint32(cache_lazy_migration, 100, "lazy upward migration sampling rate(%)");
DEFINE_bool(ycsb_scan, false, "");
DEFINE_bool(ycsb_tx, true, "");
DEFINE_bool(ycsb_2heap_eviction_index_scan, true, "");
DEFINE_uint64(ycsb_2hash_eviction_record_count, 1, "");
DEFINE_bool(ycsb_2hash_eviction_by_record, false, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
DEFINE_bool(ycsb_2hash_use_different_hash, false, "whether to use different hash functions for two hash tables");

DEFINE_string(ycsb_request_dist, "hotspot", "");
DEFINE_double(ycsb_request_hotspot_keyspace_fraction, 0.2, "");
DEFINE_double(ycsb_request_hotspot_op_fraction, 1.0, "");
DEFINE_double(ycsb_request_selfsimilar_skew, 0.2, "");
DEFINE_bool(ycsb_correctness_check, false, "correctness check");

const std::string kRequestDistributionZipfian = "zipfian";
const std::string kRequestDistributionSelfSimilar = "selfsimilar";
const std::string kRequestDistributionHotspot = "hotspot";
const std::string kRequestDistributionHotspotZipfian = "hotspot_zipfian";

const std::string kIndexTypeBTree = "BTree";
const std::string kIndexTypeHash = "Hash";
const std::string kIndexTypeHashWOH = "HashWOH";
const std::string kIndexTypeLSMT = "LSMT";
const std::string kIndexTypeHeap = "Heap";
const std::string kIndexTypeIHeap = "IHeap";

const std::string kIndexTypeAntiCache = "AntiCache";
const std::string kIndexTypeAntiCacheB = "AntiCacheB";
const std::string kIndexType2BTree = "2BTree";
const std::string kIndexType2Hash = "2Hash";
const std::string kIndexType2IHeap = "2IHeap";
const std::string kIndexTypeC2BTree = "C2BTree";
const std::string kIndexType2LSMT = "2LSMT";
const std::string kIndexType2LSMT_CF = "2LSMT-CF";
const std::string kIndexTypeUpLSMT = "UpLSMT";
const std::string kIndexTypeTrieLSMT = "Trie-LSMT";
const std::string kIndexTypeTrieBTree = "Trie-BTree";
const std::string kIndexTypeIM2BTree = "IM2BTree";
const std::string kIndexTypeSTX2BTree = "STX2BTree";
const std::string kIndexTypeSTXBTree = "STXBTree";


// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<RECORD_SIZE>;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}

std::string getCurrentDateTime() {
   std::time_t now = std::time(nullptr);
    
    // convert to local time
    std::tm *local_time = std::localtime(&now);
    
    // format the time string
    char time_str[20];
    std::strftime(time_str, sizeof(time_str), "%Y/%m/%d %H:%M:%S", local_time);
    
    return time_str;
}

void zipf_keyspace_stats(utils::ScrambledZipfGenerator * zipf_gen) {
   std::vector<YCSBKey> generated_keys;
   //std::unordered_map<YCSBKey, int> unique_keys;
   size_t n = FLAGS_ycsb_tuple_count;
   std::vector<uint64_t> key_frequency(n, 0);
   for (size_t i = 0; i < n * 2; ++i) {
      YCSBKey key = zipf_gen->zipf_generator.rand() % n;
      generated_keys.push_back(key);
      key_frequency[key]++;
   }
   sort(generated_keys.begin(), generated_keys.end());
   std::cout << "Keyspace Stats: " << std::endl;
   std::cout << "Skew factor: " << FLAGS_zipf_factor << std::endl;
   std::cout << "# total keys: " << generated_keys.size() << std::endl;
   //std::cout << "# unique keys: " << unique_keys.size() << std::endl;
   std::cout << "Most frequent key occurrence: " << key_frequency[generated_keys[0]] << std::endl;
   auto print_percentile_info = [&](int p) {
      double pd = p / 100.0;
      std::cout << "p" << p  << ": " << generated_keys[pd*generated_keys.size()] << ", covering " << generated_keys[pd*generated_keys.size()] / (0.0 + n) * 100 << "% of the keys, occurrence :" << key_frequency[generated_keys[pd*generated_keys.size()]] << std::endl;
   };
   for (size_t i = 1; i <= 99; i += 1) {
      print_percentile_info(i);
   }
   print_percentile_info(99);
}


void selfsimilar_keyspace_stats(utils::SelfSimilarGenerator * ss_gen) {
   std::vector<YCSBKey> generated_keys;
   std::unordered_map<YCSBKey, int> unique_keys;
   for (size_t i = 0; i < FLAGS_ycsb_tuple_count * 2; ++i) {
      YCSBKey key = ss_gen->rand() % FLAGS_ycsb_tuple_count;
      generated_keys.push_back(key);
      unique_keys[key]++;
   }
   sort(generated_keys.begin(), generated_keys.end());
   std::cout << "Keyspace Stats: " << std::endl;
   std::cout << "Skew factor: " << FLAGS_ycsb_request_selfsimilar_skew << std::endl;
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

void show_5_min_rule_placement(const vector<u64> & access_count,
   const vector<u64> & sum_reaccess_interval, size_t record_size) {
   constexpr double price_per_GB_DRAM = 2.69;
   constexpr double price_per_byte_dram = price_per_GB_DRAM / (1024.0 * 1024 * 1024);
   constexpr double price_per_STORAGE_DEVICE = 30;
   constexpr int IOPS = 170000;
   constexpr double price_per_SSD_IO = price_per_STORAGE_DEVICE / IOPS;
   double break_even_interval = 1 / (price_per_byte_dram * record_size) * price_per_SSD_IO;
   int on_storage = 0;
   int in_memory = 0;
   for (size_t i = 0; i < access_count.size(); ++i) {
      if (access_count[i] == 0) {
         on_storage++;
      } else {
         double avg_reaccess_interval_seconds = sum_reaccess_interval[i] / access_count[i] / 1000000;
         if (avg_reaccess_interval_seconds <= break_even_interval) {
            in_memory++;
         } else {
            on_storage++;
         }
      }
   }

   cout << "5_min_rule_break_even_interval: " << break_even_interval << " seconds" << std::endl;
   cout << "5_min_rule_placement: " << in_memory << " records in memory, " << on_storage << " on storage" << std::endl;
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
   double effective_page_to_frame_ratio = sizeof(leanstore::storage::BufferFrame::Page) / (sizeof(leanstore::storage::BufferFrame) + 0.0);
   double top_tree_size_gib = 0;
   s64 hot_pages_limit = std::numeric_limits<s64>::max();
   if (FLAGS_index_type == kIndexTypeHeap || FLAGS_index_type == kIndexTypeIHeap || FLAGS_index_type == kIndexTypeBTree || FLAGS_index_type == kIndexTypeHash || FLAGS_index_type == kIndexTypeHashWOH  || FLAGS_index_type == kIndexTypeSTXBTree ||  FLAGS_index_type == kIndexType2BTree || FLAGS_index_type == kIndexType2Hash || FLAGS_index_type == kIndexType2IHeap || FLAGS_index_type == kIndexTypeC2BTree || FLAGS_index_type == kIndexType2LSMT_CF) {
      hot_pages_limit = FLAGS_dram_gib * FLAGS_top_component_dram_ratio * 1024 * 1024 * 1024 / sizeof(leanstore::storage::BufferFrame);
      top_tree_size_gib = FLAGS_dram_gib * FLAGS_top_component_dram_ratio * effective_page_to_frame_ratio;
   } else if (FLAGS_index_type == kIndexTypeUpLSMT || 
              FLAGS_index_type == kIndexTypeLSMT || 
              FLAGS_index_type == kIndexTypeAntiCache || 
              FLAGS_index_type == kIndexTypeAntiCacheB || 
              FLAGS_index_type == kIndexTypeTrieBTree || 
              FLAGS_index_type == kIndexTypeIM2BTree ||
              FLAGS_index_type == kIndexType2LSMT || 
              FLAGS_index_type == kIndexTypeTrieLSMT ||
              FLAGS_index_type == kIndexTypeSTX2BTree) {
      top_tree_size_gib = FLAGS_dram_gib * FLAGS_top_component_dram_ratio;
      FLAGS_dram_gib = FLAGS_dram_gib * (1 - FLAGS_top_component_dram_ratio);
   } else {
      cout << "Unknown index type " << FLAGS_index_type << std::endl;
      assert(false);
      exit(1);
   }
   cout << "record_size: " << ((RECORD_SIZE) + sizeof(YCSBKey)) << std::endl;
   cout << "Date and Time of the run: " << getCurrentDateTime() << std::endl;
   cout << "lazy migration sampling rate " << FLAGS_cache_lazy_migration << "%" << std::endl;
   cout << "hot_pages_limit " << hot_pages_limit << std::endl;
   cout << "effective_page_to_frame_ratio " << effective_page_to_frame_ratio << std::endl;
   cout << "index type " << FLAGS_index_type << std::endl; 
   cout << "request distribution " << FLAGS_ycsb_request_dist << std::endl;
   cout << "dram_gib " << FLAGS_dram_gib << std::endl;
   cout << "top_component_size_gib " << top_tree_size_gib << std::endl;
   cout << "wal=" << FLAGS_wal << std::endl;
   cout << "zipf_factor=" << FLAGS_zipf_factor << std::endl;
   cout << "ycsb_read_ratio=" << FLAGS_ycsb_read_ratio << std::endl;
   cout << "ycsb_update_ratio=" << FLAGS_ycsb_update_ratio << std::endl;
   cout << "ycsb_scan_ratio=" << FLAGS_ycsb_scan_ratio << std::endl;
   cout << "run_for_seconds=" << FLAGS_run_for_seconds << std::endl;
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   std::unique_ptr<leanstore::utils::Generator> random_generator;
   if (FLAGS_ycsb_request_dist == kRequestDistributionZipfian) {
      random_generator.reset(new utils::ScrambledZipfGenerator(0, ycsb_tuple_count, FLAGS_zipf_factor));
   } else if (FLAGS_ycsb_request_dist == kRequestDistributionHotspot){
      cout << "hotspot_keyspace_fraction=" << FLAGS_ycsb_request_hotspot_keyspace_fraction << std::endl;
      cout << "hotspot_op_fraction=" << FLAGS_ycsb_request_hotspot_op_fraction << std::endl;
      random_generator.reset(new utils::HotspotGenerator(0, ycsb_tuple_count, FLAGS_ycsb_request_hotspot_keyspace_fraction, FLAGS_ycsb_request_hotspot_op_fraction));
   } else if (FLAGS_ycsb_request_dist == kRequestDistributionHotspotZipfian){
      cout << "hotspot_keyspace_fraction=" << FLAGS_ycsb_request_hotspot_keyspace_fraction << std::endl;
      cout << "hotspot_op_fraction=" << FLAGS_ycsb_request_hotspot_op_fraction << std::endl;
      random_generator.reset(new utils::HotspotZipfGenerator(0, ycsb_tuple_count, FLAGS_ycsb_request_hotspot_keyspace_fraction, FLAGS_ycsb_request_hotspot_op_fraction, FLAGS_zipf_factor));
   } else {
      //kRequestDistributionSelfSimilar
      cout << "selfsimilar_skew=" << FLAGS_ycsb_request_selfsimilar_skew << std::endl;
      random_generator.reset(new utils::SelfSimilarGenerator(0, ycsb_tuple_count, FLAGS_ycsb_request_selfsimilar_skew));
   }
   LeanStore db;
   db.getBufferManager().hot_pages_limit = hot_pages_limit;
   unique_ptr<StorageInterface<YCSBKey, YCSBPayload>> adapter;
   leanstore::storage::btree::BTreeLL* btree_ptr = nullptr;
   leanstore::storage::btree::BTreeLL* btree2_ptr = nullptr;
   leanstore::storage::hashing::LinearHashTable* ht_ptr = nullptr;
   leanstore::storage::hashing::LinearHashTable* ht2_ptr = nullptr;
   leanstore::storage::hashing::LinearHashTableWithOverflowHeap* htoh2_ptr = nullptr;
   leanstore::storage::heap::HeapFile* hf_ptr = nullptr;
   leanstore::storage::heap::HeapFile* hf2_ptr = nullptr;
   db.getCRManager().scheduleJobSync(0, [&](){
      if (FLAGS_recover) {
         btree_ptr = &db.retrieveBTreeLL("btree", true);
         btree2_ptr = &db.retrieveBTreeLL("btree_cold");
         ht_ptr = &db.retrieveHashTable("ht", true);
         ht2_ptr = &db.retrieveHashTable("ht_cold");
         hf_ptr = &db.retrieveHeapFile("hf", true);
         hf2_ptr = &db.retrieveHeapFile("hf_cold");
         htoh2_ptr = &db.retrieveHashTableWOH("htoh_cold");
      } else {
         btree_ptr = &db.registerBTreeLL("btree", true);
         btree2_ptr = &db.registerBTreeLL("btree_cold");
         ht_ptr = &db.registerHashTable("ht", true);
         ht2_ptr = &db.registerHashTable("ht_cold");
         hf_ptr = &db.registerHeapFile("hf", true);
         hf2_ptr = &db.registerHeapFile("hf_cold");
         auto heap_file_for_hashing = &db.registerHeapFile("hf_for_ht_cold");
         htoh2_ptr = &db.registerHashTableWOH("htoh_cold", *heap_file_for_hashing);
      }
   });
   
   if (FLAGS_index_type == kIndexTypeHash) {
      adapter.reset(new HashVSAdapter<YCSBKey, YCSBPayload>(*ht2_ptr, ht2_ptr->dt_id));
   } else if (FLAGS_index_type == kIndexTypeHashWOH) {
      adapter.reset(new HashWOHVSAdapter<YCSBKey, YCSBPayload>(*htoh2_ptr, htoh2_ptr->dt_id));
   } else if (FLAGS_index_type == kIndexTypeBTree) {
      adapter.reset(new BTreeVSAdapter<YCSBKey, YCSBPayload>(*btree2_ptr, btree2_ptr->dt_id));
   } else if (FLAGS_index_type == kIndexTypeHeap) {
      adapter.reset(new HeapFileAdapter<YCSBKey, YCSBPayload>(*hf_ptr, hf_ptr->dt_id));
   } else if (FLAGS_index_type == kIndexTypeIHeap) {
      adapter.reset(new IndexedHeapAdapter<YCSBKey, YCSBPayload>(*hf2_ptr, *btree2_ptr));
   } else if (FLAGS_index_type == kIndexType2IHeap) {
      adapter.reset(new TwoIHeapAdapter<YCSBKey, YCSBPayload>(*hf_ptr, *btree_ptr, *hf2_ptr, *btree2_ptr, FLAGS_ycsb_2heap_eviction_index_scan, top_tree_size_gib, FLAGS_inclusive_cache, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexType2BTree) {
      adapter.reset(new TwoBTreeAdapter<YCSBKey, YCSBPayload>(*btree_ptr, *btree2_ptr, top_tree_size_gib, FLAGS_inclusive_cache, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexType2Hash) {
      if (FLAGS_ycsb_2hash_use_different_hash) {
         adapter.reset(new TwoHashAdapter<YCSBKey, YCSBPayload, leanstore::utils::XXH, leanstore::utils::FNV>(*ht_ptr, *ht2_ptr, FLAGS_ycsb_2hash_eviction_record_count, FLAGS_ycsb_2hash_eviction_by_record, top_tree_size_gib, FLAGS_inclusive_cache, FLAGS_cache_lazy_migration));
      } else {
         adapter.reset(new TwoHashAdapter<YCSBKey, YCSBPayload>(*ht_ptr, *ht2_ptr, FLAGS_ycsb_2hash_eviction_record_count, FLAGS_ycsb_2hash_eviction_by_record, top_tree_size_gib, FLAGS_inclusive_cache, FLAGS_cache_lazy_migration));
      }
   } else if (FLAGS_index_type == kIndexTypeC2BTree) {
      adapter.reset(new ConcurrentTwoBTreeAdapter<YCSBKey, YCSBPayload>(*btree_ptr, *btree2_ptr, top_tree_size_gib, FLAGS_inclusive_cache, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexTypeIM2BTree) {
      adapter.reset(new BTreeCachedVSAdapter<YCSBKey, YCSBPayload>(*btree2_ptr, top_tree_size_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexTypeTrieBTree) {
      adapter.reset(new BTreeTrieCachedVSAdapter<YCSBKey, YCSBPayload>(*btree2_ptr, top_tree_size_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexTypeLSMT) {
      adapter.reset(new RocksDBAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_use_record_cache, FLAGS_lsmt_db_path, top_tree_size_gib, FLAGS_dram_gib, false, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexTypeUpLSMT) {
      adapter.reset(new UpMigrationRocksDBAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, FLAGS_dram_gib + top_tree_size_gib, false, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexType2LSMT) {
      adapter.reset(new TwoRocksDBAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, top_tree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexType2LSMT_CF){
      adapter.reset(new RocksDBTwoCFAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, top_tree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexTypeTrieLSMT) {
      adapter.reset(new TrieRocksDBAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, top_tree_size_gib, FLAGS_dram_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexTypeAntiCache) { 
      adapter.reset(new AntiCacheAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, top_tree_size_gib, FLAGS_dram_gib));
   } else if (FLAGS_index_type == kIndexTypeSTX2BTree) {
      adapter.reset(new STX2BTreeAdapter<YCSBKey, YCSBPayload>(top_tree_size_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else if (FLAGS_index_type == kIndexTypeSTXBTree) {
      adapter.reset(new STXBTreeAdapter<YCSBKey, YCSBPayload>(top_tree_size_gib, FLAGS_cache_lazy_migration, FLAGS_inclusive_cache));
   } else {
      assert(FLAGS_index_type == kIndexTypeAntiCacheB);
      adapter.reset(new AntiCacheBTreeAdapter<YCSBKey, YCSBPayload>(*btree2_ptr, top_tree_size_gib));
   }

   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   db.registerConfigEntry("ycsb_target_gib", FLAGS_target_gib);
   db.startProfilingThread();
   adapter->set_buffer_manager(db.buffer_manager.get());
   // -------------------------------------------------------------------------------------
   auto& table = *adapter;
   // Insert values
   const u64 n = ycsb_tuple_count;
   vector<u64> keys(n);
   vector<u8> correct_payloads(n);
   vector<bool> key_deleted(n, false);
   vector<u64> last_access_timestamps(n, 0);
   vector<u64> access_count(n, 0);
   vector<u64> sum_reaccess_interval(n, 0);
   vector<std::shared_mutex> * key_mutexes = nullptr;
   if (FLAGS_ycsb_correctness_check) {
      key_mutexes = new std::vector<std::shared_mutex>(n);
   }
   #define C_RLOCK(i) \
   if (FLAGS_ycsb_correctness_check) {\
      (*key_mutexes)[(i)].lock_shared();     \
   }
   #define C_WLOCK(i) \
   if (FLAGS_ycsb_correctness_check) {\
      (*key_mutexes)[(i)].lock();     \
   }
   #define C_RUNLOCK(i)\
   if (FLAGS_ycsb_correctness_check) {\
      (*key_mutexes)[(i)].unlock_shared();     \
   }
   #define C_WUNLOCK(i)\
   if (FLAGS_ycsb_correctness_check) {\
      (*key_mutexes)[(i)].unlock();     \
   }
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
         if (FLAGS_index_type == kIndexType2LSMT || FLAGS_index_type == kIndexTypeLSMT || FLAGS_index_type == kIndexType2LSMT_CF || FLAGS_index_type == kIndexTypeUpLSMT) {
            tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
               for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
                  YCSBKey key = keys[t_i];
                  C_WLOCK(t_i);
                  correct_payloads[t_i] = (u8)key;
                  YCSBPayload payload((u8)key);
                  //utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  table.insert(key, payload);
                  C_WUNLOCK(t_i);
                  WorkerCounters::myCounters().tx++;
               }
            });
         } else {
            tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
               //cout << "range size " << range.end() - range.begin() << std::endl;
               vector<u64> range_keys(range.end() - range.begin());
               std::iota(range_keys.begin(), range_keys.end(), range.begin());
               std::random_shuffle(range_keys.begin(), range_keys.end());
               for (u64 t_i = 0; t_i < range_keys.size(); t_i++) {
                  u64 idx = range_keys[t_i];
                  YCSBKey key = keys[idx];
                  C_WLOCK(idx);
                  YCSBPayload payload((u8)key);
                  correct_payloads[idx] = (u8)key;
                  table.insert(key, payload);
                  C_WUNLOCK(idx);

                  WorkerCounters::myCounters().tx++;
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

   adapter->report(FLAGS_ycsb_tuple_count, db.getBufferManager().consumedPages());
   std::cout << "Hot pages: " << db.buffer_manager->hot_pages << std::endl;
   //adapter->evict_all();
   // if (FLAGS_index_type == kIndexType2LSMT || FLAGS_index_type == kIndexTypeLSMT || FLAGS_index_type == kIndexType2LSMT_CF || FLAGS_index_type == kIndexTypeLSMT)
   {
      if (FLAGS_index_type == kIndexTypeHeap) {
         int count = 0;
         auto scan_processor = [&](const YCSBKey & key, const YCSBPayload & payload) {
            ++count;
            return false;
         };

         table.scan(0, scan_processor, -1);
         assert(count == ycsb_tuple_count);
      }
      cout << "Warming up" << endl;
      auto t_start = std::chrono::high_resolution_clock::now();
      int i = 0;
      while (std::chrono::duration<double, std::milli>(std::chrono::high_resolution_clock::now()- t_start).count() < 20000) {
         auto key_idx = random_generator->rand() % ycsb_tuple_count;
         YCSBKey key = keys[key_idx] % FLAGS_ycsb_keyspace_count;
         C_RLOCK(key_idx);
         YCSBPayload correct_result(correct_payloads[key_idx]);
         assert(key < ycsb_tuple_count);
         YCSBPayload result;
         bool res = table.lookup(key, result);
         assert(res);
         if (result != correct_result) {
            table.lookup(key, result);
         }
         C_RUNLOCK(key_idx);
         
         assert(result == correct_result);
         WorkerCounters::myCounters().tx++;
         ++i;
      }
      cout << "Warmed up" << endl;
   }

   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   atomic<u64> txs = 0;
   vector<thread> threads;
   begin = chrono::high_resolution_clock::now();
   int scan_len;
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      db.getCRManager().scheduleJobAsync(t_i, [&]() {
         adapter->clear_stats();
         running_threads_counter++;
         u64 tx = 0;
         
         while (keep_running) {
            auto idx = random_generator->rand() % ycsb_tuple_count;
            YCSBKey key = keys[idx];
            // auto old_access_timestamp = last_access_timestamps[idx];
            // last_access_timestamps[idx] = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            // if (old_access_timestamp != 0) {
            //    sum_reaccess_interval[idx] += last_access_timestamps[idx] - old_access_timestamp;
            // }
            //access_count[idx]++;
            assert(key < ycsb_tuple_count);
            YCSBPayload result;
            int x = utils::RandomGenerator::getRandU64(0, 100);
            if (x < FLAGS_ycsb_scan_ratio) {
               key = utils::RandomGenerator::getRandU64() % ycsb_tuple_count;
               scan_len = FLAGS_ycsb_scan_length;
               YCSBKey tk;
               YCSBPayload tv;
               auto scan_processor = [&](const YCSBKey & key, const YCSBPayload & payload) {
                  tk = key;
                  tv = payload;
                  if (--scan_len == 0) {
                     return true;
                  }
                  return false;
               };

               table.scan(key, scan_processor, scan_len);
            } else if (x < FLAGS_ycsb_scan_ratio + FLAGS_ycsb_blind_write_ratio) {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               table.insert(key, payload);
            } else if (x < FLAGS_ycsb_scan_ratio + FLAGS_ycsb_blind_write_ratio + FLAGS_ycsb_update_ratio) {
               C_WLOCK(idx);
               if (key_deleted[idx] == false) {
                  correct_payloads[idx]++;
                  YCSBPayload payload(correct_payloads[idx]);
                  if (FLAGS_update_or_put == 0) {
                     table.update(key, payload);
                  } else {
                     table.put(key, payload);
                  }
                  //assert(table.lookup(key, result));
               }
               C_WUNLOCK(idx);
            } else if (x < FLAGS_ycsb_scan_ratio + FLAGS_ycsb_blind_write_ratio + FLAGS_ycsb_update_ratio + FLAGS_ycsb_delete_ratio) {
               C_WLOCK(idx);
               if (key_deleted[idx] == false) {
                  bool res = table.remove(key);
                  // if (res == false) {
                  //    table.remove(key);
                  // }
                  assert(res == true);
                  key_deleted[idx] = true;
                  //assert(table.lookup(key, result) == false);
               }
               C_WUNLOCK(idx);
            } else if (x < FLAGS_ycsb_scan_ratio + FLAGS_ycsb_blind_write_ratio + FLAGS_ycsb_update_ratio + FLAGS_ycsb_delete_ratio + FLAGS_ycsb_insert_ratio) {
               C_WLOCK(idx);
               if (key_deleted[idx] == true) {
                  YCSBPayload payload((u8)key);
                  correct_payloads[idx] = (u8)key;
                  table.insert(key, payload);
                  key_deleted[idx] = false;
                  //assert(table.lookup(key, result));
               }
               C_WUNLOCK(idx);
            } else { // x < FLAGS_ycsb_scan_ratio + FLAGS_ycsb_update_ratio + FLAGS_ycsb_read_ratio + FLAGS_ycsb_delete_ratio
               C_RLOCK(idx);
               bool res = table.lookup(key, result);
               
               if (FLAGS_ycsb_correctness_check) {
                  if (key_deleted[idx]) {
                     assert(res == false);
                  } else {
                     if (res == false) {
                        auto res2 = table.lookup(key, result);
                     }
                     assert(res);
                     auto correct_payload_byte = correct_payloads[idx];
                     YCSBPayload correct_result(correct_payload_byte);
                     if (result != correct_result) {
                        res = table.lookup(key, result);
                     }
                     assert(result == correct_result);
                  }
               }
               C_RUNLOCK(idx);
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
      sleep(FLAGS_run_for_seconds * 0.8);
      // cout << "Clearing IO stats after reaching steady state" << endl;
      // table.clear_io_stats();
      sleep(FLAGS_run_for_seconds * 0.2);
      keep_running = false;
      while (running_threads_counter) {
         MYPAUSE();
      }
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      // -------------------------------------------------------------------------------------
      cout << "Total commit: " << calculateMTPS(begin, end, txs.load()) << " M tps" << endl;
      adapter->report(FLAGS_ycsb_tuple_count, db.getBufferManager().consumedPages());
      std::cout << "Hot pages: " << db.buffer_manager->hot_pages << std::endl;
      show_5_min_rule_placement(access_count, sum_reaccess_interval, sizeof(YCSBPayload) + sizeof(YCSBKey));
      if (FLAGS_ycsb_request_dist == kRequestDistributionZipfian) {
         zipf_keyspace_stats((utils::ScrambledZipfGenerator*)random_generator.get());
      } else if (FLAGS_ycsb_request_dist == kRequestDistributionSelfSimilar) {
         selfsimilar_keyspace_stats((utils::SelfSimilarGenerator*)random_generator.get());
      }
      
      cout << "-------------------------------------------------------------------------------------" << endl;
      
      db.getCRManager().joinAll();
   }


   cout << "-------------------------------------------------------------------------------------" << endl;
   // -------------------------------------------------------------------------------------
   return 0;
}