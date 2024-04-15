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
#include "treeline/Treeline.hpp"
#include "lsmt/upmigration_rocksdb_adapter.hpp"
#include "lsmt/bidirectional_migration_rocksdb_adapter.hpp"
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
DEFINE_string(results_csv_file_path, "", "save the results to a file in csv format");
DEFINE_bool(lsmt_populate_block_cache_for_compaction_and_flush, false, "prepopulate block cache during memtable flush and compaction?");
DEFINE_bool(lsmt_adaptive_migration, false, "Adaptively control the upward migration");
DEFINE_bool(lsmt_in_memory_migration, true, "Enable upward migration for in-memory data");
DEFINE_bool(lsmt_bulk_migration_at_flush, true, "Enable fast bulk migration at flush");
DEFINE_bool(inclusive_cache, false, "");
DEFINE_uint32(update_or_put, 0, "");
DEFINE_uint32(hotspot_shift_phases, 0, "Number of shifts of hotspots in the workloads");
DEFINE_uint32(cache_lazy_migration, 100, "lazy upward migration sampling rate(%)");
DEFINE_bool(ycsb_scan, false, "");
DEFINE_bool(ycsb_tx, true, "");
DEFINE_bool(ycsb_2heap_eviction_index_scan, true, "");
DEFINE_uint64(ycsb_2hash_eviction_record_count, 1, "");
DEFINE_bool(ycsb_2hash_eviction_by_record, false, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
DEFINE_bool(ycsb_2hash_use_different_hash, false, "whether to use different hash functions for two hash tables");

DEFINE_string(ycsb_request_dist, "hotspot", "");
DEFINE_string(trace_file, "./", "trace file path");
DEFINE_double(ycsb_request_hotspot_keyspace_fraction, 0.2, "");
DEFINE_double(ycsb_request_hotspot_op_fraction, 1.0, "");
DEFINE_double(ycsb_request_selfsimilar_skew, 0.2, "");
DEFINE_bool(ycsb_correctness_check, false, "correctness check");

const std::string kRequestDistributionZipfian = "zipfian";
const std::string kRequestDistributionZipfian2 = "zipfian2";
const std::string kRequestDistributionSelfSimilar = "selfsimilar";
const std::string kRequestDistributionHotspot = "hotspot";
const std::string kRequestDistributionHotspotZipfian = "hotspot_zipfian";

const std::string kIndexTypeBTree = "BTree";
const std::string kIndexTypeHash = "Hash";
const std::string kIndexTypeHashWOH = "HashWOH";
const std::string kIndexTypeLSMT = "LSMT";
const std::string kIndexTypeHeap = "Heap";
const std::string kIndexTypeIHeap = "IHeap";
const std::string kIndexTypeTreeline = "Treeline";

const std::string kIndexTypeAntiCache = "AntiCache";
const std::string kIndexTypeAntiCacheB = "AntiCacheB";
const std::string kIndexType2BTree = "2BTree";
const std::string kIndexType2Hash = "2Hash";
const std::string kIndexType2IHeap = "2IHeap";
const std::string kIndexTypeC2BTree = "C2BTree";
const std::string kIndexType2LSMT = "2LSMT";
const std::string kIndexType2LSMT_CF = "2LSMT-CF";
const std::string kIndexTypeUpLSMT = "UpLSMT";
const std::string kIndexTypeBiUpLSMT = "BiUpLSMT";
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

enum class OperationType {
   Get = 0,
   Set = 1
};
struct Operation {
   uint64_t key;
   OperationType type;
};

std::vector<std::string> Split(const std::string & s, const std::string & delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    std::string token;
    std::vector<std::string> res;

    while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
        token = s.substr (pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back (token);
    }

    res.push_back (s.substr (pos_start));
    return res;
}

Operation Parse(const std::string & str) {
   auto parts = Split(str, ",");
   return Operation{leanstore::utils::FNV::hash(parts[1].data(), parts[1].size()), parts[5] == "get" ? OperationType::Get : OperationType::Set };
}

template<class T>
class SynchronizedQueue {
public:
   bool pop(T & v) {
      std::lock_guard<std::mutex> g(&lock);
      if (queue.empty() == false) {
         v = std::move(queue.front());
         queue.pop_front();
         return true;
      } else {
         return false;
      }
   }
   void push(const T & v) {
      std::lock_guard<std::mutex> g(&lock);
      queue.push_back(v);
   }
   
   bool push(const T & v, int queue_limit) {
      std::lock_guard<std::mutex> g(&lock);
      if (queue.size() < queue_limit) {
         queue.push_back(v);
         return true;
      } else {
         return false;
      }
   }

   std::deque<T> queue;
   std::mutex lock;
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
   double effective_bp_dram_gib = 0;
   if (FLAGS_index_type == kIndexTypeHeap || FLAGS_index_type == kIndexTypeIHeap || FLAGS_index_type == kIndexTypeBTree || FLAGS_index_type == kIndexTypeHash || FLAGS_index_type == kIndexTypeHashWOH  || FLAGS_index_type == kIndexTypeSTXBTree ||  FLAGS_index_type == kIndexType2BTree || FLAGS_index_type == kIndexType2Hash || FLAGS_index_type == kIndexType2IHeap || FLAGS_index_type == kIndexTypeC2BTree || FLAGS_index_type == kIndexType2LSMT_CF  || 
              FLAGS_index_type == kIndexTypeTreeline) {
      hot_pages_limit = FLAGS_dram_gib * FLAGS_top_component_dram_ratio * 1024 * 1024 * 1024 / leanstore::storage::PAGE_SIZE;
      top_tree_size_gib = FLAGS_dram_gib * FLAGS_top_component_dram_ratio;
      effective_bp_dram_gib = FLAGS_dram_gib / effective_page_to_frame_ratio;
   } else if (FLAGS_index_type == kIndexTypeAntiCache || 
              FLAGS_index_type == kIndexTypeAntiCacheB || 
              FLAGS_index_type == kIndexTypeTrieBTree || 
              FLAGS_index_type == kIndexTypeIM2BTree ||
              FLAGS_index_type == kIndexType2LSMT || 
              FLAGS_index_type == kIndexTypeTrieLSMT ||
              FLAGS_index_type == kIndexTypeSTX2BTree) {
      top_tree_size_gib = FLAGS_dram_gib * FLAGS_top_component_dram_ratio;
      FLAGS_dram_gib = FLAGS_dram_gib * (1 - FLAGS_top_component_dram_ratio);
      effective_bp_dram_gib = FLAGS_dram_gib / effective_page_to_frame_ratio;
   } else if (FLAGS_index_type == kIndexTypeUpLSMT || 
              FLAGS_index_type == kIndexTypeBiUpLSMT || 
              FLAGS_index_type == kIndexTypeLSMT) {
      top_tree_size_gib = FLAGS_dram_gib * FLAGS_top_component_dram_ratio;
      FLAGS_dram_gib = FLAGS_dram_gib * (1 - FLAGS_top_component_dram_ratio);
      effective_bp_dram_gib = FLAGS_dram_gib / effective_page_to_frame_ratio;
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
   cout << "effective_bp_dram_gib " << effective_bp_dram_gib << std::endl;
   cout << "top_component_size_gib " << top_tree_size_gib << std::endl;
   cout << "wal=" << FLAGS_wal << std::endl;
   cout << "zipf_factor=" << FLAGS_zipf_factor << std::endl;
   cout << "ycsb_read_ratio=" << FLAGS_ycsb_read_ratio << std::endl;
   cout << "ycsb_update_ratio=" << FLAGS_ycsb_update_ratio << std::endl;
   cout << "ycsb_scan_ratio=" << FLAGS_ycsb_scan_ratio << std::endl;
   cout << "run_for_seconds=" << FLAGS_run_for_seconds << std::endl;
   cout << "hotspot_shift_phases=" << FLAGS_hotspot_shift_phases << std::endl;
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   cout << "ycsb_tuple_count=" << ycsb_tuple_count << std::endl;
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
   } else if (FLAGS_ycsb_request_dist == kRequestDistributionZipfian2) {
      random_generator.reset(new utils::ScrambledZipfDistGenerator(0, ycsb_tuple_count, FLAGS_zipf_factor));
   } else {
      //kRequestDistributionSelfSimilar
      cout << "selfsimilar_skew=" << FLAGS_ycsb_request_selfsimilar_skew << std::endl;
      random_generator.reset(new utils::SelfSimilarGenerator(0, ycsb_tuple_count, FLAGS_ycsb_request_selfsimilar_skew));
   }
   LeanStore db(effective_bp_dram_gib);
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
   } else if (FLAGS_index_type == kIndexTypeTreeline) {
      adapter.reset(new TreelineAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, sizeof(YCSBKey) + RECORD_SIZE, ycsb_tuple_count, ycsb_tuple_count - 1, FLAGS_dram_gib));
   } else if (FLAGS_index_type == kIndexTypeLSMT) {
      adapter.reset(new RocksDBAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_use_record_cache, FLAGS_lsmt_populate_block_cache_for_compaction_and_flush, FLAGS_lsmt_db_path, top_tree_size_gib, FLAGS_dram_gib, false));
   } else if (FLAGS_index_type == kIndexTypeUpLSMT) {
      adapter.reset(new UpMigrationRocksDBAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, FLAGS_lsmt_adaptive_migration, FLAGS_lsmt_in_memory_migration, FLAGS_lsmt_populate_block_cache_for_compaction_and_flush, FLAGS_dram_gib + top_tree_size_gib, false, FLAGS_cache_lazy_migration));
   } else if (FLAGS_index_type == kIndexTypeBiUpLSMT) {
      adapter.reset(new BidirectionalMigrationRocksDBAdapter<YCSBKey, YCSBPayload>(FLAGS_lsmt_db_path, FLAGS_lsmt_bulk_migration_at_flush, FLAGS_lsmt_adaptive_migration, FLAGS_lsmt_in_memory_migration, FLAGS_lsmt_populate_block_cache_for_compaction_and_flush, FLAGS_dram_gib + top_tree_size_gib, false, FLAGS_cache_lazy_migration));
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
   auto adapter_ptr = adapter.get();
   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   db.registerConfigEntry("ycsb_target_gib", FLAGS_target_gib);
   db.startProfilingThread(adapter_ptr->stats_column_names(), [adapter_ptr](){ return adapter_ptr->stats_columns(); }, FLAGS_results_csv_file_path);
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
      
      std::iota(keys.begin(), keys.end(), 1);
      std::random_shuffle(keys.begin(), keys.end());
      begin = chrono::high_resolution_clock::now();
      {
         if (FLAGS_index_type == kIndexTypeTreeline) {
            std::vector<YCSBKey> bulkload_keys;
            std::vector<YCSBPayload> payloads;
            for (int i = 0; i < 50240; ++i) {
               bulkload_keys.push_back(keys[i]);
            }
            sort(bulkload_keys.begin(), bulkload_keys.end());
            for (int i = 0; i < 50240; ++i) {
               YCSBPayload payload((u8)bulkload_keys[i]);
               payloads.push_back(payload);
            }
            
            table.bulk_load(bulkload_keys, payloads);
         }
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
         //}
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
   //std::cout << "All evicted" << std::endl;
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
   std::vector<SynchronizedQueue<std::vector<Operation>>> queues(FLAGS_worker_threads);

   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   atomic<u64> txs = 0;
   vector<thread> threads;
   begin = chrono::high_resolution_clock::now();
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      db.getCRManager().scheduleJobAsync(t_i, [&, t_i]() {
         int i = t_i;
         adapter->clear_stats();
         running_threads_counter++;
         u64 tx = 0;
         int scan_len;
         while (keep_running) {
            std::vector<Operation> ops;
            while(queues[i].pop(ops) == false) {
               ;
            }
            for (int j = 0; j < ops.size(); ++j) {
               auto hash = ops[j].key;
               auto type = ops[j].type;
               YCSBKey key = hash;
               auto idx = key % ycsb_tuple_count;
               assert(key < ycsb_tuple_count);
               YCSBPayload result;
               if (type == OperationType::Set) {
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
                  C_WUNLOCK(idx) ;
               } else {
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
            
         }
         txs += tx;
         running_threads_counter--;
      });
   }
   {
      if (FLAGS_hotspot_shift_phases <= 1) {
         sleep(FLAGS_run_for_seconds);
      } else {
         auto seconds_per_phase = FLAGS_run_for_seconds / FLAGS_hotspot_shift_phases;
         for (int i = 0; i < FLAGS_hotspot_shift_phases - 1; ++i) {
            sleep(seconds_per_phase);
            cout << "Shifting hotspots after phase " << (i + 1) << endl;
            std::random_shuffle(keys.begin(), keys.end());
         }
         sleep(seconds_per_phase);
      }
      std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      std::ifstream infile(trace_file);
      std::string line;
      constexpr int kBatchSize = 1024;
      constexpr bool kQueueLimit = 10;
      int queueId = 0;
      while(true) {
         std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
         auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end - begin).count();
         if (seconds > FLAGS_run_for_seconds) {
            break;
         }

         int countDown = kBatchSize;
         std::vector<Operation> ops;
         while(std::getline(infile, line) && countDown--) {
            ops.emplace_back(std::move(Parse(line)));
         }
         if (ops.empty()) {
            break;
         }
         while(true) {
            if (queues[(++queueId) % FLAGS_worker_threads].push(ops, kQueueLimit)) {
               break;
            }
         }
      }
      // Shutdown threads
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

      cout << "-------------------------------------------------------------------------------------" << endl;
      db.getCRManager().joinAll();
   }


   cout << "-------------------------------------------------------------------------------------" << endl;
   // -------------------------------------------------------------------------------------
   return 0;
}