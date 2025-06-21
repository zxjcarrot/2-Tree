#pragma once
// -------------------------------------------------------------------------------------
#include "treeline/pg_db.h"
#include "treeline/pg_stats.h"
#include "treeline/key.h"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <filesystem>
template <typename Key, typename Payload>
struct TreelineAdapter : public leanstore::StorageInterface<Key, Payload> {
   std::size_t total_lookups = 0;
   DistributedCounter<> lookups = 0;
   DistributedCounter<> level_accessed = 0;
   tl::pg::PageGroupedDB * db_;
   IOStats stat;
   TreelineAdapter(const std::string & db_dir, size_t record_size, uint64_t num_keys, uint64_t max_key, double memory_budget_gib) 
   {
      const std::string dbname = db_dir + "/tl";
      tl::pg::PageGroupedDBOptions options;
      auto record_cache_size_per_entry = record_size +  // record payload
                                         + 96 - sizeof(pthread_rwlock_t) // record cache entry for metadata without lock
                                         + sizeof(Key) + sizeof(uintptr_t); // space occupied by index, we only account for the key and pointer to the RecordCacheEntry.

      //options.buffer_pool_size = block_cache_memory_budget_gib * 1024 * 1024 * 1024UL;
      std::cerr << "Treeline memory budget " << memory_budget_gib << "GiB" << std::endl;
      std::cerr << "Treeline record cache size per entry " << record_cache_size_per_entry << std::endl;
      options.record_cache_capacity = (memory_budget_gib) * 1024 * 1024 * 1024UL / (record_cache_size_per_entry);
      options.use_memory_based_io = false;
      options.rewrite_search_radius = 0;
      options.records_per_page_goal = 16 * 1024 / (sizeof(Key) + sizeof(Payload) + 20);
      options.num_bg_threads = 0;
      options.optimistic_caching = false;
      options.rec_cache_use_lru = false;
      options.rec_cache_batch_writeout = false; // setting batch-write-out to true causes deadlocks when there are concurrent evictions
      options.use_segments = true;
      uint64_t min_key = 0;
      std::filesystem::remove_all(db_dir);
      std::filesystem::create_directory(db_dir);
      // options.key_hints.num_keys = 0;  // Needs to be empty to use bulk load.
      // if (num_keys <= 1) {
      //    // We set the step size to at least 1 to ensure any code that relies on
      //    // the step size to generate values does not end up in an infinite loop.
      //    options.key_hints.key_step_size = 1;
      // } else {
      //    // Set `key_step_size` to the smallest integer where
      //    // `min_key_ + key_step_size * (num_keys - 1) >= max_key_` holds.
      //    const size_t diff = max_key - min_key;
      //    const size_t denom = (num_keys - 1);
      //    options.key_hints.key_step_size =
      //     (diff / denom) + (diff % denom != 0);  // Computes ceil(diff/denom)
      // }

      std::cerr << "> TreeLine record cache capacity: "
                << options.record_cache_capacity << " records" << std::endl;
      // std::cerr << "> TreeLine buffer pool size: " << options.buffer_pool_size
      //           << " bytes" << std::endl;
      std::cerr << "> Opening TreeLine at " << dbname << std::endl;


      tl::Status status = tl::pg::PageGroupedDB::Open(options, dbname, &db_);
      if (!status.ok()) {
         throw std::runtime_error("Failed to start TreeLine: " +
                                 status.ToString());
      }
   }
    
   void evict_all() {
      //db_->FlushRecordCache(/*disable_deferred_io = */ true);
      db_->FlattenRange();
   }

   void clear_stats() {
      total_lookups = 0;
   }

   bool lookup(Key k, Payload& v) {
      ++total_lookups;
      ++lookups;
      uint64_t old_reads = 0, old_writes = 0;
      db_->GetIOStats(old_reads, old_writes);
      //const tl::key_utils::IntKeyAsSlice strkey(k);
      const tl::ReadOptions options;
      std::string value_out;
      //tl::Status status = db_->Get(options, strkey.as<tl::Slice>(), &value_out);
      tl::Status status = db_->Get(k, &value_out);
      assert(sizeof(v) == value_out.size());
      memcpy(&v, value_out.data(), value_out.size());
      uint64_t reads = 0, writes = 0;
      db_->GetIOStats(reads, writes);
      if (reads > old_reads) {
         stat.reads += reads - old_reads;
      }
      if (writes > old_writes) {
         stat.writes += writes - old_writes;
      }
      return status.ok();
   }

   void bulk_load(const std::vector<Key> & keys, const std::vector<Payload> & payloads)  {
      std::vector<tl::pg::Record> records;
      records.reserve(keys.size());
      for (int i = 0; i < keys.size(); ++i) {
         const tl::key_utils::IntKeyAsSlice strkey(keys[i]);
         records.emplace_back(keys[i], tl::Slice((const char *)&payloads[i], sizeof(Payload)));
      }
      tl::Status s = db_->BulkLoad(records);
      if (!s.ok()) {
         throw std::runtime_error("Failed to bulk load records!");
      }
   }

   void insert(Key k, Payload& v) {
      put(k, v);
   }

   void update(Key k, Payload& v) {
      Payload t;
      //read
      auto status __attribute__((unused)) = lookup(k, t);
      //modify
      memcpy(&t, &v, sizeof(v));
      //write
      put(k, t);
   }

   void put(Key k, Payload& v) {
      uint64_t old_reads = 0, old_writes = 0;
      db_->GetIOStats(old_reads, old_writes);
      tl::pg::WriteOptions options;
      options.is_update = true;
      //const tl::key_utils::IntKeyAsSlice strkey(k);
      //db_->Put(options, strkey.as<tl::Slice>(), tl::Slice((const char *)&v, sizeof(v)));
      auto status = db_->Put(options, k, tl::Slice((const char *)&v, sizeof(v)));
      if (!status.ok()) {
         throw std::runtime_error("Failed to put record!");
      }
      uint64_t reads = 0, writes = 0;
      db_->GetIOStats(reads, writes);
      if (reads > old_reads) {
         stat.reads += reads - old_reads;
      }
      if (writes > old_writes) {
         stat.writes += writes - old_writes;
      }
   }

   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]] int length) {
      //   virtual Status GetRange(const Key start_key, const size_t num_records,
      //                     std::vector<std::pair<Key, std::string>>* results_out,
      //                     bool use_experimental_prefetch = false) = 0;
      std::vector<std::pair<Key, std::string>> results;
      auto status = db_->GetRange(start_key, length, &results);
      if (!status.ok()) {
         throw std::runtime_error("Failed to GetRange!");
      }
      for (size_t i = 0; i < results.size(); ++i) {
         assert(results[i].second.size() == sizeof(Payload));
         if (processor(results[i].first, *(Payload*)(results[i].second.data()))) {
            break;
         }
      }
   }

   void report(u64, u64) override {
      tl::pg::PageGroupedDBStats::RunOnGlobal([](const auto& stats) {
         // clang-format off
         std::cout << "cache_hits," << stats.GetCacheHits() << std::endl;
         std::cout << "cache_misses," << stats.GetCacheMisses() << std::endl;
         std::cout << "cache_clean_evictions," << stats.GetCacheCleanEvictions() << std::endl;
         std::cout << "cache_dirty_evictions," << stats.GetCacheDirtyEvictions() << std::endl;

         std::cout << "overflows_created," << stats.GetOverflowsCreated() << std::endl;
         std::cout << "rewrites," << stats.GetRewrites() << std::endl;
         std::cout << "rewrite_input_pages," << stats.GetRewriteInputPages() << std::endl;
         std::cout << "rewrite_output_pages," << stats.GetRewriteOutputPages() << std::endl;

         std::cout << "segments," << stats.GetSegments() << std::endl;
         std::cout << "segment_index_bytes," << stats.GetSegmentIndexBytes() << std::endl;
         std::cout << "free_list_entries," << stats.GetFreeListEntries() << std::endl;
         std::cout << "free_list_bytes," << stats.GetFreeListBytes() << std::endl;
         std::cout << "cache_bytes," << stats.GetCacheBytes() << std::endl;

         std::cout << "overfetched_pages," << stats.GetOverfetchedPages() << std::endl;
         // clang-format on
      });
   }

   std::vector<std::string> stats_column_names() { return {"rc_hit_ratio", "io_r", "io_w"}; }
   std::vector<std::string> stats_columns() { 
      double record_cache_hit_ratio = 0;
      tl::pg::PageGroupedDBStats::RunOnGlobal([&](const auto& stats) {
      // clang-format off
         record_cache_hit_ratio = stats.GetCacheHits() / (stats.GetCacheHits() + stats.GetCacheMisses() + 0.0);
      });
      IOStats s;
      s.reads = stat.reads;
      s.writes = stat.writes;
      stat.Clear();
      return {std::to_string(record_cache_hit_ratio),std::to_string(s.reads),
              std::to_string(s.writes)}; 
   }
};