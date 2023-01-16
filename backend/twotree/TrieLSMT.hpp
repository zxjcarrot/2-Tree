#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "rocksdb/filter_policy.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
template <typename Key, typename Payload>
struct TrieRocksDBAdapter : public leanstore::StorageInterface<Key, Payload> {
   rocksdb::DB* bottom_db = nullptr;
   rocksdb::Options bottom_options;

   static constexpr double eviction_threshold = 0.99;
   std::size_t cache_capacity_bytes;
   std::size_t hit_count = 0;
   std::size_t miss_count = 0;
   std::size_t eviction_items = 0;
   std::size_t eviction_io_reads = 0;
   std::size_t io_reads_snapshot = 0;
   std::size_t io_reads_now = 0;
   typedef std::pair<Key, Payload> Element;
   struct TaggedPayload {
      Key key;
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };

   ARTIndex<Key, TaggedPayload> cache;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   bool inclusive;
   u64 lazy_migration_threshold = 10;
   TrieRocksDBAdapter(const std::string & db_dir, double toptree_cache_budget_gib, double bottomtree_cache_budget_gib, int lazy_migration_sampling_rate = false, bool inclusive = false) : cache_capacity_bytes(toptree_cache_budget_gib * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration_sampling_rate < 100), inclusive(inclusive) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();

      bottom_options.write_buffer_size = 8 * 1024 * 1024;
      std::cout << "RocksDB cache budget " << (bottomtree_cache_budget_gib) << " gib" << std::endl;
      std::cout << "RocksDB write_buffer_size " << bottom_options.write_buffer_size << std::endl;
      std::cout << "RocksDB max_write_buffer_number " << bottom_options.max_write_buffer_number << std::endl;
      std::size_t top_block_cache_size = (bottomtree_cache_budget_gib) * 1024ULL * 1024ULL * 1024ULL - bottom_options.write_buffer_size * bottom_options.max_write_buffer_number;
      std::cout << "RocksDB block cache size " << top_block_cache_size / 1024.0 /1024.0/1024 << " gib" << std::endl;
      mkdir(db_dir.c_str(), 0777);
      rocksdb::DestroyDB(db_dir,bottom_options);
      bottom_options.manual_wal_flush = false;
      bottom_options.use_direct_reads = true;
      bottom_options.create_if_missing = true;
      bottom_options.stats_dump_period_sec = 3000;
      bottom_options.compression = rocksdb::kNoCompression;
      bottom_options.use_direct_io_for_flush_and_compaction = true;
      bottom_options.sst_file_manager.reset(rocksdb::NewSstFileManager(rocksdb::Env::Default()));
      {
         rocksdb::BlockBasedTableOptions table_options;
         table_options.block_cache = rocksdb::NewLRUCache(top_block_cache_size, 0, true, 0);
         table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
         table_options.cache_index_and_filter_blocks = true;
         bottom_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
         rocksdb::Status status = rocksdb::DB::Open(bottom_options, db_dir, &bottom_db);
         assert(status.ok());
      }
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   inline size_t get_cache_size() {
      return cache.get_cache_size();
   }

   bool cache_under_pressure() {
      return get_cache_size() >= cache_capacity_bytes * eviction_threshold;
   }

   std::size_t lsmt_entries() {
      std::size_t num_entries = 0;
      bool r = bottom_db->GetIntProperty("rocksdb.estimate-num-keys", &num_entries);
      assert(r);
      return num_entries;
   }

   void evict_all() override {
      while (cache.size() > 200) {
         evict_one();
      }
   }

   void evict_one() {
      int cnt = 0;
      Key key = clock_hand;
      typename ARTIndex<Key, TaggedPayload>::Iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      while (it.end() == false) {
         ++cnt;
         it++;
         auto tagged_payload = it.value();
         if (tagged_payload == nullptr) {
            clock_hand = std::numeric_limits<Key>::max();
            break;
         }
         if (tagged_payload->referenced == true) {
            tagged_payload->referenced = false;
            clock_hand = it.key();
         } else {
            auto tmp = it;
            if (tmp.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               tmp++;
               clock_hand = tmp.key();
            }
            if (inclusive) {
               if (tagged_payload->modified) {
                  upsert_lsmt(it.key(), tagged_payload->payload);
               }
            } else { // exclusive, put it back in the on-disk LSM-tree
               insert_lsmt(it.key(), tagged_payload->payload);
            }
            eviction_items +=1;
            cache.erase(it.key());
            delete tagged_payload;
            break;
         }
      }
      auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
      assert(io_reads_new >= io_reads_old);
      eviction_io_reads += io_reads_new - io_reads_old;
      if (it.end()) {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }

   void evict_a_bunch() {
      static constexpr int kClockWalkSteps = 128;
      int steps = kClockWalkSteps;
      std::vector<Key> evict_keys;
      std::vector<TaggedPayload*> evict_payloads;
      bool victim_found = false;
      int cnt = 0;
      Key key = clock_hand;
      typename ARTIndex<Key, TaggedPayload>::Iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      while (it.end() == false) {
         ++cnt;
         it++;
         auto tagged_payload = it.value();
         if (tagged_payload == nullptr) {
            clock_hand = std::numeric_limits<Key>::max();
            break;
         }
         if (--steps == 0) {
            break;
         }
         clock_hand = it.key();

         if (tagged_payload->referenced == true) {
            tagged_payload->referenced = false;
         } else {
            evict_keys.push_back(it.key());
            evict_payloads.emplace_back(it.value());
            victim_found = true;
            if (evict_keys.size() >= 64) {
               break;
            }
         }
      }
      auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
      assert(io_reads_new >= io_reads_old);
      eviction_io_reads += io_reads_new - io_reads_old;

      if (victim_found) {
         io_reads_old = WorkerCounters::myCounters().io_reads.load();
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            cache.erase(key);
            if (inclusive) {
               if (tagged_payload->modified) {
                  upsert_lsmt(key, tagged_payload->payload);
               }
            } else { // exclusive, put it back in the on-disk LSM-tree
               insert_lsmt(key, tagged_payload->payload);
            }
            delete tagged_payload;
         }
         auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
         assert(io_reads_new >= io_reads_old);
         eviction_io_reads += io_reads_new - io_reads_old;
         eviction_items += evict_keys.size();
      } else {
         if (steps == kClockWalkSteps) {
            clock_hand = std::numeric_limits<Key>::max();
         }
      }
   }

   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_one();
      }
   }

   static constexpr int kEvictionCheckInterval = 50;
   int eviction_count_down = kEvictionCheckInterval;
   void try_eviction() {
      if (--eviction_count_down == 0) {
         if (cache_under_pressure()) {
            evict_a_bunch();
         }
         eviction_count_down = kEvictionCheckInterval;
      }
   }

   void admit_element(Key k, Payload & v, bool dirty = false) {
      try_eviction();
      assert(cache.exists(k) == false);
      cache.insert(k, new TaggedPayload{k, v, dirty, true});
      assert(cache.exists(k) == true);
   }

   bool lookup(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      TaggedPayload * v_ptr = nullptr;
      if (cache.find(k, v_ptr) == true) {
         v_ptr->referenced = true;
         v = v_ptr->payload;
         hit_count++;
         return true;
      }
      bool res = lookup_lsmt(k, v);
      if (res) {
         if (should_migrate()) {
            admit_element(k, v);
            if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
               delete_lsmt(k);
            }
         }
      }
      return res;
   }

   void upsert_lsmt(Key k, Payload& v)
   {
      insert_lsmt(k, v);
   }
   
   bool lookup_lsmt(Key k, Payload &v) {
      rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(Key)];
      leanstore::fold(key_bytes, k);
      std::string value;
      auto s = bottom_db->Get(options, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)), &value);
      if (s == rocksdb::Status::OK()) {
         v = *((Payload*)value.data());
         return true;
      }
      return false;
   }
   
   void insert_lsmt(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      rocksdb::WriteOptions options;
      options.disableWAL = false;
      leanstore::fold(key_bytes, k);
      auto s = bottom_db->Put(options, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)), rocksdb::Slice((const char *)&v, sizeof(Payload)));
      assert(s == rocksdb::Status::OK());
   }

   void delete_lsmt(Key k)
   {
      u8 key_bytes[sizeof(Key)];
      rocksdb::WriteOptions options;
      options.disableWAL = false;
      leanstore::fold(key_bytes, k);
      auto s = bottom_db->Delete(options, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)));
      assert(s == rocksdb::Status::OK());
   }

   void insert(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      if (inclusive) {
         admit_element(k, v, true);
      } else {
         admit_element(k, v);
      }
   }

   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void update(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();try_eviction();});
      TaggedPayload * v_ptr = nullptr;
      if (cache.find(k, v_ptr)) {
         v_ptr->referenced = true;
         v_ptr->modified = true;
         v_ptr->payload = v;
         hit_count++;
         return;
      }

      if (should_migrate()) {
         u8 key_bytes[sizeof(Key)];
         Payload old_v;
         bool res __attribute__((unused)) = lookup_lsmt(k, old_v);
         assert(res == true);
         admit_element(k, old_v);
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            delete_lsmt(k);
         }
         update(k, v);
      } else {
         upsert_lsmt(k, v);
      }
   }

   void put(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      TaggedPayload * v_ptr = nullptr;
      if (cache.find(k, v_ptr)) {
         v_ptr->referenced = true;
         v_ptr->modified = true;
         v_ptr->payload = v;
         hit_count++;
         return;
      }
      if (inclusive) {
         admit_element(k, v, true);
         return;
      }
      upsert_lsmt(k, v);
      if (should_migrate()) {
         admit_element(k, v);
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            delete_lsmt(k);
         }
      }
   }

   void report_cache() {
      std::cout << "Cache capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Cache size bytes " << get_cache_size() << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Cache hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Cache miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
   }

   void report(u64, u64 pages) override {
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      auto num_lsmt_entries = lsmt_entries();
      report_cache();
      std::cout << "RocksDB # entries~ " << num_lsmt_entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
   }
};

}