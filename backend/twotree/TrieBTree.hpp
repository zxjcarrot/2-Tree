#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "stx/btree_map.h"
#include "leanstore/BTreeAdapter.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
template <typename Key, typename Payload>
struct BTreeTrieCachedVSAdapter : BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;
   
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
   BTreeTrieCachedVSAdapter(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, bool lazy_migration = false, bool inclusive = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration), inclusive(inclusive) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
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

   std::size_t btree_entries() {
      Key start_key = std::numeric_limits<Key>::min();
      u8 key_bytes[sizeof(Key)];
      std::size_t entries = 0;
      while (true) {
         bool good = false;
         btree.scanAsc(key_bytes, fold(key_bytes, start_key),
         [&](const u8 * key, u16, const u8 *, u16) -> bool {
            auto real_key = unfold(*(Key*)(key));
            good = true;
            start_key = real_key + 1;
            return false;
         }, [](){});
         if (good == false) {
            break;
         }
         entries++;
      }
      return entries;
   }

   void evict_all() override {
      while (cache.size() > 100) {
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
                  upsert_btree(it.key(), tagged_payload->payload);
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_btree(it.key(), tagged_payload->payload);
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

   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_one();
      }
   }

   void admit_element(Key k, Payload & v, bool dirty = false) {
      if (cache_under_pressure())
         evict_till_safe();
      assert(cache.exists(k) == false);
      cache.insert(k, new TaggedPayload{k, v, dirty, true});
      assert(cache.exists(k) == true);
   }

   bool lookup(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      TaggedPayload * v_ptr = nullptr;
      if (cache.find(k, v_ptr) == true) {
         v_ptr->referenced = true;
         v = v_ptr->payload;
         hit_count++;
         return true;
      }
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
            OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (res) {
         if (should_migrate()) {
            admit_element(k, v);
            if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
               old_miss = WorkerCounters::myCounters().io_reads.load();
               auto op_res __attribute__((unused)) = btree.remove(key_bytes, fold(key_bytes, k));
               new_miss = WorkerCounters::myCounters().io_reads.load();
               assert(op_res == OP_RESULT::OK);
               if (old_miss != new_miss) {
                  miss_count += new_miss - old_miss;
               }
            }
         }
      }
      return res;
   }

   void upsert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(op_res == OP_RESULT::OK);
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (op_res == OP_RESULT::NOT_FOUND) {
         insert_btree(k, v);
      }
   }

   void insert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res = btree.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(op_res == OP_RESULT::OK);
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
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
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
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
         auto old_miss = WorkerCounters::myCounters().io_reads.load();
         bool res __attribute__((unused)) = btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&old_v, payload, payload_length); }) ==
               OP_RESULT::OK;
         assert(res == true);
         auto new_miss = WorkerCounters::myCounters().io_reads.load();
         if (old_miss != new_miss) {
            miss_count += new_miss - old_miss;
         }
         admit_element(k, old_v);
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            old_miss = WorkerCounters::myCounters().io_reads.load();
            auto op_res __attribute__((unused)) = btree.remove(key_bytes, fold(key_bytes, k));
            new_miss = WorkerCounters::myCounters().io_reads.load();
            if (old_miss != new_miss) {
               miss_count += new_miss - old_miss;
            }
            assert(op_res == OP_RESULT::OK);
         }
         update(k, v);
      } else {
         u8 key_bytes[sizeof(Key)];
         auto old_miss = WorkerCounters::myCounters().io_reads.load();
         auto op_res __attribute__((unused))= btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
         auto new_miss = WorkerCounters::myCounters().io_reads.load();
         if (old_miss != new_miss) {
            miss_count += new_miss - old_miss;
         }
         assert(op_res == OP_RESULT::OK);
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
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res __attribute__((unused)) = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      assert(op_res == OP_RESULT::OK);
      if (should_migrate()) {
         admit_element(k, v);
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            old_miss = WorkerCounters::myCounters().io_reads.load();
            op_res = btree.remove(key_bytes, fold(key_bytes, k));
            new_miss = WorkerCounters::myCounters().io_reads.load();
            assert(op_res == OP_RESULT::OK);
            if (old_miss != new_miss) {
               miss_count += new_miss - old_miss;
            }
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
      auto num_btree_entries = btree_entries();
      report_cache();
      std::cout << "BTree # entries " << num_btree_entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "BTree height " << btree.getHeight() << std::endl;
      auto minimal_pages = num_btree_entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " << minimal_pages / (pages + 0.0) << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
   }
};

}