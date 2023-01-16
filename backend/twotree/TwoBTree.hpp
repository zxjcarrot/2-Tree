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
struct BTreeCachedNoninlineVSAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;
   
   static constexpr double eviction_threshold = 0.99;
   std::size_t cache_capacity_bytes;
   std::size_t hit_count = 0;
   std::size_t miss_count = 0;
   std::size_t eviction_items = 0;
   std::size_t eviction_io_reads = 0;
   std::size_t io_reads_snapshot = 0;
   std::size_t io_reads_now = 0;
   struct TaggedPayload {
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };
   typedef std::pair<Key, TaggedPayload*> Element;

   stx::btree_map<Key, TaggedPayload*> cache;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   bool inclusive;
   u64 lazy_migration_threshold = 10;
   BTreeCachedNoninlineVSAdapter(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, bool lazy_migration = false, bool inclusive = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration), inclusive(inclusive) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   inline size_t get_cache_btree_size() {
      auto & stat = cache.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size() + sizeof(TaggedPayload) * stat.itemcount;
   }

   bool cache_under_pressure() {
      return get_cache_btree_size() >= cache_capacity_bytes * eviction_threshold;
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

   void evict_one() {
      Key key = clock_hand;
      typename stx::btree_map<Key, TaggedPayload*>::iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      while (it != cache.end()) {
         if (it.data()->referenced == true) {
            it.data()->referenced = false;
            clock_hand = it.key();
            ++it;
         } else {
            auto tmp = it;
            ++tmp;
            if (tmp == cache.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               clock_hand = it.key();
            }
            if (inclusive) {
               if (it.data()->modified) {
                  upsert_btree(it.key(), it.data()->payload);
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_btree(it.key(), it.data()->payload);
            }
            eviction_items += 1;
            delete it.data();
            cache.erase(it);
            break;
         }
      }
      auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
      assert(io_reads_new >= io_reads_old);
      eviction_io_reads += io_reads_new - io_reads_old;
      if (it == cache.end()) {
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
      assert(cache.find(k) == cache.end());
      cache[k] = new TaggedPayload{v, dirty, true};
   }

   bool lookup(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data()->referenced = true;
         v = it.data()->payload;
         hit_count++;
         return true;
      }
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
            OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (res) {
         if (should_migrate()) {
            admit_element(k, v);
            if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
               old_miss = WorkerCounters::myCounters().io_reads.load();
               btree.remove(key_bytes, fold(key_bytes, k));
               new_miss = WorkerCounters::myCounters().io_reads.load();
               assert(new_miss >= old_miss);
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
      assert(op_res == OP_RESULT::OK);
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
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
      assert(op_res == OP_RESULT::OK);
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
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
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data()->referenced = true;
         it.data()->modified = true;
         it.data()->payload = v;
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
         auto op_res __attribute__((unused)) = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
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
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data()->referenced = true;
         it.data()->modified = true;
         it.data()->payload = v;
         hit_count++;
         return;
      }
      if (inclusive) { // Not found in cache and inclusive cache, put a dirty payload in cache
         admit_element(k, v, true);
         return;
      }
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res __attribute__((unused)) = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      assert(op_res == OP_RESULT::OK);
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (should_migrate()) {
         admit_element(k, v);
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            old_miss = WorkerCounters::myCounters().io_reads.load();
            op_res = btree.remove(key_bytes, fold(key_bytes, k));
            assert(op_res == OP_RESULT::OK);
            new_miss = WorkerCounters::myCounters().io_reads.load();
            if (old_miss != new_miss) {
               miss_count += new_miss - old_miss;
            }
         }
      }
   }

   void report(u64 entries __attribute__((unused)) , u64 pages) override {
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      auto num_btree_entries = btree_entries();
      std::cout << "Cache capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Cache size bytes " << get_cache_btree_size() << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "Cache BTree average leaf fill factor " << cache.get_stats().avgfill_leaves() << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Cache hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Cache miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "BTree height " << btree.getHeight() << std::endl;
      std::cout << "BTree # entries " << num_btree_entries << std::endl;
      auto minimal_pages = num_btree_entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " << minimal_pages / (pages + 0.0) << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
   }
};


template <typename Key, typename Payload, int NodeSize = 1024>
struct BTreeCachedVSAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;
   
   static constexpr double eviction_threshold = 0.99;
   std::size_t cache_capacity_bytes;
   std::size_t hit_count = 0;
   std::size_t miss_count = 0;
   std::size_t eviction_items = 0;
   std::size_t eviction_io_reads = 0;
   std::size_t io_reads_snapshot = 0;
   std::size_t io_reads_now = 0;
   std::size_t scan_ops = 0;
   std::size_t io_reads_scan = 0;
   typedef std::pair<Key, Payload> Element;
   struct TaggedPayload {
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };

   class my_stx_btree_map_traits {
      public:
      /// If true, the tree will self verify it's invariants after each insert()
      /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
      static const bool selfverify = false;

      /// If true, the tree will print out debug information and a tree dump
      /// during insert() or erase() operation. The header must have been
      /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
      /// printable.
      static const bool debug = false;
      
      static const size_t node_size = NodeSize;
      /// Number of slots in each leaf of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int leafslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(TaggedPayload)));

      /// Number of slots in each inner node of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int innerslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(void*)));

      /// As of stx-btree-0.9, the code does linear search in find_lower() and
      /// find_upper() instead of binary_search, unless the node size is larger
      /// than this threshold. See notes at
      /// http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
      static const size_t binsearch_threshold = 256;
   };

   typedef stx::btree_map<Key, TaggedPayload, std::less<Key>, my_stx_btree_map_traits> cache_type;
   cache_type cache;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   bool inclusive;
   u64 lazy_migration_threshold = 10;
   BTreeCachedVSAdapter(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, int lazy_migration_sampling_rate = 100, bool inclusive = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration_sampling_rate < 100), inclusive(inclusive) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      scan_ops = io_reads_scan = 0;
   }

   void evict_all() override {
      while (cache.size() > 0) {
         evict_one();
      }
   }

   inline size_t get_cache_btree_size() {
      auto & stat = cache.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size();
   }

   bool cache_under_pressure() {
      return get_cache_btree_size() >= cache_capacity_bytes * eviction_threshold;
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

   void evict_one() {
      Key key = clock_hand;
      typename cache_type::iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      while (it != cache.end()) {
         if (it.data().referenced == true) {
            it.data().referenced = false;
            clock_hand = it.key();
            ++it;
         } else {
            auto tmp = it;
            ++tmp;
            if (tmp == cache.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               clock_hand = tmp.key();
            }
            if (inclusive) {
               if (it.data().modified) {
                  upsert_btree(it.key(), it.data().payload);
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_btree(it.key(), it.data().payload);
            }
            eviction_items +=1;
            cache.erase(it);
            break;
         }
      }
      auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
      assert(io_reads_new >= io_reads_old);
      eviction_io_reads += io_reads_new - io_reads_old;
      if (it == cache.end()) {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }
   static constexpr int kClockWalkSteps = 128;
   void evict_a_bunch() {
      int steps = kClockWalkSteps;
      Key key = clock_hand;
      typename cache_type::iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }

      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      bool victim_found = false;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      while (it != cache.end()) {
         if (--steps == 0) {
            break;
         }
         if (it.data().referenced == true) {
            it.data().referenced = false;
            clock_hand = it.key();
            ++it;
         } else {
            auto tmp = it;
            ++tmp;
            if (tmp == cache.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               clock_hand = tmp.key();
            }

            evict_keys.push_back(it.key());
            evict_payloads.emplace_back(it.data());
            victim_found = true;
            if (evict_keys.size() >= 64) {
               break;
            }
            it = tmp;
         }
      }
      auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
      assert(io_reads_new >= io_reads_old);
      eviction_io_reads += io_reads_new - io_reads_old;
      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            cache.erase(key);
            if (inclusive) {
               if (tagged_payload.modified) {
                  upsert_btree(key, tagged_payload.payload);
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_btree(key, tagged_payload.payload);
            }
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
   int eviction_check_countdown = kEvictionCheckInterval;
   void try_eviction() {
      if (--eviction_check_countdown == 0) {
         if (cache_under_pressure())
            evict_a_bunch();
         eviction_check_countdown = kEvictionCheckInterval;
      }
   }
   void admit_element(Key k, Payload & v, bool dirty = false) {
      if (cache_under_pressure())
         evict_a_bunch();
      assert(cache.find(k) == cache.end());
      cache[k] = TaggedPayload{v, dirty, true};
   }

   bool lookup(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data().referenced = true;
         v = it.data().payload;
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


   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, int length) {
      ++scan_ops;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      constexpr std::size_t scan_buffer_size = 32;
      u8 key_bytes[sizeof(Key)];
      int hot_len = 0;
      Key hot_keys[scan_buffer_size];
      Payload hot_payloads[scan_buffer_size];
      int cold_len = 0;
      Key cold_keys[scan_buffer_size];
      Payload cold_payloads[scan_buffer_size];
      bool hot_tree_end = false;
      bool cold_tree_end = false;
      auto fill_hot_scan_buffer = [&](Key startk) {
         if (hot_len < scan_buffer_size && hot_tree_end == false) {
            auto it = cache.lower_bound(startk);
            while (it != cache.end()) {
               hot_keys[hot_len] = it.key();
               hot_payloads[hot_len] = it.data().payload;
               ++hot_len;
               if (hot_len >= scan_buffer_size) {
                  break;;
               }
               ++it;
            }
            if (hot_len < scan_buffer_size) {
               hot_tree_end = true;
            }
         }
      };
      auto fill_cold_scan_buffer = [&](Key startk) {
         if (cold_len < scan_buffer_size && cold_tree_end == false) {
            btree.scanAsc(key_bytes, fold(key_bytes, startk),
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
               auto real_key = unfold(*(Key*)(key));
               assert(key_length == sizeof(Key));
               assert(value_length == sizeof(Payload));
               const Payload * p = reinterpret_cast<const Payload*>(value);
               cold_keys[cold_len] = real_key;
               cold_payloads[cold_len] = *p;
               cold_len++;
               if (cold_len >= scan_buffer_size) {
                  return false;
               }
               return true;
            }, [](){});
            if (cold_len < scan_buffer_size) {
               cold_tree_end = true;
            }
         }
      };

      int hot_idx = 0;
      int cold_idx = 0;
      fill_hot_scan_buffer(start_key);
      fill_cold_scan_buffer(start_key);
      while (true) {
         while (hot_idx < hot_len && cold_idx < cold_len) {
            if (hot_keys[hot_idx] <= cold_keys[cold_idx]) {
               if (processor(hot_keys[hot_idx], hot_payloads[hot_idx])) {
                  goto end;
               }
               hot_idx++;
            } else {
               if (processor(cold_keys[cold_idx], cold_payloads[cold_idx])) {
                  goto end;
               }
               cold_idx++;
            }
         }

         if (hot_idx < hot_len && cold_idx == cold_len && cold_tree_end) { // 
            while (hot_idx < hot_len) {
               if (processor(hot_keys[hot_idx], hot_payloads[hot_idx])) {
                  goto end;
               }
               hot_idx++;
            }
         }

         if (cold_idx < cold_len && hot_idx == hot_len && hot_tree_end) {
            // reached the end of 
            while (cold_idx < cold_len) {
               if (processor(cold_keys[cold_idx], cold_payloads[cold_idx])) {
                  goto end;
               }
               cold_idx++;
            }
         }

         if (hot_idx >= hot_len && hot_tree_end == false) { // try to refill hot scan buffer
            assert(hot_idx > 0);
            auto hot_start_key = hot_keys[hot_idx - 1] + 1;
            hot_idx = 0;
            hot_len = 0;
            fill_hot_scan_buffer(hot_start_key);
         }

         if (cold_idx >= cold_len && cold_tree_end == false) { // try to refill cold scan buffer
            assert(cold_idx > 0);
            auto cold_start_key = cold_keys[cold_idx - 1] + 1;
            cold_idx = 0;
            cold_len = 0;
            fill_cold_scan_buffer(cold_start_key);
         }

         if (cold_idx >= cold_len && cold_tree_end && hot_idx >= hot_len && hot_tree_end) {
            goto end;
         }
      }

   end:
      io_reads_scan += WorkerCounters::myCounters().io_reads.load() - io_reads_old;
      return;
   }

   void upsert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
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
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data().referenced = true;
         it.data().modified = true;
         it.data().payload = v;
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
         auto op_res __attribute__((unused)) = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length __attribute__((unused)) ) { memcpy(payload, &v, payload_length); });
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
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data().referenced = true;
         it.data().modified = true;
         it.data().payload = v;
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
      std::cout << "Cache size bytes " << get_cache_btree_size() << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "Cache BTree average leaf fill factor " << cache.get_stats().avgfill_leaves() << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Cache hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Cache miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
   }

   void report(u64 entries __attribute__((unused)) , u64 pages) override {
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
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01);
   }
};


template <typename Key, typename Payload, int NodeSize = 1024>
struct BTreeCachedCompressedVSAdapter : StorageInterface<Key, Payload> {
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
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };

   class my_stx_btree_map_traits {
      public:
      /// If true, the tree will self verify it's invariants after each insert()
      /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
      static const bool selfverify = false;

      /// If true, the tree will print out debug information and a tree dump
      /// during insert() or erase() operation. The header must have been
      /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
      /// printable.
      static const bool debug = false;
      
      static const size_t node_size = NodeSize;
      /// Number of slots in each leaf of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int leafslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(TaggedPayload)));

      /// Number of slots in each inner node of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int innerslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(void*)));

      /// As of stx-btree-0.9, the code does linear search in find_lower() and
      /// find_upper() instead of binary_search, unless the node size is larger
      /// than this threshold. See notes at
      /// http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
      static const size_t binsearch_threshold = 256;
   };

   typedef stx::compressed_btree_map<Key, TaggedPayload, std::less<Key>, my_stx_btree_map_traits> cache_type;
   //typedef stx::btree_map<Key, TaggedPayload, std::less<Key>, my_stx_btree_map_traits> cache_type;
   cache_type cache;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   bool inclusive;
   u64 lazy_migration_threshold = 10;
   BTreeCachedCompressedVSAdapter(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, bool lazy_migration = false, bool inclusive = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration), inclusive(inclusive) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void evict_all() override {
      while (cache.size() > 0) {
         evict_one();
      }
   }

   inline size_t get_cache_btree_size() {
      // auto & stat = cache.get_stats();
      // auto leaves = stat.leaves;
      // auto innernodes = stat.innernodes;
      // return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size();
      return cache.get_stats().tree_total_bytes();
   }

   bool cache_under_pressure() {
      return get_cache_btree_size() >= cache_capacity_bytes * eviction_threshold;
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

   void evict_one() {
      Key key = clock_hand;
      typename cache_type::iterator it;
      // DeferCode c([&](){
      //    it.ensure_node_compressed();
      // });
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      while (it != cache.end()) {
         if (it.data().referenced == true) {
            it.data().referenced = false;
            clock_hand = it.key();
            ++it;
         } else {
            auto tmp = it;
            ++tmp;
            if (tmp == cache.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               clock_hand = tmp.key();
            }
            if (inclusive) {
               if (it.data().modified) {
                  upsert_btree(it.key(), it.data().payload);
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_btree(it.key(), it.data().payload);
            }
            eviction_items +=1;
            cache.erase(it);
            break;
         }
      }
      auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
      assert(io_reads_new >= io_reads_old);
      eviction_io_reads += io_reads_new - io_reads_old;
      if (it == cache.end()) {
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
      assert(cache.find(k) == cache.end());
      cache[k] = TaggedPayload{v, dirty, true};
   }

   bool lookup(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      auto it = cache.find(k);
      DeferCode c2([&it, this](){ it.ensure_node_compressed();});
      if (it != cache.end()) {
         it.data().referenced = true;
         v = it.data().payload;
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
      auto it = cache.find(k);
      DeferCode c2([&it, this](){ it.ensure_node_compressed();});
      if (it != cache.end()) {
         it.data().referenced = true;
         it.data().modified = true;
         it.data().payload = v;
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
         auto op_res __attribute__((unused)) = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length __attribute__((unused)) ) { memcpy(payload, &v, payload_length); });
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
      auto it = cache.find(k);
      DeferCode c2([&it, this](){ it.ensure_node_compressed();});
      if (it != cache.end()) {
         it.data().referenced = true;
         it.data().modified = true;
         it.data().payload = v;
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
      std::cout << "\nCache capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Cache compressed size bytes " << get_cache_btree_size() << std::endl;
      std::cout << "Cache uncompressed size bytes " << cache.get_stats().uncompressed_tree_total_bytes() << std::endl;
      std::cout << "Cache # Uncompressed/Compressed leaves " << cache.get_stats().uncompressed_leaves << "/" << cache.get_stats().compressed_leaves << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "Cache BTree average leaf fill factor " << cache.get_stats().avgfill_leaves() << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Cache hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Cache miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
   }

   void report(u64 entries __attribute__((unused)) , u64 pages) override {
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





#define HIT_STAT_START \
auto old_miss = WorkerCounters::myCounters().io_reads.load();

#define HIT_STAT_END \
      auto new_miss = WorkerCounters::myCounters().io_reads.load(); \
      assert(new_miss >= old_miss); \
      if (old_miss == new_miss) { \
         btree_buffer_hit++; \
      } else { \
         btree_buffer_miss += new_miss - old_miss; \
      }

#define OLD_HIT_STAT_START \
   old_miss = WorkerCounters::myCounters().io_reads.load();

#define OLD_HIT_STAT_END \
      new_miss = WorkerCounters::myCounters().io_reads.load(); \
      assert(new_miss >= old_miss); \
      if (old_miss == new_miss) { \
         btree_buffer_hit++; \
      } else { \
         btree_buffer_miss += new_miss - old_miss; \
      }

template <typename Key, typename Payload>
struct TwoBTreeAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& hot_btree;
   leanstore::storage::btree::BTreeInterface& cold_btree;
   uint64_t hot_tree_ios = 0;
   uint64_t btree_buffer_miss = 0;
   uint64_t btree_buffer_hit = 0;
   DTID dt_id;
   std::size_t hot_partition_capacity_bytes;
   std::size_t hot_partition_size_bytes = 0;
   std::size_t scan_ops = 0;
   std::size_t io_reads_scan = 0;
   bool inclusive = false;
   static constexpr double eviction_threshold = 0.7;
   uint64_t eviction_items = 0;
   uint64_t eviction_io_reads= 0;
   uint64_t io_reads_snapshot = 0;
   uint64_t io_reads_now = 0;
   uint64_t hot_partition_item_count = 0;
   std::size_t upward_migrations = 0;
   std::size_t downward_migrations = 0;
   u64 lazy_migration_threshold = 10;
   bool lazy_migration = false;
   struct alignas(1) TaggedPayload {
      Payload payload;
      unsigned char bits = 0;
      bool modified() const { return bits & 1; }
      void set_modified() { bits |= 1; }
      void clear_modified() { bits &= ~(1u); }
      bool referenced() const { return bits & (2u); }
      void set_referenced() { bits |= (2u); }
      void clear_referenced() { bits &= ~(2u); }
   };
   static_assert(sizeof(TaggedPayload) == sizeof(Payload) + 1, "!!!");
   uint64_t total_lookups = 0;
   uint64_t lookups_hit_top = 0;
   Key clock_hand = std::numeric_limits<Key>::max();
   constexpr static u64 PartitionBitPosition = 63;
   constexpr static u64 PartitionBitMask = 0x8000000000000000;
   Key tag_with_hot_bit(Key key) {
      return key;
   }
   
   bool is_in_hot_partition(Key key) {
      return (key & PartitionBitMask) == 0;
   }

   Key tag_with_cold_bit(Key key) {
      return key;
   }

   Key strip_off_partition_bit(Key key) {
      return key;
   }

   TwoBTreeAdapter(leanstore::storage::btree::BTreeInterface& hot_btree, leanstore::storage::btree::BTreeInterface& cold_btree, double hot_partition_size_gb, bool inclusive = false, int lazy_migration_sampling_rate = 100) : hot_btree(hot_btree), cold_btree(cold_btree), hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL), inclusive(inclusive), lazy_migration(lazy_migration_sampling_rate < 100) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   bool cache_under_pressure() {
      return hot_partition_size_bytes >= hot_partition_capacity_bytes * eviction_threshold;
   }

   void clear_stats() override {
      btree_buffer_miss = btree_buffer_hit = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      lookups_hit_top = total_lookups = 0;
      hot_tree_ios = 0;
      upward_migrations = downward_migrations = 0;
      io_reads_scan = 0;
      scan_ops = 0;
   }

   std::size_t btree_entries(leanstore::storage::btree::BTreeInterface& btree, std::size_t & pages) {
      constexpr std::size_t scan_buffer_cap = 64;
      int scan_buffer_len = 0;
      Key keys[scan_buffer_cap];
      Payload payloads[scan_buffer_cap];
      bool tree_end = false;
      pages = 0;
      const char * last_leaf_frame = nullptr;
      u8 key_bytes[sizeof(Key)];
      auto fill_scan_buffer = [&](Key startk) {
         if (scan_buffer_len < scan_buffer_cap && tree_end == false) {
            btree.scanAsc(key_bytes, fold(key_bytes, startk),
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length, const char * leaf_frame) -> bool {
               auto real_key = unfold(*(Key*)(key));
               assert(key_length == sizeof(Key));
               keys[scan_buffer_len] = real_key;
               scan_buffer_len++;
               if (last_leaf_frame != leaf_frame) {
                  last_leaf_frame = leaf_frame;
                  ++pages;
               }
               if (scan_buffer_len >= scan_buffer_cap) {
                  return false;
               }
               return true;
            }, [](){});
            if (scan_buffer_len < scan_buffer_cap) {
               tree_end = true;
            }
         }
      };
      Key start_key = std::numeric_limits<Key>::min();
      
      std::size_t entries = 0;
      
      while (true) {
         fill_scan_buffer(start_key);
         int idx = 0;
         while (idx < scan_buffer_len) {
            idx++;
            entries++;
         }

         if (idx >= scan_buffer_len && tree_end) {
            break;
         }
         assert(idx > 0);
         start_key = keys[idx - 1] + 1;
         scan_buffer_len = 0;
      }
      return entries;
   }

   void evict_all() override {
      while (hot_partition_item_count > 0) {
         evict_a_bunch();
      }
   }

   static constexpr int kClockWalkSteps = 4096;
   void evict_a_bunch() {
      int steps = kClockWalkSteps; // number of steps to walk
      Key start_key = clock_hand;
      if (start_key == std::numeric_limits<Key>::max()) {
         start_key = tag_with_hot_bit(std::numeric_limits<Key>::min());
      }

      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      Key evict_key;
      Payload evict_payload;
      bool victim_found = false;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      hot_btree.scanAsc(key_bytes, fold(key_bytes, start_key),
      [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
         if (--steps == 0) {
            return false;
         }
         auto real_key = unfold(*(Key*)(key));
         assert(key_length == sizeof(Key));
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
         assert(value_length == sizeof(TaggedPayload));
         if (tp->referenced() == true) {
            tp->clear_referenced();
            return true;
         }
         evict_key = real_key;
         clock_hand = real_key;
         evict_payload = tp->payload;
         victim_found = true;
         evict_keys.push_back(real_key);
         evict_payloads.emplace_back(*tp);
         if (evict_keys.size() >= 2048) {
            return false;
         }
         return true;
      }, [](){});
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }

      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            assert(is_in_hot_partition(key));
            // auto op_res = hot_btree.remove(key_bytes, fold(key_bytes, key));
            // hot_partition_item_count--;
            // assert(op_res == OP_RESULT::OK);
            // hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
            if (inclusive) {
               if (tagged_payload.modified()) {
                  upsert_cold_partition(strip_off_partition_bit(key), tagged_payload.payload); // Cold partition
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_partition(strip_off_partition_bit(key), tagged_payload.payload, true); // Cold partition
            }
            ++downward_migrations;
         }
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            assert(is_in_hot_partition(key));
            auto op_res = hot_btree.remove(key_bytes, fold(key_bytes, key));
            hot_partition_item_count--;
            assert(op_res == OP_RESULT::OK);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
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
         evict_a_bunch();
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

   void admit_element(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      try_eviction();
      insert_partition(strip_off_partition_bit(k), v, false, dirty, referenced); // Hot partition
      hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
   }
   
   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, int length) {
      scan_ops++;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      constexpr std::size_t scan_buffer_size = 8;
      u8 key_bytes[sizeof(Key)];
      int hot_len = 0;
      Key hot_keys[scan_buffer_size];
      Payload hot_payloads[scan_buffer_size];
      int cold_len = 0;
      Key cold_keys[scan_buffer_size];
      Payload cold_payloads[scan_buffer_size];
      bool hot_tree_end = false;
      bool cold_tree_end = false;
      auto fill_hot_scan_buffer = [&](Key startk) {
         if (hot_len < scan_buffer_size && hot_tree_end == false) {
            hot_btree.scanAsc(key_bytes, fold(key_bytes, startk),
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
               auto real_key = unfold(*(Key*)(key));
               assert(key_length == sizeof(Key));
               assert(value_length == sizeof(TaggedPayload));
               const TaggedPayload * p = reinterpret_cast<const TaggedPayload*>(value);
               hot_keys[hot_len] = real_key;
               hot_payloads[hot_len] = p->payload;
               hot_len++;
               if (hot_len >= scan_buffer_size) {
                  return false;
               }
               return true;
            }, [](){});
            if (hot_len < scan_buffer_size) {
               hot_tree_end = true;
            }
         }
      };
      auto fill_cold_scan_buffer = [&](Key startk) {
         if (cold_len < scan_buffer_size && cold_tree_end == false) {
            cold_btree.scanAsc(key_bytes, fold(key_bytes, startk),
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
               auto real_key = unfold(*(Key*)(key));
               assert(key_length == sizeof(Key));
               assert(value_length == sizeof(Payload));
               const Payload * p = reinterpret_cast<const Payload*>(value);
               cold_keys[cold_len] = real_key;
               cold_payloads[cold_len] = *p;
               cold_len++;
               if (cold_len >= scan_buffer_size) {
                  return false;
               }
               return true;
            }, [](){});
            if (cold_len < scan_buffer_size) {
               cold_tree_end = true;
            }
         }
      };

      int hot_idx = 0;
      int cold_idx = 0;
      fill_hot_scan_buffer(start_key);
      fill_cold_scan_buffer(start_key);
      while (true) {
         while (hot_idx < hot_len && cold_idx < cold_len) {
            if (hot_keys[hot_idx] <= cold_keys[cold_idx]) {
               if (processor(hot_keys[hot_idx], hot_payloads[hot_idx])) {
                  goto end;
               }
               hot_idx++;
            } else {
               if (processor(cold_keys[cold_idx], cold_payloads[cold_idx])) {
                  goto end;
               }
               cold_idx++;
            }
         }

         if (hot_idx < hot_len && cold_idx == cold_len && cold_tree_end) { // 
            while (hot_idx < hot_len) {
               if (processor(hot_keys[hot_idx], hot_payloads[hot_idx])) {
                  goto end;
               }
               hot_idx++;
            }
         }

         if (cold_idx < cold_len && hot_idx == hot_len && hot_tree_end) {
            // reached the end of 
            while (cold_idx < cold_len) {
               if (processor(cold_keys[cold_idx], cold_payloads[cold_idx])) {
                  goto end;
               }
               cold_idx++;
            }
         }

         if (hot_idx >= hot_len && hot_tree_end == false) { // try to refill hot scan buffer
            assert(hot_idx > 0);
            auto hot_start_key = hot_keys[hot_idx - 1] + 1;
            hot_idx = 0;
            hot_len = 0;
            fill_hot_scan_buffer(hot_start_key);
         }

         if (cold_idx >= cold_len && cold_tree_end == false) { // try to refill cold scan buffer
            assert(cold_idx > 0);
            auto cold_start_key = cold_keys[cold_idx - 1] + 1;
            cold_idx = 0;
            cold_len = 0;
            fill_cold_scan_buffer(cold_start_key);
         }

         if (cold_idx >= cold_len && cold_tree_end && hot_idx >= hot_len && hot_tree_end) {
            goto end;
         }
      }

   end:
      io_reads_scan += WorkerCounters::myCounters().io_reads.load() - io_reads_old;
      return;
   }

   bool lookup(Key k, Payload& v) override
   {
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      // try hot partition first
      auto hot_key = tag_with_hot_bit(k);
      uint64_t old_miss, new_miss;
      OLD_HIT_STAT_START;
      auto res = hot_btree.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         tp->set_referenced();
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;
      OLD_HIT_STAT_END;
      hot_tree_ios += new_miss - old_miss;
      if (res) {
         ++lookups_hit_top;
         return res;
      }

      auto cold_key = tag_with_cold_bit(k);
      res = cold_btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;

      if (res) {
         if (should_migrate()) {
            ++upward_migrations;
            if (inclusive == false) {
               //OLD_HIT_STAT_START;
               // remove from the cold partition
               auto op_res __attribute__((unused))= cold_btree.remove(key_bytes, fold(key_bytes, cold_key));
               assert(op_res == OP_RESULT::OK);
               //OLD_HIT_STAT_END
            }
            // move to hot partition
            OLD_HIT_STAT_START;
            admit_element(k, v);
            OLD_HIT_STAT_END;
            hot_tree_ios += new_miss - old_miss;
         }
      }
      return res;
   }

   void upsert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);
      TaggedPayload tp;
      tp.set_modified();
      tp.set_referenced();
      tp.payload = v;
      //HIT_STAT_START;
      // auto op_res = cold_btree.updateSameSize(key_bytes, fold(key_bytes, key), [&](u8* payload, u16 payload_length) {
      //    TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
      //    tp->set_referenced();
      //    memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      // }, Payload::wal_update_generator);
      //HIT_STAT_END;

      auto op_res = cold_btree.upsert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      assert(op_res == OP_RESULT::OK);
      // if (op_res == OP_RESULT::NOT_FOUND) {
      //    insert_partition(k, v, true);
      // }
   }

   // hot_or_cold: false => hot partition, true => cold partition
   void insert_partition(Key k, Payload & v, bool hot_or_cold, bool modified = false, bool referenced = true) {
      u8 key_bytes[sizeof(Key)];
      Key key = hot_or_cold == false ? tag_with_hot_bit(k) : tag_with_cold_bit(k);
      if (hot_or_cold == false) {
         hot_partition_item_count++;
      }
      TaggedPayload tp;
      if (referenced == false) {
         tp.clear_referenced();
      } else {
         tp.set_referenced();
      }

      if (modified) {
         tp.set_modified();
      } else {
         tp.clear_modified();
      }
      tp.payload = v;
      
      if (hot_or_cold == false) {
         HIT_STAT_START;
         auto op_res = hot_btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
         assert(op_res == OP_RESULT::OK);
         HIT_STAT_END;
      } else {
         auto op_res = cold_btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
         assert(op_res == OP_RESULT::OK);
      }
   }

   void insert(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      //admit_element(k, v);
      if (inclusive) {
         admit_element(k, v, true, false);
      } else {
         admit_element(k, v);
      }
   }

   void update(Key k, Payload& v) override {
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = hot_btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->set_referenced();
         tp->set_modified();
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator);
      HIT_STAT_END;
      hot_tree_ios += new_miss - old_miss;
      if (op_res == OP_RESULT::OK) {
         ++lookups_hit_top;
         return;
      }

      Payload old_v;
      auto cold_key = tag_with_cold_bit(k);
      //OLD_HIT_STAT_START;
      auto res __attribute__((unused)) = cold_btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&old_v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;
      assert(res);
      //OLD_HIT_STAT_END;

      if (should_migrate()) {
         ++upward_migrations;
         OLD_HIT_STAT_START;
         admit_element(k, v, true);
         OLD_HIT_STAT_END;
         hot_tree_ios += new_miss - old_miss;
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            //OLD_HIT_STAT_START;
            // remove from the cold partition
            auto op_res __attribute__((unused)) = cold_btree.remove(key_bytes, fold(key_bytes, cold_key));
            //OLD_HIT_STAT_END;
         }
      } else {
         cold_btree.updateSameSize(key_bytes, fold(key_bytes, cold_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
            TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
            tp->set_referenced();
            tp->set_modified();
            memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
         }, Payload::wal_update_generator);
      }
   }


   void put(Key k, Payload& v) override {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = hot_btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->set_referenced();
         tp->set_modified();
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator);
      HIT_STAT_END;

      if (op_res == OP_RESULT::OK) {
         return;
      }

      if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
         auto cold_key = tag_with_cold_bit(k);
         //OLD_HIT_STAT_START;
         // remove from the cold partition
         auto op_res __attribute__((unused)) = cold_btree.remove(key_bytes, fold(key_bytes, cold_key));
         assert(op_res == OP_RESULT::OK);
         //OLD_HIT_STAT_END;
      }
      // move to hot partition
      admit_element(k, v, true);
   }

   double hot_btree_utilization(u64 entries, u64 pages) {
      u64 minimal_pages = (entries) * (sizeof(Key) + sizeof(TaggedPayload)) / leanstore::storage::PAGE_SIZE;
      return minimal_pages / (pages + 0.0);
   }

   double cold_btree_utilization(u64 entries, u64 pages) {
      u64 minimal_pages = (entries) * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      return minimal_pages / (pages + 0.0);
   }

   void report(u64 entries, u64 pages) override {
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      std::size_t hot_btree_pages = 0;
      auto num_btree_hot_entries = btree_entries(hot_btree, hot_btree_pages);
      std::size_t cold_btree_pages = 0;
      auto num_btree_cold_entries = btree_entries(cold_btree, cold_btree_pages);
      pages = hot_btree_pages + cold_btree_pages;
      auto num_btree_entries = num_btree_cold_entries + num_btree_hot_entries;
      std::cout << "Hot partition capacity in bytes " << hot_partition_capacity_bytes << std::endl;
      std::cout << "Hot partition size in bytes " << hot_partition_size_bytes << std::endl;
      std::cout << "BTree # entries " << num_btree_entries << std::endl;
      std::cout << "BTree # hot entries " << num_btree_hot_entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "Hot BTree height " << hot_btree.getHeight() << " # pages " << hot_btree_pages << " util " << hot_btree_utilization(num_btree_hot_entries, hot_btree_pages) << std::endl;
      std::cout << "Cold BTree height " << cold_btree.getHeight() << " # pages " << cold_btree_pages << " util " << hot_btree_utilization(num_btree_cold_entries, cold_btree_pages) << std::endl;
      auto minimal_pages = (num_btree_entries) * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double btree_hit_rate = btree_buffer_hit / (btree_buffer_hit + btree_buffer_miss + 1.0);
      std::cout << "BTree buffer hits/misses " <<  btree_buffer_hit << "/" << btree_buffer_miss << std::endl;
      std::cout << "BTree buffer hit rate " <<  btree_hit_rate << " miss rate " << (1 - btree_hit_rate) << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
      std::cout << total_lookups<< " lookups, "  << lookups_hit_top << " lookup hit top" << " hot_tree_ios " << hot_tree_ios << " ios/tophit " << hot_tree_ios / (lookups_hit_top + 0.00)  << std::endl;
      std::cout << upward_migrations << " upward_migrations, "  << downward_migrations << " downward_migrations" << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01);
   }
};


template <typename Key, typename Payload, int NodeSize = 512>
struct STX2BTreeAdapter : StorageInterface<Key, Payload> {
   
   static constexpr double eviction_threshold = 1;
   std::size_t cache_capacity_bytes;
   std::size_t hit_count = 0;
   std::size_t miss_count = 0;
   std::size_t eviction_items = 0;
   std::size_t eviction_io_reads = 0;
   std::size_t io_reads_snapshot = 0;
   std::size_t io_reads_now = 0;
   typedef std::pair<Key, Payload> Element;
   struct alignas(1) TaggedPayload {
      Payload payload;
      unsigned char bits = 0;
      bool modified() const { return bits & 1; }
      void set_modified() { bits |= 1; }
      void clear_modified() { bits &= ~1; }
      bool referenced() const { return bits & 2; }
      void set_referenced() { bits |= 2; }
      void clear_referenced() { bits &= ~2; }
   };
   static_assert(sizeof(TaggedPayload) == sizeof(Payload) + 1, "!!!");

   class my_stx_btree_map_traits {
      public:
      /// If true, the tree will self verify it's invariants after each insert()
      /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
      static const bool selfverify = false;

      /// If true, the tree will print out debug information and a tree dump
      /// during insert() or erase() operation. The header must have been
      /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
      /// printable.
      static const bool debug = false;
      
      static const size_t node_size = NodeSize;
      /// Number of slots in each leaf of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int leafslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(TaggedPayload)));

      /// Number of slots in each inner node of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int innerslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(void*)));

      /// As of stx-btree-0.9, the code does linear search in find_lower() and
      /// find_upper() instead of binary_search, unless the node size is larger
      /// than this threshold. See notes at
      /// http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
      static const size_t binsearch_threshold = 256;
   };

   typedef stx::btree_map<Key, TaggedPayload, std::less<Key>, my_stx_btree_map_traits> cache_type;
   typedef stx::btree_map<Key, Payload, std::less<Key>, my_stx_btree_map_traits> base_type;
   cache_type cache;
   base_type btree;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   bool inclusive;
   u64 upward_migrations = 0;
   u64 lazy_migration_threshold = 10;
   STX2BTreeAdapter(double cache_size_gb, int lazy_migration_sampling_rate = 100, bool inclusive = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration_sampling_rate < 100), inclusive(inclusive) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      upward_migrations = 0;
   }

   void evict_all() override {
      while (cache.size() > 0) {
         evict_a_bunch();
      }
   }

   inline size_t get_cache_btree_size() {
      auto & stat = cache.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size();
   }

  inline size_t get_btree_size() {
      auto & stat = btree.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size();
   }

   bool cache_under_pressure() {
      return get_cache_btree_size() >= cache_capacity_bytes * eviction_threshold;
   }

   std::size_t btree_entries() {
      return btree.size();
   }

   static constexpr int kClockWalkSteps = 128;
   void evict_a_bunch() {
      int steps = kClockWalkSteps;
      Key key = clock_hand;
      typename cache_type::iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }

      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      bool victim_found = false;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      while (it != cache.end()) {
         if (--steps == 0) {
            break;
         }
         if (it.data().referenced() == false) {
            it.data().clear_referenced();
            clock_hand = it.key();
            ++it;
         } else {
            auto tmp = it;
            ++tmp;
            if (tmp == cache.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               clock_hand = tmp.key();
            }

            evict_keys.push_back(it.key());
            evict_payloads.emplace_back(it.data());
            victim_found = true;
            if (evict_keys.size() >= 64) {
               break;
            }
            it = tmp;
         }
      }
      auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
      assert(io_reads_new >= io_reads_old);
      eviction_io_reads += io_reads_new - io_reads_old;
      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            cache.erase(key);
            if (inclusive) {
               if (tagged_payload.modified()) {
                  upsert_btree(key, tagged_payload.payload);
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_btree(key, tagged_payload.payload);
            }
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
         evict_a_bunch();
      }
   }

   static constexpr int kEvictionCheckInterval = 128;
   int eviction_check_countdown = kEvictionCheckInterval;

   void try_eviction() {
      if (--eviction_check_countdown == 0) {
         if (cache_under_pressure())
            evict_a_bunch();
         eviction_check_countdown = kEvictionCheckInterval;
      }
   }

   void admit_element(Key k, Payload & v, bool dirty = false) {
      if (cache_under_pressure())
         evict_a_bunch();
      assert(cache.find(k) == cache.end());
      TaggedPayload tp;
      tp.payload = v;
      if (dirty) {
         tp.set_modified();
      }
      tp.set_referenced();
      cache[k] = tp;
   }

   bool lookup(Key k, Payload& v) override
   {
      //DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); });
      try_eviction();
      auto it = cache.find(k);
      if (it != cache.end()) {
         if (!it.data().referenced()) {
            it.data().set_referenced();
         }
         v = it.data().payload;
         hit_count++;
         return true;
      }
      
      miss_count++;
      u8 key_bytes[sizeof(Key)];
      bool res = false;
      auto it_btree = btree.find(k);
      if (it_btree != btree.end()) {
         v = it_btree.data();
         res = true;
      }

      if (res) {
         if (should_migrate()) {
            upward_migrations++;
            admit_element(k, v);
            if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
               btree.erase(k);
            }
         }
      }
      return res;
   }

   void upsert_btree(Key k, Payload& v)
   {
      btree[k] = v;
   }

   void insert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      btree[k] = v;
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
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data().set_referenced();
         it.data().set_modified();
         it.data().payload = v;
         hit_count++;
         return;
      }

      if (should_migrate()) {
         upward_migrations++;
         u8 key_bytes[sizeof(Key)];
         Payload old_v;
         auto it_btree = btree.find(k);
         bool res = false;
         assert(it_btree != btree.end());
         if (it_btree != btree.end()) {
            old_v = it_btree.data();
            res = true;
         }

         admit_element(k, old_v);
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            btree.erase(k);
         }
         update(k, v);
      } else {
         btree[k] = v;
      }
   }

   void put(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data().set_referenced();
         it.data().set_modified();
         it.data().payload = v;
         hit_count++;
         return;
      }
      if (inclusive) {
         admit_element(k, v, true);
         return;
      }
      btree[k] = v;
      if (should_migrate()) {
         upward_migrations++;
         admit_element(k, v);
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            btree.erase(k);
         }
      }
   }

   void report_cache() {
      std::cout << "Top capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Top btree size bytes " << get_cache_btree_size() << std::endl;
      std::cout << "Top btree size # entries " << cache.size() << std::endl;
      std::cout << "Top btree hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "Top btree BTree average leaf fill factor " << cache.get_stats().avgfill_leaves() << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Top btree hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Top btree miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
   }

   void report(u64 entries __attribute__((unused)) , u64 pages) override {
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      auto num_btree_entries = btree_entries();
      report_cache();
      std::cout << "Bottom btree size bytes " << get_btree_size() << std::endl;
      std::cout << "Bottom btree size # entries " << btree.size() << std::endl;
      std::cout << "Bottom btree BTree average leaf fill factor " << btree.get_stats().avgfill_leaves() << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
      std::cout << "Upward migrations " << upward_migrations++;
   }
};


template <typename Key, typename Payload, int NodeSize = 512>
struct STXBTreeAdapter : StorageInterface<Key, Payload> {
   
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
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };

   class my_stx_btree_map_traits {
      public:
      /// If true, the tree will self verify it's invariants after each insert()
      /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
      static const bool selfverify = false;

      /// If true, the tree will print out debug information and a tree dump
      /// during insert() or erase() operation. The header must have been
      /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
      /// printable.
      static const bool debug = false;
      
      static const size_t node_size = NodeSize;
      /// Number of slots in each leaf of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int leafslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(TaggedPayload)));

      /// Number of slots in each inner node of the tree. Estimated so that each node
      /// has a size of about 256 bytes.
      static const int innerslots = BTREE_MAX(8, node_size / (sizeof(Key) + sizeof(void*)));

      /// As of stx-btree-0.9, the code does linear search in find_lower() and
      /// find_upper() instead of binary_search, unless the node size is larger
      /// than this threshold. See notes at
      /// http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
      static const size_t binsearch_threshold = 256;
   };

   typedef stx::btree_map<Key, Payload, std::less<Key>, my_stx_btree_map_traits> base_type;
   base_type btree;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   bool inclusive;
   u64 lazy_migration_threshold = 10;
   STXBTreeAdapter(double cache_size_gb, int lazy_migration_sampling_rate = 100, bool inclusive = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration_sampling_rate < 100), inclusive(inclusive) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

  inline size_t get_btree_size() {
      auto & stat = btree.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size();
   }

   std::size_t btree_entries() {
      return btree.size();
   }


   bool lookup(Key k, Payload& v) override
   {
      //DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      auto it = btree.find(k);
      if (it != btree.end()) {
         v = it.data();
         return true;
      }
      return false;
   }

   void upsert_btree(Key k, Payload& v)
   {
      btree[k] = v;
   }

   void insert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      btree[k] = v;
   }

   void insert(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      btree[k] = v;
   }

   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void update(Key k, Payload& v) override
   {
      btree[k] = v;
   }

   void put(Key k, Payload& v) override
   {
      btree[k] = v;
   }

   void report(u64 entries __attribute__((unused)) , u64 pages) override {
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      auto num_btree_entries = btree_entries();
      std::cout << "capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "stx::btree size bytes " << get_btree_size() << std::endl;
      std::cout << "stx::btree size # entries " << btree.size() << std::endl;
      std::cout << "stx::btree hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "stx::btree average leaf fill factor " << btree.get_stats().avgfill_leaves() << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "stx::btree hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "stx::btree miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
   }
};
}