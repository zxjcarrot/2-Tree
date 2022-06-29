#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "btreeolc/btreeolc.hpp"

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{

thread_local int index_op_iters = 0;
static constexpr int ebr_exit_threshold = 100; // stay in the epoch for this many ops before exiting
/*

***Invariant: At any time, there is at most one thread working on the migration of a tuple.***

Lookup:
1. read cache
   1.1 on hit
      if record.inflight == true, goto step 1.
      else, return record
   1.2 on miss, atomically install a record in the cache with inflight=true
      on success, fetch record from underlying BTree.
      on failure, goto step 1.

Eviction: (only one thread is allowed to perform the eviction)
Threads race to set an atomic flag (in_eviction). 
Winner gets to perform the eviction.

1. scan the cache until victim set has enough entries:
   1.1 if record.inflight == true, skip
   1.2 if record.referenced == true, set record.referenced = false
   1.3 if record.referenced = false, set record.inflight=true, add to victim set.

2. Push the records in victim set to the underlying btree

3. Remove records in victim set from the cache

Insert: (assuming key does not exist)

1. atomically install a record into the cache with record.referenced = true, record.modified = true, record.inflight = false.

Update:

read cache:
   on record.inflight=true, goto step 1.
   on miss, perform Lookup and goto step 1.
   on hit, update record in cache by setting record.modified=true, record.referenced=true

*/
template <typename Key, typename Payload, int NodeSize = 1024>
struct ConcurrentBTreeBTree : BTreeInterface<Key, Payload> {
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
      bool inflight = false; // True if tuple is being migrated downwards
   };

   class KeyComparator {
   public:
      int operator()(const Key &lhs, const Key &rhs) const {
         return lhs < rhs ? -1 : (lhs > rhs ? 1 : 0);
      }
   };

   class ValueComparator {
   public:
      bool operator()(const TaggedPayload &lhs, const TaggedPayload &rhs) const {
         assert(false);
         return false;
      }
   };

   btreeolc::BPlusTree<Key, TaggedPayload, KeyComparator, ValueComparator> cache;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   bool inclusive;
   u64 lazy_migration_threshold = 10;
   std::atomic<bool> in_eviction{false};
   ConcurrentBTreeBTree(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, bool lazy_migration = false, bool inclusive = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration), inclusive(inclusive) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void evict_all() override {
      while (cache.size() > 0) {
         evict_a_bunch();
      }
   }

   inline size_t get_cache_btree_size() {
      auto leaves = cache.getNumLeafNodes();
      auto innernodes = cache.getNumInnerNodes();
      return leaves * cache.getLeafNodeSize() + innernodes * cache.getInnerNodeSize();
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

   static constexpr int eviction_limit = 100;
   void evict_a_bunch() {
      Key start_key = clock_hand;
      if (start_key == std::numeric_limits<Key>::max()) {
         start_key = std::numeric_limits<Key>::min();
      }
      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      cache.scanForUpdate(start_key, [&](const Key & key, TaggedPayload & payload, bool last_item) {
         clock_hand = key;
         if (payload.referenced == true) {
            payload.referenced = false;
         } else {
            evict_keys.push_back(key);
            assert(payload.inflight == false);
            payload.inflight = true;
            evict_payloads.push_back(payload);
            evict_payloads.back().inflight = false;
         }
         if (last_item) {
            clock_hand = std::numeric_limits<Key>::max();
            return true;
         }
         if (evict_payloads.size() > eviction_limit) { // evict a bunch of payloads at a time
            return true;
         }
         return false;
      });

      for (size_t i = 0; i < evict_payloads.size(); ++i) {
         if (inclusive) {
            if (evict_payloads[i].modified) {
               upsert_btree(evict_keys[i], evict_payloads[i].payload);
            }
         } else { // exclusive, put it back in the on-disk B-Tree
            insert_btree(evict_keys[i], evict_payloads[i].payload);
         }
         bool res = cache.remove(evict_keys[i]);
         assert(res == true);
      }
   }

   void evict_till_safe() {
      while (cache_under_pressure()) {
         if (in_eviction.load() == false) {
            bool state = false;
            if (in_eviction.compare_exchange_strong(state, true)) {
               // Allow only one thread to perform the eviction
               evict_a_bunch();
               in_eviction.store(false);
            }
         } else {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
         }
      }
   }

   bool admit_element(Key k, Payload & v, bool modified = false) {
      if (cache_under_pressure())
         evict_till_safe();
      bool res = cache.insert(k, TaggedPayload{v, modified, true /* referenced */, false /* inflight */ });
      return res;
   }

   bool admit_lookup_placeholder_atomically(Key k) {
      if (cache_under_pressure())
         evict_till_safe();
      Payload empty_payload;
      bool res = cache.insert(k, TaggedPayload{empty_payload, false /* modified */, true /* referenced */, true /* inflight */ });
      return res;
   }

   void fill_lookup_placeholder(Key k, Payload &v) {
      bool found_in_cache = cache.lookupForUpdate(k, [&](const Key & key, TaggedPayload & payload){
         assert(payload.inflight);
         assert(payload.modified == false);
         payload.referenced = true;
         payload.payload = v;
         payload.inflight = false;
         return;
      });
      assert(found_in_cache);
   }

   bool lookup(Key k, Payload& v) override
   {
      if (index_op_iters == 0) {
         cache.EBREnter();
      }
      DeferCode c([&, this](){
         io_reads_now = WorkerCounters::myCounters().io_reads.load(); 
         if (++index_op_iters >= ebr_exit_threshold) {
            cache.EBRExit();
            index_op_iters = 0;
         }
         }
      );
      retry:
      bool inflight = false;
      bool found_in_cache = cache.lookupForUpdate(k, [&](const Key & key, TaggedPayload & payload){
         if (payload.inflight) {
            inflight = true;
            return;
         }
         payload.referenced = true;
         v = payload.payload;
         hit_count++;
         return;
      });

      if (inflight) {
         goto retry;
      }
      
      if (found_in_cache) {
         return true;
      }

      if (admit_lookup_placeholder_atomically(k) == false) {
         goto retry;
      }

      /* Now we have the inflight token placeholder, let's move the tuple from underlying btree to cache btree. */

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
         fill_lookup_placeholder(k, v);
      } else {
         // Remove the lookup placeholder.
         bool removeRes = cache.remove(k);
         // the placeholder must be there.
         assert(removeRes);
      }
      return res;
   }

   void upsert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(op_res == OP_RESULT::OK || op_res == OP_RESULT::NOT_FOUND);
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
         bool res = admit_element(k, v, true);
         assert(res);
      } else {
         bool res = admit_element(k, v);
         assert(res);
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
      if (index_op_iters == 0) {
         cache.EBREnter();
      }
      DeferCode c([&, this](){
         io_reads_now = WorkerCounters::myCounters().io_reads.load(); 
         if (++index_op_iters >= ebr_exit_threshold) {
            cache.EBRExit();
            index_op_iters = 0;
         }
         }
      );
      retry:
      bool inflight = false;
      bool found_in_cache = cache.lookupForUpdate(k, [&](const Key & key, TaggedPayload & payload){
         if (payload.inflight) {
            inflight = true;
            return;
         }
         payload.referenced = true;
         payload.modified = true;
         payload.payload = v;
         hit_count++;
         return;
      });

      if (inflight) {
         goto retry;
      }
      
      if (found_in_cache == false) {
         Payload empty_v;
         // on miss, perform a read first.
         lookup(k, empty_v);
         goto retry;
      }

      // if (should_migrate()) {
      //    u8 key_bytes[sizeof(Key)];
      //    Payload old_v;
      //    auto old_miss = WorkerCounters::myCounters().io_reads.load();
      //    bool res __attribute__((unused)) = btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&old_v, payload, payload_length); }) ==
      //          OP_RESULT::OK;
      //    assert(res == true);
      //    auto new_miss = WorkerCounters::myCounters().io_reads.load();
      //    if (old_miss != new_miss) {
      //       miss_count += new_miss - old_miss;
      //    }
      //    admit_element(k, old_v);
      //    if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
      //       old_miss = WorkerCounters::myCounters().io_reads.load();
      //       auto op_res __attribute__((unused)) = btree.remove(key_bytes, fold(key_bytes, k));
      //       new_miss = WorkerCounters::myCounters().io_reads.load();
      //       if (old_miss != new_miss) {
      //          miss_count += new_miss - old_miss;
      //       }
      //       assert(op_res == OP_RESULT::OK);
      //    }
      //    update(k, v);
      // } else {
      //    u8 key_bytes[sizeof(Key)];
      //    auto old_miss = WorkerCounters::myCounters().io_reads.load();
      //    auto op_res __attribute__((unused)) = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length __attribute__((unused)) ) { memcpy(payload, &v, payload_length); });
      //    auto new_miss = WorkerCounters::myCounters().io_reads.load();
      //    if (old_miss != new_miss) {
      //       miss_count += new_miss - old_miss;
      //    }
      // }
   }

   void put(Key k, Payload& v) override
   {
      assert(false);
      // DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      // auto it = cache.find(k);
      // if (it != cache.end()) {
      //    it.data().referenced = true;
      //    it.data().modified = true;
      //    it.data().payload = v;
      //    hit_count++;
      //    return;
      // }
      // if (inclusive) {
      //    admit_element(k, v, true);
      //    return;
      // }
      // u8 key_bytes[sizeof(Key)];
      // auto old_miss = WorkerCounters::myCounters().io_reads.load();
      // auto op_res __attribute__((unused)) = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      // auto new_miss = WorkerCounters::myCounters().io_reads.load();
      // if (old_miss != new_miss) {
      //    miss_count += new_miss - old_miss;
      // }
      // assert(op_res == OP_RESULT::OK);
      // if (should_migrate()) {
      //    admit_element(k, v);
      //    if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
      //       old_miss = WorkerCounters::myCounters().io_reads.load();
      //       op_res = btree.remove(key_bytes, fold(key_bytes, k));
      //       new_miss = WorkerCounters::myCounters().io_reads.load();
      //       assert(op_res == OP_RESULT::OK);
      //       if (old_miss != new_miss) {
      //          miss_count += new_miss - old_miss;
      //       }
      //    }
      // }
   }

   void report_cache() {
      std::cout << "Cache capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Cache size bytes " << get_cache_btree_size() << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "Cache BTree average leaf fill factor " << cache.getAverageFillFactor() << std::endl;
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


}