#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "interface/StorageInterface.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
static constexpr int max_eviction_workers = 4;
namespace leanstore
{

static constexpr int kEvictionCheckInterval = 50;
static thread_local int eviction_count_down = kEvictionCheckInterval;
class AdmissionControl {
public:
   int max_workers;
   std::atomic<int> active_workers;
   AdmissionControl(int max_workers = max_eviction_workers): max_workers(max_workers) {}
   
   bool enter() {
      if (active_workers.fetch_add(1) < max_workers) {
         return true;
      } else {
         active_workers.fetch_sub(1);
         return false;
      }
   }

   void exit() {
      active_workers.fetch_sub(1);
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
struct ConcurrentPartitionedLeanstore: BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& hot_btree;
   leanstore::storage::btree::BTreeInterface& cold_btree;

   uint64_t btree_buffer_miss = 0;
   uint64_t btree_buffer_hit = 0;
   DTID dt_id;
   std::size_t hot_partition_capacity_bytes;
   std::atomic<std::size_t> hot_partition_size_bytes{0};
   bool inclusive = false;
   static constexpr double eviction_threshold = 0.7;
   uint64_t eviction_items = 0;
   uint64_t eviction_io_reads= 0;
   uint64_t io_reads_snapshot = 0;
   uint64_t io_reads_now = 0;
   u64 lazy_migration_threshold = 10;
   bool lazy_migration = false;
   std::atomic<uint64_t> placeholder_installations = 0;
   std::atomic<uint64_t> upward_migrations = 0;
   std::atomic<uint64_t> downward_migrations = 0;
   std::atomic<uint64_t> hot_partition_item_count{0};
   std::atomic<uint64_t> lookup_retries = 0;
   std::atomic<uint64_t> total_lookups = 0;
   //uint64_t hot_tree_ios = 0;
   struct TaggedPayload {
      Payload payload;
      bool modified = false;
      bool referenced = false;
      bool inflight = false;
   };
   
   Key clock_hand = std::numeric_limits<Key>::max();
   constexpr static u64 PartitionBitPosition = 63;
   constexpr static u64 PartitionBitMask = 0x8000000000000000;
   std::atomic<bool> in_eviction{false};
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

   ConcurrentPartitionedLeanstore(leanstore::storage::btree::BTreeInterface& hot_btree, leanstore::storage::btree::BTreeInterface& cold_btree, double hot_partition_size_gb, bool inclusive = false, int lazy_migration_sampling_rate = 100) : hot_btree(hot_btree), cold_btree(cold_btree), hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL), inclusive(inclusive), lazy_migration(lazy_migration_sampling_rate < 100) {
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
      upward_migrations = downward_migrations = placeholder_installations =0;
      lookup_retries = total_lookups = 0;
   }

   std::size_t btree_entries(leanstore::storage::btree::BTreeInterface& btree, std::size_t & pages) {
      Key start_key = std::numeric_limits<Key>::min();
      u8 key_bytes[sizeof(Key)];
      std::size_t entries = 0;
      pages = 0;
      const char * last_leaf_frame = nullptr;
      while (true) {
         bool good = false;
         btree.scanAsc(key_bytes, fold(key_bytes, start_key),
         [&](const u8 * key, u16, const u8 *, u16, const char * leaf_frame) -> bool {
            auto real_key = unfold(*(Key*)(key));
            good = true;
            start_key = real_key + 1;
            if (last_leaf_frame != leaf_frame) {
               last_leaf_frame = leaf_frame;
               ++pages;
            }
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
      while (hot_partition_item_count > 0) {
         evict_a_bunch();
      }
   }

   static constexpr int kClockWalkSteps = 1024;
   void evict_a_bunch() {
      int steps = kClockWalkSteps; // number of steps to walk
      Key start_key = clock_hand;
      if (start_key == std::numeric_limits<Key>::max()) {
         start_key = tag_with_hot_bit(std::numeric_limits<Key>::min());
      }

      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      bool victim_found = false;
      assert(is_in_hot_partition(start_key));
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res = hot_btree.scanAsc(key_bytes, fold(key_bytes, start_key),
         [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
            if (--steps == 0) {
               return false;
            }
            auto real_key = unfold(*(Key*)(key));
            assert(key_length == sizeof(Key));

            TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
            assert(value_length == sizeof(TaggedPayload));
            if (tp->inflight) { // skip inflight records
               return true;
            }
            if (tp->referenced == true) {
               tp->referenced = false;
               return true;
            }
            clock_hand = real_key;
            tp->inflight = true;
            victim_found = true;
            evict_keys.push_back(real_key);
            evict_payloads.emplace_back(*tp);
            if (evict_keys.size() >= 256) {
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
      in_eviction.store(false);
      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            if (inclusive) {
               if (tagged_payload.modified) {
                  upsert_cold_partition(strip_off_partition_bit(key), tagged_payload.payload); // Cold partition
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               bool res = insert_partition(strip_off_partition_bit(key), tagged_payload.payload, true /* cold */, false /* inflight=false */); // Cold partition
               assert(res);
            }

            assert(is_in_hot_partition(key));
            auto op_res = hot_btree.remove(key_bytes, fold(key_bytes, key));
            assert(op_res == OP_RESULT::OK);
            hot_partition_item_count--;
            hot_partition_size_bytes.fetch_sub(sizeof(Key) + sizeof(TaggedPayload));
         }
         auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
         assert(io_reads_new >= io_reads_old);
         eviction_io_reads += io_reads_new - io_reads_old;
         eviction_items += evict_keys.size();
         downward_migrations += evict_keys.size();
      } else {
         if (steps == kClockWalkSteps) {
            clock_hand = std::numeric_limits<Key>::max();
         }
      }
   }

   AdmissionControl control;
   void evict_till_safe() {
      while (cache_under_pressure()) {
         if (in_eviction.load() == false) {
            bool state = false;
            if (in_eviction.compare_exchange_strong(state, true)) {
               // Allow only one thread to perform the eviction
               evict_a_bunch();
               //in_eviction.store(false);
            }
         } else {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
         }
      }
   }

   void try_eviction() {
      if (--eviction_count_down == 0) {
         if (cache_under_pressure()) {
            if (in_eviction.load() == false) {
               bool state = false;
               if (in_eviction.compare_exchange_strong(state, true)) {
                  // Allow only one thread to perform the eviction
                  evict_a_bunch();
                  //in_eviction.store(false);
               } else {
                  //std::this_thread::sleep_for(std::chrono::microseconds(10));   
               }
            } else {
               //std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
         }
         eviction_count_down = kEvictionCheckInterval;
      }
   }


   bool admit_element(Key k, Payload & v, bool dirty = false) {
      try_eviction();
      bool res = insert_partition(strip_off_partition_bit(k), v, false /* hot*/, false /* inflight */, dirty, false /* reference */); // Hot partition
      assert(res);
      hot_partition_size_bytes.fetch_add(sizeof(Key) + sizeof(TaggedPayload));
      hot_partition_item_count++;
      return res;
   }

   void fill_lookup_placeholder(Key k, Payload &v) {
      auto hot_key = tag_with_hot_bit(k);
      u8 key_bytes[sizeof(Key)];
      HIT_STAT_START;
      auto op_res = hot_btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->referenced = true;
         tp->modified = false;
         tp->inflight = false;
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator);
      HIT_STAT_END;

      assert(op_res == OP_RESULT::OK);
   }

   bool admit_lookup_placeholder_atomically(Key k) {
      Payload empty_payload;
      bool res = insert_partition(strip_off_partition_bit(k), empty_payload, false /* hot*/, true /* inflight */, false);
      if (res) {
         hot_partition_size_bytes.fetch_add(sizeof(Key) + sizeof(TaggedPayload));
         hot_partition_item_count++;
      }
      ++placeholder_installations;
      return res;
   }

   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void update_reference_bit(Key k) {
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      hot_btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         assert(payload_length == sizeof(TaggedPayload));
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         tp->referenced = true;
      });
   }

   bool lookup(Key k, Payload& v) override
   {
      uint64_t retries = 0;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction(); lookup_retries += retries; total_lookups++;});
      u8 key_bytes[sizeof(Key)];
      //TaggedPayload tp;
      // try hot partition first
      auto hot_key = tag_with_hot_bit(k);
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      retry:
      bool inflight = false;
      bool referenced = false;
      bool found_in_cache = hot_btree.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
         assert(payload_length == sizeof(TaggedPayload));
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         if (tp->inflight) {
            inflight = true;
            return;
         }
         referenced = tp->referenced;
         tp->referenced = true;
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }, retries) == OP_RESULT::OK;

      if (inflight) {
         goto retry;
      }
      
      if (found_in_cache) {
         // if (referenced == false) {
         //    update_reference_bit(k);
         // }
         return true;
      }

      if (admit_lookup_placeholder_atomically(k) == false) {
         goto retry;
      }

      /* Now we have the inflight token placeholder, let's move the tuple from underlying btree to cache btree. */

      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }

      auto cold_key = tag_with_cold_bit(k);
      old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = cold_btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;
      new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }

      if (res) {
         if (should_migrate()) {
            ++upward_migrations;
            // fill in the placeholder
            fill_lookup_placeholder(k, v);
            if (inclusive == false) {
               // remove from the cold partition
               auto op_res __attribute__((unused))= cold_btree.remove(key_bytes, fold(key_bytes, cold_key));
               assert(op_res == OP_RESULT::OK);
            }
         } else {
            // Remove the lookup placeholder.
            bool removeRes = hot_btree.remove(key_bytes, fold(key_bytes, hot_key)) == OP_RESULT::OK;
            // the placeholder must be there.
            assert(removeRes);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
            hot_partition_item_count--;
         }
      } else {
         // Remove the lookup placeholder.
         bool removeRes = hot_btree.remove(key_bytes, fold(key_bytes, hot_key)) == OP_RESULT::OK;
         // the placeholder must be there.
         assert(removeRes);
         hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
         hot_partition_item_count--;
      }
      return res;
   }

   void upsert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);
      // TaggedPayload tp;
      // tp.referenced = true;
      // tp.payload = v;
      auto op_res = cold_btree.updateSameSize(key_bytes, fold(key_bytes, key), [&](u8* payload, u16 payload_length) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->referenced = true;
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator);

      if (op_res == OP_RESULT::NOT_FOUND) {
         bool res = insert_partition(k, v, true /* cold */, false /* inflight=false */);
         assert(res);
      }
   }

   // hot_or_cold: false => hot partition, true => cold partition
   bool insert_partition(Key k, Payload & v, bool hot_or_cold, bool inflight, bool modified = false, bool referenced = true) {
      u8 key_bytes[sizeof(Key)];
      Key key = hot_or_cold == false ? tag_with_hot_bit(k) : tag_with_cold_bit(k);
      TaggedPayload tp;
      tp.referenced = referenced;
      tp.modified = modified;
      tp.inflight = inflight;
      tp.payload = v;

      if (hot_or_cold == false) {
         HIT_STAT_START;
         auto op_res = hot_btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
         HIT_STAT_END;
         if (op_res != OP_RESULT::OK) {
            assert(op_res == OP_RESULT::DUPLICATE);
         }
         return op_res == OP_RESULT::OK;
      } else {
         auto op_res = cold_btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
         if (op_res != OP_RESULT::OK) {
            assert(op_res == OP_RESULT::DUPLICATE);
         }
         return op_res == OP_RESULT::OK;
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

   void update(Key k, Payload& v) override {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);

      retry:
      HIT_STAT_START;
      bool inflight = false;
      bool found_in_cache = hot_btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         if (tp->inflight) {
            inflight = true;
            return;
         }
         tp->referenced = true;
         tp->modified = true;
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator) == OP_RESULT::OK;
      HIT_STAT_END;

      if (inflight) {
         goto retry;
      }
      
      if (found_in_cache == false) {
         Payload empty_v;
         // on miss, perform a read first.
         lookup(k, empty_v);
         goto retry;
      }
   }


   void put(Key k, Payload& v) override {
      assert(false);
      // DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      // u8 key_bytes[sizeof(Key)];
      // auto hot_key = tag_with_hot_bit(k);
      // HIT_STAT_START;
      // auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
      //    TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
      //    tp->referenced = true;
      //    tp->modified = true;
      //    memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      // }, Payload::wal_update_generator);
      // HIT_STAT_END;

      // if (op_res == OP_RESULT::OK) {
      //    return;
      // }

      // if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
      //    auto cold_key = tag_with_cold_bit(k);
      //    OLD_HIT_STAT_START;
      //    // remove from the cold partition
      //    auto op_res __attribute__((unused)) = btree.remove(key_bytes, fold(key_bytes, cold_key));
      //    assert(op_res == OP_RESULT::OK);
      //    OLD_HIT_STAT_END;
      // }
      // // move to hot partition
      // admit_element(k, v, true);
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
      std::cout << "Buffer Managed # pages " << pages << std::endl;
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
      std::cout << total_lookups<< " lookups, lookup retries " << lookup_retries << std::endl;
      // std::cout << total_lookups<< " lookups, "  << lookups_hit_top << " lookup hit top" << " hot_tree_ios " << hot_tree_ios << " ios/tophit " << hot_tree_ios / (lookups_hit_top + 0.00)  << std::endl;
      std::cout << upward_migrations << " upward_migrations, "  << downward_migrations << " downward_migrations, " << " placeholder_installations "<< placeholder_installations<< std::endl;
   }
};


}