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
struct BTreeVSHotColdPartitionedAdapter : BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;

   uint64_t btree_buffer_miss = 0;
   uint64_t btree_buffer_hit = 0;
   DTID dt_id;
   std::size_t hot_partition_capacity_bytes;
   std::size_t hot_partition_size_bytes = 0;
   bool inclusive = false;
   static constexpr double eviction_threshold = 0.99;
   uint64_t eviction_items = 0;
   uint64_t eviction_io_reads= 0;
   uint64_t io_reads_snapshot = 0;
   uint64_t io_reads_now = 0;
   uint64_t hot_partition_item_count = 0;
   struct TaggedPayload {
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };
   
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
      return key | (1ul << PartitionBitPosition);
   }

   Key strip_off_partition_bit(Key key) {
      return key & ~(PartitionBitMask);
   }

   BTreeVSHotColdPartitionedAdapter(leanstore::storage::btree::BTreeInterface& btree, DTID dt_id,  double hot_partition_size_gb, bool inclusive = false) : btree(btree), dt_id(dt_id), hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL), inclusive(inclusive) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   bool cache_under_pressure() {
      return hot_partition_size_bytes >= hot_partition_capacity_bytes * eviction_threshold;
   }

   void clear_stats() override {
      btree_buffer_miss = btree_buffer_hit = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   std::size_t btree_hot_entries() {
      Key start_key = std::numeric_limits<Key>::min();
      u8 key_bytes[sizeof(Key)];
      std::size_t entries = 0;
      while (true) {
         bool good = false;
         btree.scanAsc(key_bytes, fold(key_bytes, start_key),
         [&](const u8 * key, u16 key_length, const u8 *, u16) -> bool {
            auto real_key = unfold(*(Key*)(key));
            assert(key_length == sizeof(Key));
            if (is_in_hot_partition(real_key) == false) { // stop at first cold key
               return false;
            }
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
      while (hot_partition_item_count > 0) {
         evict_one();
      }
   }

   void evict_one() {
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
      assert(is_in_hot_partition(start_key));
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      btree.scanAsc(key_bytes, fold(key_bytes, start_key),
      [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
         auto real_key = unfold(*(Key*)(key));
         assert(key_length == sizeof(Key));
         if (is_in_hot_partition(real_key) == false) {
            clock_hand = std::numeric_limits<Key>::max();
            return false;
         }
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
         assert(value_length == sizeof(TaggedPayload));
         if (tp->referenced == true) {
            tp->referenced = false;
            return true;
         }
         evict_key = real_key;
         clock_hand = real_key;
         evict_payload = tp->payload;
         victim_found = true;
         evict_keys.push_back(real_key);
         evict_payloads.emplace_back(*tp);
         if (evict_keys.size() >= 10) {
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
            auto op_res = btree.remove(key_bytes, fold(key_bytes, key));
            hot_partition_item_count--;
            assert(op_res == OP_RESULT::OK);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
            if (inclusive) {
               if (tagged_payload.modified) {
                  upsert_cold_partition(strip_off_partition_bit(key), tagged_payload.payload); // Cold partition
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_partition(strip_off_partition_bit(key), tagged_payload.payload, true); // Cold partition
            }
         }
         auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
         assert(io_reads_new >= io_reads_old);
         eviction_io_reads += io_reads_new - io_reads_old;
         eviction_items += evict_keys.size();
      } else {
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
      insert_partition(strip_off_partition_bit(k), v, false, dirty); // Hot partition
      hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
   }

   bool lookup(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      // try hot partition first
      auto hot_key = tag_with_hot_bit(k);
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = btree.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         tp->referenced = true;
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }
      if (res) {
         return res;
      }

      auto cold_key = tag_with_cold_bit(k);
      old_miss = WorkerCounters::myCounters().io_reads.load();
      res = btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
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
         if (inclusive == false) {
            OLD_HIT_STAT_START;
            // remove from the cold partition
            auto op_res __attribute__((unused))= btree.remove(key_bytes, fold(key_bytes, cold_key));
            assert(op_res == OP_RESULT::OK);
            OLD_HIT_STAT_END
         }

         // move to hot partition
         admit_element(k, v);
      }
      return res;
   }

   void upsert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);
      TaggedPayload tp;
      tp.referenced = true;
      tp.payload = v;
      HIT_STAT_START;
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, key), [&](u8* payload, u16 payload_length) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->referenced = true;
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator);
      HIT_STAT_END;

      if (op_res == OP_RESULT::NOT_FOUND) {
         insert_partition(k, v, true);
      }
   }

   // hot_or_cold: false => hot partition, true => cold partition
   void insert_partition(Key k, Payload & v, bool hot_or_cold, bool modified = false) {
      u8 key_bytes[sizeof(Key)];
      Key key = hot_or_cold == false ? tag_with_hot_bit(k) : tag_with_cold_bit(k);
      if (hot_or_cold == false) {
         hot_partition_item_count++;
      }
      TaggedPayload tp;
      tp.referenced = true;
      tp.modified = modified;
      tp.payload = v;
      HIT_STAT_START;
      auto op_res = btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      assert(op_res == OP_RESULT::OK);
      HIT_STAT_END;
   }

   void insert(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      //admit_element(k, v);
      if (inclusive) {
         admit_element(k, v, true);
      } else {
         admit_element(k, v);
      }
   }

   void update(Key k, Payload& v) override {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->referenced = true;
         tp->modified = true;
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator);
      HIT_STAT_END;

      if (op_res == OP_RESULT::OK) {
         return;
      }

      Payload old_v;
      auto cold_key = tag_with_cold_bit(k);
      OLD_HIT_STAT_START;
      auto res __attribute__((unused)) = btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&old_v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;
      assert(res);
      OLD_HIT_STAT_END;

      admit_element(k, old_v, false);
      if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
         auto cold_key = tag_with_cold_bit(k);
         OLD_HIT_STAT_START;
         // remove from the cold partition
         auto op_res __attribute__((unused)) = btree.remove(key_bytes, fold(key_bytes, cold_key));
         OLD_HIT_STAT_END;
      }
      update(k, v);
   }


   void put(Key k, Payload& v) override {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->referenced = true;
         tp->modified = true;
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      }, Payload::wal_update_generator);
      HIT_STAT_END;

      if (op_res == OP_RESULT::OK) {
         return;
      }

      if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
         auto cold_key = tag_with_cold_bit(k);
         OLD_HIT_STAT_START;
         // remove from the cold partition
         auto op_res __attribute__((unused)) = btree.remove(key_bytes, fold(key_bytes, cold_key));
         assert(op_res == OP_RESULT::OK);
         OLD_HIT_STAT_END;
      }
      // move to hot partition
      admit_element(k, v, true);
   }

   void report(u64 entries, u64 pages) override {
      assert(io_reads_now >= io_reads_snapshot);
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      auto num_btree_entries = btree_entries();
      auto num_btree_hot_entries = btree_hot_entries();
      std::cout << "Hot partition capacity in bytes " << hot_partition_capacity_bytes << std::endl;
      std::cout << "Hot partition size in bytes " << hot_partition_size_bytes << std::endl;
      std::cout << "BTree # entries " << num_btree_entries << std::endl;
      std::cout << "BTree # hot entries " << num_btree_hot_entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "BTree height " << btree.getHeight() << std::endl;
      auto minimal_pages = entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double btree_hit_rate = btree_buffer_hit / (btree_buffer_hit + btree_buffer_miss + 1.0);
      std::cout << "BTree buffer hits/misses " <<  btree_buffer_hit << "/" << btree_buffer_miss << std::endl;
      std::cout << "BTree buffer hit rate " <<  btree_hit_rate << " miss rate " << (1 - btree_hit_rate) << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
   }
};


}