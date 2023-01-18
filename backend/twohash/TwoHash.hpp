#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/hashing/LinearHashing.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "stx/btree_map.h"
#include "leanstore/BTreeAdapter.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{

using HT_OP_RESULT = leanstore::storage::hashing::OP_RESULT;


#undef HIT_STAT_START
#undef HIT_STAT_END
#undef OLD_HIT_STAT_START
#undef OLD_HIT_STAT_END
#define HIT_STAT_START \
auto old_miss = WorkerCounters::myCounters().io_reads.load();

#define HIT_STAT_END \
      auto new_miss = WorkerCounters::myCounters().io_reads.load(); \
      assert(new_miss >= old_miss); \
      if (old_miss == new_miss) { \
         ht_buffer_hit++; \
      } else { \
         ht_buffer_miss += new_miss - old_miss; \
      }

#define OLD_HIT_STAT_START \
   old_miss = WorkerCounters::myCounters().io_reads.load();

#define OLD_HIT_STAT_END \
      new_miss = WorkerCounters::myCounters().io_reads.load(); \
      assert(new_miss >= old_miss); \
      if (old_miss == new_miss) { \
         ht_buffer_hit++; \
      } else { \
         ht_buffer_miss += new_miss - old_miss; \
      }

template <typename Key, typename Payload>
struct TwoHashAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::hashing::LinearHashTable & hot_hash_table;
   leanstore::storage::hashing::LinearHashTable & cold_hash_table;

   uint64_t hot_ht_ios = 0;
   uint64_t ht_buffer_miss = 0;
   uint64_t ht_buffer_hit = 0;
   DTID dt_id;
   std::size_t hot_partition_capacity_bytes;
   std::size_t hot_partition_size_bytes = 0;
   std::size_t scan_ops = 0;
   std::size_t io_reads_scan = 0;
   bool inclusive = false;
   static constexpr double eviction_threshold = 0.5;
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
   u64 clock_hand = 0;
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

   TwoHashAdapter(leanstore::storage::hashing::LinearHashTable& hot_hash_table, 
                  leanstore::storage::hashing::LinearHashTable& cold_hash_table, 
                  double hot_partition_size_gb, bool inclusive = false, int lazy_migration_sampling_rate = 100) 
                  : hot_hash_table(hot_hash_table), cold_hash_table(cold_hash_table), 
                  hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL), 
                  inclusive(inclusive), lazy_migration(lazy_migration_sampling_rate < 100) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   bool cache_under_pressure() {
      return hot_partition_size_bytes >= hot_partition_capacity_bytes * eviction_threshold;
   }

   void clear_stats() override {
      ht_buffer_miss = ht_buffer_hit = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      lookups_hit_top = total_lookups = 0;
      hot_ht_ios = 0;
      upward_migrations = downward_migrations = 0;
      io_reads_scan = 0;
      scan_ops = 0;
   }

   std::size_t hash_table_entries(leanstore::storage::hashing::LinearHashTable& hash_table, std::size_t & pages) {
      pages = hash_table.countPages();
      return hash_table.countEntries();
   }

   void evict_all() override {
      while (hot_partition_item_count > 0) {
         evict_a_bunch();
      }
   }

   static constexpr int kClockWalkSteps = 4; // # number of buckets
   void evict_a_bunch() {
      int steps = kClockWalkSteps; // number of buckets to walk
      u64 start_key = clock_hand;
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
      while (steps--) {
         auto evict_keys_size_snapshot = evict_keys.size();
         hot_hash_table.iterate(clock_hand,
         [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
            auto real_key = unfold(*(Key*)(key));
            assert(key_length == sizeof(Key));
            TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
            assert(value_length == sizeof(TaggedPayload));
            if (tp->referenced() == true) {
               tp->clear_referenced();
               return true;
            }
            evict_key = real_key;
            evict_payload = tp->payload;
            victim_found = true;
            evict_keys.push_back(real_key);
            evict_payloads.emplace_back(*tp);
            if (evict_keys.size() >= 512) {
               return false;
            }
            return true;
         }, [&](){
            evict_keys.resize(evict_keys_size_snapshot);
            evict_payloads.resize(evict_keys_size_snapshot);
         });
         clock_hand = (clock_hand + 1) % hot_hash_table.countBuckets();
      }
      
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         ht_buffer_hit++;
      } else {
         ht_buffer_miss += new_miss - old_miss;
      }

      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            assert(is_in_hot_partition(key));
            // auto op_res = hot_hash_table.remove(key_bytes, fold(key_bytes, key));
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
            auto op_res = hot_hash_table.remove(key_bytes, fold(key_bytes, key));
            hot_partition_item_count--;
            assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
         }
         
         auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
         assert(io_reads_new >= io_reads_old);
         eviction_io_reads += io_reads_new - io_reads_old;
         eviction_items += evict_keys.size();
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
      auto res = hot_hash_table.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         tp->set_referenced();
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            leanstore::storage::hashing::OP_RESULT::OK;
      OLD_HIT_STAT_END;
      hot_ht_ios += new_miss - old_miss;
      if (res) {
         ++lookups_hit_top;
         return res;
      }

      auto cold_key = tag_with_cold_bit(k);
      res = cold_hash_table.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            leanstore::storage::hashing::OP_RESULT::OK;

      if (res) {
         if (should_migrate()) {
            ++upward_migrations;
            if (inclusive == false) {
               //OLD_HIT_STAT_START;
               // remove from the cold partition
               auto op_res __attribute__((unused))= cold_hash_table.remove(key_bytes, fold(key_bytes, cold_key));
               assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
               //OLD_HIT_STAT_END
            }
            // move to hot partition
            OLD_HIT_STAT_START;
            admit_element(k, v);
            OLD_HIT_STAT_END;
            hot_ht_ios += new_miss - old_miss;
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
      // auto op_res = cold_hash_table.updateSameSize(key_bytes, fold(key_bytes, key), [&](u8* payload, u16 payload_length) {
      //    TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
      //    tp->set_referenced();
      //    memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      // }, Payload::wal_update_generator);
      //HIT_STAT_END;

      auto op_res = cold_hash_table.upsert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
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
         auto op_res = hot_hash_table.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
         assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
         HIT_STAT_END;
      } else {
         auto op_res = cold_hash_table.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
         assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
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
      auto op_res = hot_hash_table.lookupForUpdate(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->set_referenced();
         tp->set_modified();
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
         return true;
      });
      HIT_STAT_END;
      hot_ht_ios += new_miss - old_miss;
      if (op_res == leanstore::storage::hashing::OP_RESULT::OK) {
         ++lookups_hit_top;
         return;
      }

      Payload old_v;
      auto cold_key = tag_with_cold_bit(k);
      //OLD_HIT_STAT_START;
      auto res __attribute__((unused)) = cold_hash_table.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&old_v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            leanstore::storage::hashing::OP_RESULT::OK;
      assert(res);
      //OLD_HIT_STAT_END;

      if (should_migrate()) {
         ++upward_migrations;
         OLD_HIT_STAT_START;
         admit_element(k, v, true);
         OLD_HIT_STAT_END;
         hot_ht_ios += new_miss - old_miss;
         if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
            //OLD_HIT_STAT_START;
            // remove from the cold partition
            auto op_res __attribute__((unused)) = cold_hash_table.remove(key_bytes, fold(key_bytes, cold_key));
            //OLD_HIT_STAT_END;
         }
      } else {
         bool res = cold_hash_table.lookupForUpdate(key_bytes, fold(key_bytes, cold_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
            TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
            tp->set_referenced();
            tp->set_modified();
            memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
            return true;
         }) == leanstore::storage::hashing::OP_RESULT::OK;
         assert(res);
      }
   }


   void put(Key k, Payload& v) override {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load(); try_eviction();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = hot_hash_table.lookupForUpdate(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->set_referenced();
         tp->set_modified();
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
         return true;
      });
      HIT_STAT_END;

      if (op_res == leanstore::storage::hashing::OP_RESULT::OK) {
         return;
      }

      if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
         auto cold_key = tag_with_cold_bit(k);
         //OLD_HIT_STAT_START;
         // remove from the cold partition
         auto op_res __attribute__((unused)) = cold_hash_table.remove(key_bytes, fold(key_bytes, cold_key));
         assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
         //OLD_HIT_STAT_END;
      }
      // move to hot partition
      admit_element(k, v, true);
   }

   double hot_hash_table_utilization(u64 entries, u64 pages) {
      u64 minimal_pages = (entries) * (sizeof(Key) + sizeof(TaggedPayload)) / leanstore::storage::PAGE_SIZE;
      return minimal_pages / (pages + 0.0);
   }

   double cold_hash_table_utilization(u64 entries, u64 pages) {
      u64 minimal_pages = (entries) * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      return minimal_pages / (pages + 0.0);
   }

   void report(u64 entries, u64 pages) override {
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      std::size_t hot_hash_table_pages = 0;
      auto num_hot_entries = hash_table_entries(hot_hash_table, hot_hash_table_pages);
      std::size_t cold_hash_table_pages = 0;
      auto num_cold_entries = hash_table_entries(cold_hash_table, cold_hash_table_pages);
      pages = hot_hash_table_pages + cold_hash_table_pages;
      auto num_hash_table_entries = num_hot_entries + num_cold_entries;

      std::cout << "Hot partition capacity in bytes " << hot_partition_capacity_bytes << std::endl;
      std::cout << "Hot partition size in bytes " << hot_partition_size_bytes << std::endl;
      std::cout << "Hash Table # entries " << num_hash_table_entries << std::endl;
      std::cout << "Hot Hash Table # hot entries " << num_hot_entries << std::endl;
      std::cout << "Hot Hash Table # pages " << hot_hash_table_pages << std::endl;
      std::cout << "Hot Hash Table # buckets " << hot_hash_table.countBuckets() << std::endl;
      std::cout << "Hot Hash Table power multiplier " << hot_hash_table.powerMultiplier() << std::endl;
      std::cout << "Cold Hash Table # hot entries " << num_cold_entries << std::endl;
      std::cout << "Cold Hash Table # pages " << cold_hash_table_pages << std::endl;
      std::cout << "Cold Hash Table # buckets " << cold_hash_table.countBuckets() << std::endl;
      std::cout << "Cold Hash Table power multiplier " << cold_hash_table.powerMultiplier() << std::endl;
      auto minimal_pages = (num_hash_table_entries) * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "Average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double btree_hit_rate = ht_buffer_hit / (ht_buffer_hit + ht_buffer_miss + 1.0);
      std::cout << "Hash Table buffer hits/misses " <<  ht_buffer_hit << "/" << ht_buffer_miss << std::endl;
      std::cout << "Hash Table buffer hit rate " <<  btree_hit_rate << " miss rate " << (1 - btree_hit_rate) << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
      std::cout << total_lookups<< " lookups, "  << lookups_hit_top << " lookup hit top" << " hot_ht_ios " << hot_ht_ios << " ios/tophit " << hot_ht_ios / (lookups_hit_top + 0.00)  << std::endl;
      std::cout << upward_migrations << " upward_migrations, "  << downward_migrations << " downward_migrations" << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01) << std::endl;
   }
};


}