#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/hashing/LinearHashing.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "stx/btree_map.h"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/utils/DistributedCounter.hpp"
#include "LockManager/LockManager.hpp"
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
   OptimisticLockTable lock_table;
   leanstore::storage::hashing::LinearHashTable & hot_hash_table;
   leanstore::storage::hashing::LinearHashTable & cold_hash_table;

   leanstore::storage::BufferManager * buf_mgr = nullptr;

   DistributedCounter<> hot_ht_ios = 0;
   DistributedCounter<> ht_buffer_miss = 0;
   DistributedCounter<> ht_buffer_hit = 0;
   DTID dt_id;
   std::size_t hot_partition_capacity_bytes;
   DistributedCounter<> hot_partition_size_bytes = 0;
   DistributedCounter<> scan_ops = 0;
   DistributedCounter<> io_reads_scan = 0;
   bool inclusive = false;
   static constexpr double eviction_threshold = 0.5;
   DistributedCounter<> eviction_items = 0;
   DistributedCounter<> eviction_io_reads= 0;
   DistributedCounter<> io_reads_snapshot = 0;
   DistributedCounter<> io_reads_now = 0;
   DistributedCounter<> hot_partition_item_count = 0;
   DistributedCounter<> upward_migrations = 0;
   DistributedCounter<> downward_migrations = 0;
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
      //return buf_mgr->hot_pages.load() * leanstore::storage::PAGE_SIZE >=  hot_partition_capacity_bytes;
      return hot_partition_size_bytes >= hot_partition_capacity_bytes * eviction_threshold;
      //return hot_hash_table.dataStored() / (hot_hash_table.dataPages() * leanstore::storage::PAGE_SIZE + 0.00001) >= hot_partition_capacity_bytes;
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

   static constexpr u16 kClockWalkSteps = 4;
   alignas(64) std::atomic<u64> eviction_round_ref_counter[2];
   alignas(64) std::atomic<u64> eviction_round{0};
   alignas(64) std::mutex eviction_mutex;
   void evict_a_bunch() {
      u64 this_eviction_round;
      u32 start_bucket;
      u32 end_bucket;
      {
         std::lock_guard<std::mutex> g(eviction_mutex);
         start_bucket = clock_hand;
         end_bucket = clock_hand + kClockWalkSteps;
         u32 buckets = hot_hash_table.countBuckets();
         this_eviction_round = eviction_round.load();
         
         if (eviction_round_ref_counter[1 - (this_eviction_round % 2)] != 0) {
            return; // There are evictors working in the previous round, retry later
         }

         if (end_bucket > buckets) { // rewind
            end_bucket = buckets;
            clock_hand = 0;
            eviction_round++; // end of the bucket scan, switch to the next round
         } else {
            clock_hand = end_bucket;
         }

         // Bump the ref count so that evictors in the next round won't start 
         // until all of the evictors in this round finish
         eviction_round_ref_counter[this_eviction_round % 2]++;
      }

      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      evict_keys.reserve(512);
      std::vector<u64> evict_keys_lock_versions;
      evict_keys_lock_versions.reserve(512);
      std::vector<TaggedPayload> evict_payloads;
      evict_payloads.reserve(512);
      bool victim_found = false;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      for (size_t b = start_bucket; b != end_bucket; ++b) {
         auto evict_keys_size_snapshot = evict_keys.size();
         hot_hash_table.iterate(b,
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
               auto real_key = unfold(*(Key*)(key));
               assert(key_length == sizeof(Key));
               TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
               assert(value_length == sizeof(TaggedPayload));
               if (tp->referenced() == true) {
                  tp->clear_referenced();
               } else {
                  LockGuardProxy g(&lock_table, real_key);
                  if (g.read_lock()) { // Skip evicting records that are write-locked
                     victim_found = true;
                     evict_keys.emplace_back(real_key);
                     evict_payloads.emplace_back(*tp);
                     evict_keys_lock_versions.emplace_back(g.get_version());
                  }
               }
               return true;
            }, [&](){
               evict_keys.resize(evict_keys_size_snapshot);
               evict_payloads.resize(evict_keys_size_snapshot);
               evict_keys_lock_versions.resize(evict_keys_size_snapshot);
            });
      }
   
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         ht_buffer_hit++;
      } else {
         ht_buffer_miss += new_miss - old_miss;
      }

      if (victim_found) {
         std::size_t evicted_count = 0;
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            LockGuardProxy write_guard(&lock_table, key);
            if (write_guard.write_lock() == false) { // Skip eviction if it is undergoing migration or modification
               continue;
            }
            auto key_lock_version_from_scan = evict_keys_lock_versions[i];
            if (write_guard.more_than_one_writer_since(key_lock_version_from_scan)) {
               //continue;
               // There has been other writes to this key in the hot tree between the scan and the write locking above
               // Need to update the payload content by re-reading it from the hot tree
               bool mark_dirty = false;
               auto res = hot_hash_table.lookup(key_bytes, fold(key_bytes, key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
                  TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
                  evict_payloads[i] = *tp;
                  }, mark_dirty) == leanstore::storage::hashing::OP_RESULT::OK;
               assert(res);
            }
            evicted_count++;
            auto tagged_payload = evict_payloads[i];
            assert(is_in_hot_partition(key));
            if (inclusive) {
               if (tagged_payload.modified()) {
                  upsert_cold_partition(strip_off_partition_bit(key), tagged_payload.payload); // Cold partition
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_cold_partition(strip_off_partition_bit(key), tagged_payload.payload); // Cold partition
            }
            assert(is_in_hot_partition(key));
            auto op_res = hot_hash_table.remove(key_bytes, fold(key_bytes, key));
            hot_partition_item_count--;
            assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
            ++downward_migrations;
         }
         auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
         assert(io_reads_new >= io_reads_old);
         eviction_io_reads += io_reads_new - io_reads_old;
         eviction_items += evicted_count;
      }

      eviction_round_ref_counter[this_eviction_round % 2]--;
   }


   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_a_bunch();
      }
   }
   
   static constexpr int kEvictionCheckInterval = 300;
   DistributedCounter<> eviction_count_down = kEvictionCheckInterval;
   void try_eviction() {
      --eviction_count_down;
      if (eviction_count_down.load() <= 0) {
         if (cache_under_pressure()) {
            evict_a_bunch();
         }
         eviction_count_down = kEvictionCheckInterval;
      }
   }

   void admit_element(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      insert_hot_partition(strip_off_partition_bit(k), v, dirty, referenced); // Hot partition
      hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
   }
   
   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void scan([[maybe_unused]] Key start_key, [[maybe_unused]] std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]] int length) { ensure(false); }
   
   void set_buffer_manager(storage::BufferManager * buf_mgr) { 
      this->buf_mgr = buf_mgr;
   }

   bool lookup(Key k, Payload & v) override {
      bool ret = false;
      while (true) {
         LockGuardProxy g(&lock_table, k);
         if (!g.read_lock()) {
            continue;
         }
         bool res = lookup_internal(k, v, g);
         if (g.validate()) {
            ret = res;
            break;
         }
      }
      if (ret == true) {
         try_eviction();
      }
      return ret;
   }

   bool lookup_internal(Key k, Payload& v, LockGuardProxy & g)
   {
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      // try hot partition first
      auto hot_key = tag_with_hot_bit(k);
      uint64_t old_miss, new_miss;
      OLD_HIT_STAT_START;
      bool mark_dirty = false;
      auto res = hot_hash_table.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         if (tp->referenced() == false) {
            tp->set_referenced();
            mark_dirty = true;
         }
         
         memcpy(&v, &tp->payload, sizeof(tp->payload)); 
         }, mark_dirty) ==
            leanstore::storage::hashing::OP_RESULT::OK;
      OLD_HIT_STAT_END;
      hot_ht_ios += new_miss - old_miss;
      if (res) {
         ++lookups_hit_top;
         return res;
      }

      auto cold_key = tag_with_cold_bit(k);
      mark_dirty = false;
      res = cold_hash_table.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
         Payload *pl =  const_cast<Payload*>(reinterpret_cast<const Payload*>(payload));
         memcpy(&v, pl, sizeof(Payload)); 
         }, mark_dirty) ==
            leanstore::storage::hashing::OP_RESULT::OK;

      if (res) {
         if (should_migrate()) {
            if (g.upgrade_to_write_lock() == false) {
               return false;
            }
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

      auto op_res = cold_hash_table.upsert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&v), sizeof(Payload));
      assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
   }

   void insert_hot_partition(Key k, Payload & v, bool modified = false, bool referenced = true) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_hot_bit(k);
      hot_partition_item_count++;
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
      
      HIT_STAT_START;
      auto op_res = hot_hash_table.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
      HIT_STAT_END;
   }

   void insert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);
      auto op_res = cold_hash_table.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&v), sizeof(Payload));
      assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
   }

   void insert(Key k, Payload& v) override
   {
      try_eviction();
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      LockGuardProxy g(&lock_table, k);
      
      while (g.write_lock() == false);

      if (inclusive) {
         admit_element(k, v, true, false);
      } else {
         admit_element(k, v);
      }
   }

   bool remove([[maybe_unused]] Key k) override { 
      ensure(false); // not supported yet
   }

   void update(Key k, Payload& v) override {
      try_eviction();
      LockGuardProxy g(&lock_table, k);
      while (g.write_lock() == false);
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = hot_hash_table.lookupForUpdate(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         bool ret = false;
         if (tp->referenced() == false) {
            tp->set_referenced();
            ret = true;
         }
         if (tp->modified() == false) {
            tp->set_modified();
            ret = true;
         }

         memcpy(&tp->payload, &v, sizeof(Payload)); 
         return ret;
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
      bool mark_dirty = false;
      auto res __attribute__((unused)) = cold_hash_table.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         Payload *pl =  const_cast<Payload*>(reinterpret_cast<const Payload*>(payload));
         memcpy(&old_v, pl, sizeof(Payload)); 
         }, mark_dirty);
         
      if(res == leanstore::storage::hashing::OP_RESULT::NOT_FOUND){
         return;
      }
      assert(res == leanstore::storage::hashing::OP_RESULT::OK);
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
            Payload *pl =  reinterpret_cast<Payload*>(payload);
            memcpy(pl, &v, sizeof(Payload)); 
            return true;
         }) == leanstore::storage::hashing::OP_RESULT::OK;
         assert(res);
      }
   }


   void put(Key k, Payload& v) override {
      try_eviction();
      LockGuardProxy g(&lock_table, k);
      while (g.write_lock() == false);
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = hot_hash_table.lookupForUpdate(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         bool ret = false;
         if (tp->referenced() == false) {
            tp->set_referenced();
            ret = true;
         }
         if (tp->modified() == false) {
            tp->set_modified();
            ret = true;
         }

         memcpy(&tp->payload, &v, sizeof(Payload)); 
         return ret;
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

   void report([[maybe_unused]] u64 entries, u64 pages) override {
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
      std::cout << "Hot Hash Table # entries " << num_hot_entries << std::endl;
      std::cout << "Hot Hash Table # pages " << hot_hash_table_pages << std::endl;
      std::cout << "Hot Hash Table # pages (fast) " << hot_hash_table.dataPages() << std::endl;
      std::cout << "Hot Hash Table bytes stored " << hot_hash_table.dataStored() << std::endl;
      std::cout << "Hot Hash Table # buckets " << hot_hash_table.countBuckets() << std::endl;
      std::cout << "Hot Hash Table power multiplier " << hot_hash_table.powerMultiplier() << std::endl;
      std::cout << "Cold Hash Table # entries " << num_cold_entries << std::endl;
      std::cout << "Cold Hash Table # pages " << cold_hash_table_pages << std::endl;
      std::cout << "Cold Hash Table # pages (fast) " << cold_hash_table.dataPages() << std::endl;
      std::cout << "Cold Hash Table bytes stored " << cold_hash_table.dataStored() << std::endl;
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