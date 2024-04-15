#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/hashing/LinearHashing.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "stx/btree_map.h"
#include "leanstore/BTreeAdapter.hpp"
#include "common/DistributedCounter.hpp"
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

template <typename Key, typename Payload, typename Hasher1=leanstore::utils::XXH, typename Hasher2=leanstore::utils::XXH>
struct TwoHashAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::hashing::LinearHashTable & hot_hash_table;
   leanstore::storage::hashing::LinearHashTable & cold_hash_table;

   leanstore::storage::BufferManager * buf_mgr = nullptr;

   DistributedCounter<> hot_ht_ios = 0;
   DistributedCounter<> ht_buffer_miss = 0;
   DistributedCounter<> ht_buffer_hit = 0;
   OptimisticLockTable lock_table;
   DTID dt_id;
   alignas(64) std::atomic<uint64_t> bp_dirty_page_flushes_snapshot = 0;
   std::size_t hot_partition_capacity_bytes;
   DistributedCounter<> hot_partition_size_bytes = 0;
   DistributedCounter<> scan_ops = 0;
   DistributedCounter<> io_reads_scan = 0;
   bool inclusive = false;
   static constexpr double eviction_threshold = 0.65;
   DistributedCounter<> eviction_write_lock_fails = 0;
   DistributedCounter<> eviction_items = 0;
   DistributedCounter<> scanned_items = 0;
   DistributedCounter<> eviction_io_reads= 0;
   DistributedCounter<> io_reads_snapshot = 0;
   DistributedCounter<> io_reads_now = 0;
   DistributedCounter<> io_reads = 0;
   DistributedCounter<> hot_partition_item_count = 0;
   DistributedCounter<> upward_migrations = 0;
   DistributedCounter<> failed_upward_migrations = 0;
   DistributedCounter<> downward_migrations = 0;
   uint64_t buffer_pool_background_writes_last = 0;
   IOStats stats;
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
   DistributedCounter<> total_lookups = 0;
   DistributedCounter<> lookups_hit_top = 0;
   u64 clock_hand = 0;
   u64 clock_hand_bucket = 0;
   u64 number_records_to_scan_per_eviction = 1;
   u64 clock_hand_bucket_record_seq = 0;
   u64 clock_hand_bucket_record_count = std::numeric_limits<u64>::max();
   bool evict_by_records = false;
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
                  size_t eviction_scan_size, bool evict_by_records,
                  double hot_partition_size_gb, bool inclusive = false, int lazy_migration_sampling_rate = 100) 
                  : hot_hash_table(hot_hash_table), cold_hash_table(cold_hash_table), 
                  hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL), 
                  inclusive(inclusive), lazy_migration(lazy_migration_sampling_rate < 100), number_records_to_scan_per_eviction(eviction_scan_size), evict_by_records(evict_by_records) {
      cout << "eviction_scan_size=" << number_records_to_scan_per_eviction << std::endl;
      cout << "evict_by_records=" << evict_by_records << std::endl;
      hot_hash_table.register_hash_function(Hasher1::hash);
      cold_hash_table.register_hash_function(Hasher2::hash);
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   bool cache_under_pressure() {
      return buf_mgr->hot_pages.load() * leanstore::storage::PAGE_SIZE >=  hot_partition_capacity_bytes;
      //return hot_partition_size_bytes >= hot_partition_capacity_bytes * eviction_threshold;
      //return hot_hash_table.dataStored() / (hot_hash_table.dataPages() * leanstore::storage::PAGE_SIZE + 0.00001) >= hot_partition_capacity_bytes;
   }

   void clear_stats() override {
      upward_migrations = downward_migrations = 0;
      clear_io_stats();
   }

   void clear_io_stats() override {
      ht_buffer_miss = ht_buffer_hit = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      lookups_hit_top = total_lookups = 0;
      hot_ht_ios = 0;
      io_reads = 0;
      io_reads_scan = 0;
      bp_dirty_page_flushes_snapshot.store(this->buf_mgr->dirty_page_flushes.load());
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
   void evict_a_bunch() {
      if (evict_by_records) {
         evict_a_bunch_records();
      } else {
         evict_a_bunch_buckets();
      }
   }

   static constexpr u16 kClockWalkSteps = 1;
   alignas(64) std::atomic<u64> eviction_round_ref_counter[2];
   alignas(64) std::atomic<u64> eviction_round{0};
   alignas(64) std::mutex eviction_mutex;
   std::vector<Key> current_bucket_keys_for_eviction;
   void evict_a_bunch_records() {
      retry:
      u64 this_eviction_round;
      u64 bucket_record_seq_start;
      u64 bucket_record_seq_end;
      auto refill_current_bucket_keys_for_eviction = [&](u64 b){
         current_bucket_keys_for_eviction.clear();
         hot_hash_table.iterate(b,
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
               auto real_key = unfold(*(Key*)(key));
               current_bucket_keys_for_eviction.push_back(real_key);
               assert(key_length == sizeof(Key));
               return true;
            }, [&](){
               current_bucket_keys_for_eviction.clear();
            });
      };
      std::vector<Key> keys_to_scan;
      keys_to_scan.reserve(number_records_to_scan_per_eviction);
      {
         std::lock_guard<std::mutex> g(eviction_mutex);
         u32 buckets = hot_hash_table.countBuckets();
         if (clock_hand_bucket >= buckets) { 
            clock_hand_bucket = 0;// rewind
            clock_hand_bucket_record_seq = 0;
            refill_current_bucket_keys_for_eviction(clock_hand_bucket);
            eviction_round++; // end of the bucket scan, switch to the next round
         }
         
         while (clock_hand_bucket_record_seq >= current_bucket_keys_for_eviction.size()) {
            clock_hand_bucket = (clock_hand_bucket + 1) % buckets;
            clock_hand_bucket_record_seq = 0;
            refill_current_bucket_keys_for_eviction(clock_hand_bucket);
            if (clock_hand_bucket == 0) {
               eviction_round++;
            }
         }
         bucket_record_seq_start = clock_hand_bucket_record_seq;
         bucket_record_seq_end = bucket_record_seq_start + number_records_to_scan_per_eviction;
         if (bucket_record_seq_end > current_bucket_keys_for_eviction.size()) {
            bucket_record_seq_end = current_bucket_keys_for_eviction.size();
         }         
         this_eviction_round = eviction_round.load();
         
         if (eviction_round_ref_counter[1 - (this_eviction_round % 2)] != 0) {
            return; // There are evictors working in the previous round, retry later
         }

         assert(bucket_record_seq_end - bucket_record_seq_start > 0);
         keys_to_scan.insert(keys_to_scan.end(), current_bucket_keys_for_eviction.begin() + bucket_record_seq_start, current_bucket_keys_for_eviction.begin() + bucket_record_seq_end);
         assert(bucket_record_seq_end > clock_hand_bucket_record_seq);
         clock_hand_bucket_record_seq = bucket_record_seq_end;
         
         // Bump the ref count so that evictors in the next round won't start 
         // until all of the evictors in this round finish
         eviction_round_ref_counter[this_eviction_round % 2]++;
      }

      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      evict_keys.reserve(number_records_to_scan_per_eviction);
      std::vector<u64> evict_keys_lock_versions;
      evict_keys_lock_versions.reserve(number_records_to_scan_per_eviction);
      std::vector<TaggedPayload> evict_payloads;
      evict_payloads.reserve(number_records_to_scan_per_eviction);
      bool victim_found = false;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto evict_keys_size_snapshot = evict_keys.size();
      bool bucket_scan_finished = false;
      int records_to_scan = (int)number_records_to_scan_per_eviction;
      size_t records_scanned = 0;
      for (auto key_to_scan : keys_to_scan) {
         hot_hash_table.lookupForUpdate(key_bytes, fold(key_bytes, key_to_scan), [&](u8* value, u16 value_length __attribute__((unused)) ) {
            TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
            assert(value_length == sizeof(TaggedPayload));
            if (tp->referenced() == true) {
               tp->clear_referenced();
               return true;
            } else {
               LockGuardProxy g(&lock_table, key_to_scan);
               if (g.read_lock()) { // Skip evicting records that are write-locked
                  victim_found = true;
                  evict_keys.emplace_back(key_to_scan);
                  evict_payloads.emplace_back(*tp);
                  evict_keys_lock_versions.emplace_back(g.get_version());
               }
            }
            return false;
         });
      }
      
      scanned_items += keys_to_scan.size();
   
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
               eviction_write_lock_fails++;
               continue;
            }
            auto key_lock_version_from_scan = evict_keys_lock_versions[i];
            if (write_guard.more_than_one_writer_since(key_lock_version_from_scan)) {
               // There has been other writes to this key in the hot tree between the scan and the write locking above
               // Need to update the payload content by re-reading it from the hot tree
               bool mark_dirty = false;
               auto res = hot_hash_table.lookup(key_bytes, fold(key_bytes, key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
                  TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
                  evict_payloads[i] = *tp;
                  }, mark_dirty) == leanstore::storage::hashing::OP_RESULT::OK;
               if (!res) {
                  eviction_write_lock_fails++;
                  continue;
               }
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

   //std::mutex mtx;
   void evict_a_bunch_buckets() {
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
         int records_scanned = 0;
         hot_hash_table.iterate(b,
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
               auto real_key = unfold(*(Key*)(key));
               assert(key_length == sizeof(Key));
               records_scanned++;
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
               records_scanned = 0;
               evict_keys.resize(evict_keys_size_snapshot);
               evict_payloads.resize(evict_keys_size_snapshot);
               evict_keys_lock_versions.resize(evict_keys_size_snapshot);
            });
         scanned_items += records_scanned;
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
               eviction_write_lock_fails++;
               continue;
            }
            auto key_lock_version_from_scan = evict_keys_lock_versions[i];
            if (write_guard.more_than_one_writer_since(key_lock_version_from_scan)) {
               // There has been other writes to this key in the hot tree between the scan and the write locking above
               // Need to update the payload content by re-reading it from the hot tree
               bool mark_dirty = false;
               auto res = hot_hash_table.lookup(key_bytes, fold(key_bytes, key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
                  TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
                  evict_payloads[i] = *tp;
                  }, mark_dirty) == leanstore::storage::hashing::OP_RESULT::OK;
               if (!res) {
                  eviction_write_lock_fails++;
                  continue;
               }
            }

            auto op_res = hot_hash_table.remove(key_bytes, fold(key_bytes, key));
            assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
            hot_partition_item_count--;
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
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
      if (cache_under_pressure()) {
         DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
            auto new_reads = WorkerCounters::myCounters().io_reads.load();
            if (new_reads > old_reads) {
               stats.eviction_reads += new_reads - old_reads;
            }
         }, WorkerCounters::myCounters().io_reads.load());
         int steps = 10; // && cache_under_pressure()
         while (steps > 0) {
            evict_a_bunch();
            if (evict_by_records) {
               steps -= number_records_to_scan_per_eviction;
            } else {
               steps -= PAGE_SIZE / (sizeof(TaggedPayload) + sizeof(Key));
            }
         }
      }
   }

   void admit_element(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      insert_hot_partition(strip_off_partition_bit(k), v, dirty, referenced); // Hot partition
      hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
   }

   bool admit_element_might_fail(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      if (insert_hot_partition_might_fail(strip_off_partition_bit(k), v, dirty, referenced)) { // Hot partition
         hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
         return true;
      }; 
      return false;
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
      bp_dirty_page_flushes_snapshot.store(this->buf_mgr->dirty_page_flushes.load());
   }

   bool lookup(Key k, Payload & v) override {
      bool ret = false;
      try_eviction();
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
      return ret;
   }

   bool lookup_internal(Key k, Payload& v, LockGuardProxy & g)
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
         auto new_reads = WorkerCounters::myCounters().io_reads.load();
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, WorkerCounters::myCounters().io_reads.load());
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
            // move to hot partition
            bool res = false;
            if (utils::RandomGenerator::getRandU64(0, 100) < 5) {
               admit_element(k, v);
               res = true;
            } else {
               res = admit_element_might_fail(k, v);
            }
            OLD_HIT_STAT_END;
            hot_ht_ios += new_miss - old_miss;
            if (res) {
               ++upward_migrations;
            } else {
               ++failed_upward_migrations;
            }
            if (inclusive == false && res) {
               //OLD_HIT_STAT_START;
               // remove from the cold partition
               auto op_res __attribute__((unused))= cold_hash_table.remove(key_bytes, fold(key_bytes, cold_key));
               assert(op_res == leanstore::storage::hashing::OP_RESULT::OK);
               //OLD_HIT_STAT_END
            }
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

   bool insert_hot_partition_might_fail(Key k, Payload & v, bool modified = false, bool referenced = true) {
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
      auto op_res = hot_hash_table.insert_might_fail(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      if (op_res == leanstore::storage::hashing::OP_RESULT::OK) {
         return true;
      } else {
         assert(op_res == leanstore::storage::hashing::OP_RESULT::NOT_ENOUGH_SPACE);
         return false;
      }
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
         admit_element(k, v, true, false);
      }
   }

   bool remove([[maybe_unused]] Key k) override { 
      ensure(false); // not supported yet
   }

   void update(Key k, Payload& v) override {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      try_eviction();
      LockGuardProxy g(&lock_table, k);
      while (g.write_lock() == false);
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
         auto new_reads = WorkerCounters::myCounters().io_reads.load();
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, WorkerCounters::myCounters().io_reads.load());
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
         // move to hot partition
         bool res = false;
         if (utils::RandomGenerator::getRandU64(0, 100) < 5) {
            admit_element(k, v);
            res = true;
         } else {
            res = admit_element_might_fail(k, v);
         }
         OLD_HIT_STAT_END;
         hot_ht_ios += new_miss - old_miss;
         if (res) {
            ++upward_migrations;
         } else {
            ++failed_upward_migrations;
         }
         hot_ht_ios += new_miss - old_miss;
         if (inclusive == false && res) { // If exclusive, remove the tuple from the on-disk B-Tree
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
      DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
         auto new_reads = WorkerCounters::myCounters().io_reads.load();
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, WorkerCounters::myCounters().io_reads.load());
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

   double space_utilization(leanstore::storage::hashing::LinearHashTable::stat stat) {
      double space_utilization_avg_primary = stat.space_used_primary_pages / (stat.num_primary_pages * leanstore::storage::PAGE_SIZE + 0.0);
      double space_utilization_avg_overflow = stat.space_used_overflow_pages / (stat.num_overflow_pages * leanstore::storage::PAGE_SIZE + 0.0);
      double ratio_primary = stat.num_primary_pages / (stat.num_primary_pages + stat.num_overflow_pages + 0.0);
      double ratio_overflow = stat.num_overflow_pages / (stat.num_primary_pages + stat.num_overflow_pages + 0.0);
      return space_utilization_avg_primary * ratio_primary + space_utilization_avg_overflow * ratio_overflow;
   }

   double space_utilization(leanstore::storage::hashing::LinearHashTable::stat hot_stat, leanstore::storage::hashing::LinearHashTable::stat cold_stat) {
      double hot_util = space_utilization(hot_stat);
      double cold_util = space_utilization(cold_stat);
      double hot_pages = hot_stat.num_primary_pages + hot_stat.num_overflow_pages;
      double cold_pages = cold_stat.num_primary_pages + cold_stat.num_overflow_pages;
      return hot_util * hot_pages / (hot_pages + cold_pages) + cold_util * cold_pages / (hot_pages + cold_pages);
   }

   std::string hashTableStatToString(leanstore::storage::hashing::LinearHashTable::stat stat, const std::string name) {
      double space_utilization_avg_primary = stat.space_used_primary_pages / (stat.num_primary_pages * leanstore::storage::PAGE_SIZE + 0.0);
      double space_utilization_avg_overflow = stat.space_used_overflow_pages / (stat.num_overflow_pages * leanstore::storage::PAGE_SIZE + 0.0);
      return name + " primary # pages " + std::to_string(stat.num_primary_pages) + "\n" +
             name + " overflow # pages " + std::to_string(stat.num_overflow_pages) + "\n" +
             name + " # entries in primary pages  " + std::to_string(stat.num_entries_primary_page) + "\n" +
             name + " # entries in overflow pages  " + std::to_string(stat.num_entries_overflow_page) + "\n" +
             name + " avg utilization in primary pages " + std::to_string(space_utilization_avg_primary) + "\n" +
             name + " avg utilization in overflow pages " + std::to_string(space_utilization_avg_overflow);
   }

   void report([[maybe_unused]] u64 entries, u64 pages) override {
      assert(this->buf_mgr->dirty_page_flushes >= this->bp_dirty_page_flushes_snapshot);
      auto io_writes = this->buf_mgr->dirty_page_flushes - this->bp_dirty_page_flushes_snapshot;
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      auto stat_hot = hot_hash_table.get_stat();
      auto stat_cold = cold_hash_table.get_stat();
      auto stat_string_hot = hashTableStatToString(stat_hot, "Hot Hash Table");
      auto stat_string_cold = hashTableStatToString(stat_cold, "Cold Hash Table");
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      std::size_t hot_hash_table_pages = stat_hot.num_primary_pages + stat_hot.num_overflow_pages;
      auto num_hot_entries = stat_hot.num_entries_primary_page + stat_hot.num_entries_overflow_page;
      std::size_t cold_hash_table_pages = stat_cold.num_primary_pages + stat_cold.num_overflow_pages;
      auto num_cold_entries = stat_cold.num_entries_primary_page + stat_cold.num_entries_overflow_page;
      pages = hot_hash_table_pages + cold_hash_table_pages;
      auto num_hash_table_entries = num_hot_entries + num_cold_entries;

      std::cout << "Hot partition capacity in bytes " << hot_partition_capacity_bytes << std::endl;
      std::cout << "Hot partition size in bytes " << hot_partition_size_bytes << std::endl;
      std::cout << "Hash Table # entries " << num_hash_table_entries << std::endl;
      std::cout << stat_string_hot << std::endl;
      std::cout << "Hot Hash Table # entries " << num_hot_entries << std::endl;
      std::cout << "Hot Hash Table # pages " << hot_hash_table_pages << std::endl;
      std::cout << "Hot Hash Table # pages (fast) " << hot_hash_table.dataPages() << std::endl;
      std::cout << "Hot Hash Table bytes stored " << hot_hash_table.dataStored() << std::endl;
      std::cout << "Hot Hash Table # buckets " << hot_hash_table.countBuckets() << std::endl;
      std::cout << "Hot Hash Table merge chains " << hot_hash_table.get_merge_chains() << std::endl;
      std::cout << "Hot Hash Table power multiplier " << hot_hash_table.powerMultiplier() << std::endl;
      std::cout << "Hot Hash Table space utilization " << space_utilization(stat_hot) << std::endl;
      std::cout << stat_string_cold << std::endl;
      std::cout << "Cold Hash Table # entries " << num_cold_entries << std::endl;
      std::cout << "Cold Hash Table # pages " << cold_hash_table_pages << std::endl;
      std::cout << "Cold Hash Table # pages (fast) " << cold_hash_table.dataPages() << std::endl;
      std::cout << "Cold Hash Table bytes stored " << cold_hash_table.dataStored() << std::endl;
      std::cout << "Cold Hash Table # buckets " << cold_hash_table.countBuckets() << std::endl;
      std::cout << "Cold Hash Table power multiplier " << cold_hash_table.powerMultiplier() << std::endl;
      std::cout << "Cold Hash Table merge chains " << cold_hash_table.get_merge_chains() << std::endl;
      std::cout << "Cold Hash Table space utilization " << space_utilization(stat_cold) << std::endl;
      std::cout << "Average utilization " << space_utilization(stat_hot, stat_cold) << std::endl;
      double btree_hit_rate = ht_buffer_hit / (ht_buffer_hit + ht_buffer_miss + 1.0);
      std::cout << "Hash Table buffer hits/misses " <<  ht_buffer_hit << "/" << ht_buffer_miss << std::endl;
      std::cout << "Hash Table buffer hit rate " <<  btree_hit_rate << " miss rate " << (1 - btree_hit_rate) << std::endl;
      std::cout << "Scanned " << scanned_items.load() << " records, " << eviction_round.load() << " rounds, Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << " io_rws/eviction " << (eviction_io_reads + io_writes) / (eviction_items  + 1.0) << std::endl;
      std::cout << "eviction_write_lock_fails " << eviction_write_lock_fails.load() << std::endl;
      std::cout << total_lookups<< " lookups, " << io_reads.get() << " i/o reads, " << io_writes << " i/o writes, " << io_reads / (total_lookups + 0.00) << " i/o reads/lookup, "  << (io_reads + io_writes) / (total_lookups + 0.00) << " ios/lookup, " << lookups_hit_top << " lookup hit top" << " hot_ht_ios " << hot_ht_ios<< " ios/tophit " << hot_ht_ios / (lookups_hit_top + 0.00)  << std::endl;
      std::cout << upward_migrations << " upward_migrations, "  << downward_migrations << " downward_migrations, "<< failed_upward_migrations << " failed_upward_migrations" << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01) << std::endl;
   }

   std::vector<std::string> stats_column_names() { return {"hot_pages", "down_mig", "io_r", "io_w", "io_ev"}; }
   std::vector<std::string> stats_columns() { 
      IOStats empty;
      auto s = exchange_stats(empty);
      return {std::to_string(buf_mgr->hot_pages.load()), 
              std::to_string(downward_migrations.get()),
              std::to_string(s.reads),
              std::to_string(s.writes), 
              std::to_string(s.eviction_reads)}; 
   }

   IOStats exchange_stats(IOStats s) { 
      IOStats ret;
      auto t = buffer_pool_background_writes_last;
      buffer_pool_background_writes_last =  this->buf_mgr->dirty_page_flushes;
      ret.reads = stats.reads;
      ret.eviction_reads = stats.eviction_reads;
      ret.writes = buffer_pool_background_writes_last - t;
      stats = s;
      return ret;
   }
};


}