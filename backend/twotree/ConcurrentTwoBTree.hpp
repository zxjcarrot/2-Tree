#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "btreeolc/btreeolc.hpp"
#include "LockManager/LockManager.hpp"
#include "leanstore/utils/ScopedTimer.hpp"
#include "common/DistributedCounter.hpp"
#include "common/utils.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
template <typename Key, typename Payload>
struct ConcurrentTwoBTreeAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& hot_btree;
   leanstore::storage::btree::BTreeInterface& cold_btree;

   DistributedCounter<> hot_tree_ios;
   DistributedCounter<> btree_buffer_miss;
   DistributedCounter<> btree_buffer_hit;
   // alignas(64) uint64_t hot_tree_ios = 0;
   // alignas(64) uint64_t btree_buffer_miss = 0;
   // alignas(64) uint64_t btree_buffer_hit = 0;
   DTID dt_id;

   OptimisticLockTable lock_table;
   
   DistributedCounter<> hot_partition_size_bytes;
   DistributedCounter<> scan_ops;
   DistributedCounter<> io_reads_scan;
   alignas(64) std::atomic<uint64_t> bp_dirty_page_flushes_snapshot = 0;
   alignas(64) std::atomic<uint64_t> hot_partition_capacity_bytes;
   // alignas(64) std::size_t hot_partition_size_bytes = 0;
   // alignas(64) std::size_t scan_ops = 0;
   // alignas(64) std::size_t io_reads_scan = 0;
   bool inclusive = false;
   static constexpr double eviction_threshold = 0.7;

   DistributedCounter<> eviction_items;
   DistributedCounter<> eviction_io_reads;
   DistributedCounter<> io_reads_snapshot;
   DistributedCounter<> io_reads_now;
   DistributedCounter<> io_reads = 0;
   // alignas(64) uint64_t eviction_items = 0;
   // alignas(64) uint64_t eviction_io_reads= 0;
   // alignas(64) uint64_t io_reads_snapshot = 0;
   // alignas(64) uint64_t io_reads_now = 0;

   DistributedCounter<> hot_partition_item_count = 0;
   DistributedCounter<> upward_migrations = 0;
   DistributedCounter<> failed_upward_migrations = 0;
   DistributedCounter<> downward_migrations = 0;
   DistributedCounter<> eviction_bounding_time = 0;
   DistributedCounter<> eviction_bounding_n = 0;

   uint64_t buffer_pool_background_writes_last = 0;
   IOStats stats;
   leanstore::storage::BufferManager * buf_mgr = nullptr;

   // alignas(64) std::atomic<uint64_t> hot_partition_item_count = 0;
   // alignas(64) std::atomic<uint64_t> upward_migrations = 0;
   // alignas(64) std::atomic<uint64_t> downward_migrations = 0;
   // alignas(64) std::atomic<uint64_t> eviction_bounding_time = 0;
   // alignas(64) std::atomic<uint64_t> eviction_bounding_n = 0;

   u64 lazy_migration_threshold = 10;
   bool lazy_migration = false;
   constexpr static unsigned char kDeletedBits = 4;
   constexpr static unsigned char kReferenceMask = 0xE0; // We use top 3 bits as reference count.
   constexpr static unsigned char kReferenceOffset = 5; // We use top 3 bits as reference count.
   constexpr static unsigned char kDefaultReferenceCount = 4;
   constexpr static unsigned char kMaxReferenceCount = 7;
   struct alignas(1) TaggedPayload {
      Payload payload;
      unsigned char bits = 0;
      bool modified() const { return bits & 1; }
      void set_modified() { bits |= 1; }
      void clear_modified() { bits &= ~(1u); }

      unsigned char get_reference_count() { return bits >> kReferenceOffset; }
      void set_reference_count(unsigned char count) { bits = ((~kReferenceMask) & bits) | (count << kReferenceOffset); }
      void bump_reference_count() { 
         auto old_ref_count = get_reference_count();
         set_reference_count(old_ref_count == 7 ? 7: old_ref_count + 1);
      }
      void dec_reference_count() { 
         auto old_ref_count = get_reference_count();
         set_reference_count(old_ref_count == 0 ? 0: old_ref_count - 1); 
      }

      bool referenced() const { return bits & (2u); }
      void set_referenced() { bits |= (2u); }
      void clear_referenced() { bits &= ~(2u); }
      bool deleted() { return bits & (4u); }
      void set_deleted() { bits |= (4u); }
      void clear_deleted() { bits &= ~(4u); }
   };
   static_assert(sizeof(TaggedPayload) == sizeof(Payload) + 1, "!!!");
   DistributedCounter<> total_lookups = 0;
   DistributedCounter<> lookups_hit_top = 0;
   Key clock_hand = 0;
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

   ConcurrentTwoBTreeAdapter(leanstore::storage::btree::BTreeInterface& hot_btree, leanstore::storage::btree::BTreeInterface& cold_btree, double hot_partition_size_gb, bool inclusive = false, int lazy_migration_sampling_rate = 100) : hot_btree(hot_btree), cold_btree(cold_btree), hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL), inclusive(inclusive), lazy_migration(lazy_migration_sampling_rate < 100) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      eviction_round_ref_counter[0] = eviction_round_ref_counter[1] = 0;
   }

   bool cache_under_pressure() {
      return buf_mgr->hot_pages.load() * leanstore::storage::PAGE_SIZE >=  hot_partition_capacity_bytes;
   }

   bool cache_under_pressure_soft() {
      return buf_mgr->hot_pages.load() * leanstore::storage::PAGE_SIZE >=  hot_partition_capacity_bytes * 0.97; 
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
      io_reads = 0;
      eviction_bounding_time = eviction_bounding_n = 0;
      bp_dirty_page_flushes_snapshot.store(this->buf_mgr->dirty_page_flushes.load());
   }

   static std::size_t btree_entries(leanstore::storage::btree::BTreeInterface& btree, std::size_t & pages) {
      constexpr std::size_t scan_buffer_cap = 64;
      size_t scan_buffer_len = 0;
      Key keys[scan_buffer_cap];
      [[maybe_unused]] Payload payloads[scan_buffer_cap];
      bool tree_end = false;
      pages = 0;
      const char * last_leaf_frame = nullptr;
      u8 key_bytes[sizeof(Key)];
      auto fill_scan_buffer = [&](Key startk) {
         if (scan_buffer_len < scan_buffer_cap && tree_end == false) {
            btree.scanAsc(key_bytes, fold(key_bytes, startk),
            [&](const u8 * key, u16 key_length, [[maybe_unused]] const u8 * value, [[maybe_unused]] u16 value_length, const char * leaf_frame) -> bool {
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
         size_t idx = 0;
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
      while (hot_partition_item_count > 10) {
         evict_a_bunch();
      }
   }

   static constexpr u16 kClockWalkSteps = 10;
   alignas(64) std::atomic<u64> eviction_round_ref_counter[2];
   alignas(64) std::atomic<u64> eviction_round{0};
   alignas(64) std::mutex eviction_mutex;
   void evict_a_bunch(bool no_merge = false, int steps_to_walk = kClockWalkSteps) {
      [[maybe_unused]] u16 steps = steps_to_walk; // number of nodes to scan
      Key start_key;
      Key end_key;
      u64 this_eviction_round;
      u8 key_bytes[sizeof(Key)];
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      {
         std::lock_guard<std::mutex> g(eviction_mutex);
         start_key = clock_hand;
         end_key = start_key;
         this_eviction_round = eviction_round.load();
         bool rewind = false;
         bool triggered = false;
         
         if (eviction_round_ref_counter[1 - (this_eviction_round % 2)] != 0) {
            return; // There are evictors working in the previous round, retry later
         }
         ScopedTimer t([&](uint64_t ts){
            eviction_bounding_time += ts;
            eviction_bounding_n++;
         });

         hot_btree.findLeafNeighbouringNodeBoundary(key_bytes, fold(key_bytes, start_key), kClockWalkSteps,
            [&](const u8 * k, const u16 key_length, bool end) {
               assert(key_length == sizeof(Key));
               auto real_key = unfold(*(Key*)(k));
               end_key = real_key;
               rewind = end == true;
               triggered = true;
            });

         if (triggered == false) {
            return;
         }
         // Bump the ref count so that evictors in the next round won't start 
         // until all of the evictors in this round finish
         eviction_round_ref_counter[this_eviction_round % 2]++;
         if (rewind == true) {
            clock_hand = 0;
            eviction_round++; // end of the range scan, switch to the next round
         } else {
            clock_hand = end_key + 1;
         }
      }
      
      std::vector<Key> evict_keys;
      evict_keys.reserve(512);
      std::vector<u64> evict_keys_lock_versions;
      evict_keys_lock_versions.reserve(512);
      std::vector<TaggedPayload> evict_payloads;
      evict_payloads.reserve(512);
      Key evict_key;
      bool victim_found = false;
      
      hot_btree.scanAsc(key_bytes, fold(key_bytes, start_key),
      [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {

         auto real_key = unfold(*(Key*)(key));
         if (real_key > end_key) {
            return false;
         }
         assert(key_length == sizeof(Key));
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
         assert(value_length == sizeof(TaggedPayload));
         auto ref_cnt = tp->get_reference_count();
         tp->dec_reference_count();
         if (tp->referenced() == true) {
            tp->clear_referenced();
         } else {
            LockGuardProxy g(&lock_table, real_key);
            if (g.read_lock()) { // Skip evicting records that are write-locked
               evict_key = real_key;
               victim_found = true;
               evict_keys.emplace_back(real_key);
               evict_payloads.emplace_back(*tp);
               evict_keys_lock_versions.emplace_back(g.get_version());
            }
         }

         // if (ref_cnt == 0) {
         //    LockGuardProxy g(&lock_table, real_key);
         //    if (g.read_lock()) { // Skip evicting records that are write-locked
         //       evict_key = real_key;
         //       victim_found = true;
         //       evict_keys.emplace_back(real_key);
         //       evict_payloads.emplace_back(*tp);
         //       evict_keys_lock_versions.emplace_back(g.get_version());
         //    }
         // }

         if (real_key >= end_key) {
            return false;
         } else {
            return true;
         }
      }, [](){});

      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            LockGuardProxy write_guard(&lock_table, key);
            if (write_guard.write_lock() == false) { // Skip eviction if it is undergoing migration or modification
               continue;
            }
            auto key_lock_version_from_scan = evict_keys_lock_versions[i];
            if (write_guard.more_than_one_writer_since(key_lock_version_from_scan)) {
               // There has been other writes to this key in the hot tree between the scan and the write locking above
               // Need to update the payload content by re-reading it from the hot tree
               auto res = hot_btree.lookup(key_bytes, fold(key_bytes, key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
                  TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
                  evict_payloads[i] = *tp;
                  }) == OP_RESULT::OK;
               assert(res);
            }
            auto tagged_payload = &evict_payloads[i];
            assert(is_in_hot_partition(key));
            if (inclusive) {
               if (tagged_payload->modified()) {
                  upsert_cold_partition(strip_off_partition_bit(key), tagged_payload->payload); // Cold partition
                  ++downward_migrations;
               }
            } else { // exclusive, put it back in the on-disk B-Tree
               insert_cold_partition(strip_off_partition_bit(key), tagged_payload->payload); // Cold partition
               ++downward_migrations;
            }

            assert(is_in_hot_partition(key));
            auto op_res = hot_btree.remove(key_bytes, fold(key_bytes, key), no_merge);
            hot_partition_item_count--;
            assert(op_res == OP_RESULT::OK);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
         }

         auto io_reads_new = WorkerCounters::myCounters().io_reads.load();
         assert(io_reads_new >= io_reads_old);
         eviction_io_reads += io_reads_new - io_reads_old;
         eviction_items += evict_keys.size();
      }

      eviction_round_ref_counter[this_eviction_round % 2]--;
   }


   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_a_bunch();
      }
   }
   
   static constexpr s64 kEvictionCheckInterval = 30;
   DistributedCounter<> eviction_count_down {kEvictionCheckInterval};

   void try_eviction() {
      if (cache_under_pressure()) {
         DeferCodeWithContext cc([&, this](uint64_t old_reads) {
            auto new_reads = WorkerCounters::myCounters().io_reads.load();
            if (new_reads > old_reads) {
               stats.eviction_reads += new_reads - old_reads;
            }
         }, WorkerCounters::myCounters().io_reads.load());
         evict_a_bunch(false);
      }
   }

   bool admit_element_might_fail(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      if (insert_hot_partition_might_fail(strip_off_partition_bit(k), v, dirty, referenced)) {
         hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
         return true;
      }
      return false;
   }

   void admit_element(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      insert_hot_partition(strip_off_partition_bit(k), v, dirty, referenced);
      hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
   }

   void admit_delete_mark_element(Key k) {
      insert_hot_partition_deleted_mark(strip_off_partition_bit(k));
      hot_partition_size_bytes += sizeof(Key) + sizeof(kDeletedBits);
   }
   
   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]] int length) {
      scan_ops++;
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
         auto new_reads = WorkerCounters::myCounters().io_reads.load();
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, WorkerCounters::myCounters().io_reads.load());
      constexpr std::size_t scan_buffer_size = 8;
      u8 key_bytes[sizeof(Key)];
      size_t hot_len = 0;
      Key hot_keys[scan_buffer_size];
      Payload hot_payloads[scan_buffer_size];
      size_t cold_len = 0;
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

      size_t hot_idx = 0;
      size_t cold_idx = 0;
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

   void set_buffer_manager(storage::BufferManager * buf_mgr) { 
      this->buf_mgr = buf_mgr;
      bp_dirty_page_flushes_snapshot.store(this->buf_mgr->dirty_page_flushes.load());
   }

   bool lookup(Key k, Payload & v) {
      try_eviction();
      bool should_evict = false;
      bool res = false;
      while (true) {
         LockGuardProxy g(&lock_table, k);
         if (!g.read_lock()) {
            continue;
         }
         bool migration_failed = false;
         res = lookup_internal(k, v, g, migration_failed);
         if (migration_failed) {
            should_evict = true;
         }
         if (g.validate()) {
            break;
         }
      }
      // if (should_evict) {
      //    evict_a_bunch(true);
      // }
      return res;
   }

   bool lookup_internal(Key k, Payload& v, LockGuardProxy & g, bool & migration_failed)
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
         auto new_reads = WorkerCounters::myCounters().io_reads.load();
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, WorkerCounters::myCounters().io_reads.load());
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      // try hot partition first
      auto hot_key = tag_with_hot_bit(k);
      uint64_t old_miss, new_miss;
      bool deleted = false;
      OLD_HIT_STAT_START;
      bool mark_dirty = false;
      auto res = hot_btree.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         if (payload_length == 1) {
            deleted = true;
            assert(*((unsigned char *) payload) == kDeletedBits);
         } else {
            TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
            if (tp->referenced() == false) {
               tp->set_referenced();
               //mark_dirty = utils::RandomGenerator::getRandU64(0, 100) < 10;
            }
            if (tp->get_reference_count() < kMaxReferenceCount) {
               tp->bump_reference_count();
            }
            memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }
         }, mark_dirty) ==
            OP_RESULT::OK;
      OLD_HIT_STAT_END;
      //hot_tree_ios += new_miss - old_miss;
      if (deleted) {
         return false;
      }
      if (res) {
         //++lookups_hit_top;
         return res;
      }

      auto cold_key = tag_with_cold_bit(k);
      res = cold_btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
         Payload *pl =  const_cast<Payload*>(reinterpret_cast<const Payload*>(payload));
         memcpy(&v, pl, sizeof(Payload)); 
         }) ==
            OP_RESULT::OK;

      if (res) {
         if (should_migrate()) {
            if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
               return false;
            }
            
            // move to hot partition
            bool res = false;
            OLD_HIT_STAT_START;
            if (cache_under_pressure_soft() && !cache_under_pressure()) {
               if (utils::RandomGenerator::getRandU64(0, 100) < 5) {
                   admit_element(k, v, false, false);
                   res = true;
               } else {
                  res = admit_element_might_fail(k, v, false, false);
               }
            } else {
               admit_element(k, v, false, false);
               res = true;
            }
            //admit_element(k, v, false, false);
            //res = true;

            OLD_HIT_STAT_END;
            hot_tree_ios += new_miss - old_miss;
            if (res) {
               ++upward_migrations;
            } else {
               ++failed_upward_migrations;
               migration_failed = true;
            }
            if (inclusive == false && res) {
               //OLD_HIT_STAT_START;
               // remove from the cold partition
               auto op_res __attribute__((unused))= cold_btree.remove(key_bytes, fold(key_bytes, cold_key));
               assert(op_res == OP_RESULT::OK);
               //OLD_HIT_STAT_END
            }
         }
      }
      return res;
   }

   void upsert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);

      auto op_res = cold_btree.upsert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&v), sizeof(Payload));
      assert(op_res == OP_RESULT::OK);
   }

   // hot_or_cold: false => hot partition, true => cold partition
   bool insert_hot_partition_might_fail(Key k, Payload & v, bool modified = false, bool referenced = true) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_hot_bit(k);
      TaggedPayload tp;
      if (referenced == false) {
         tp.clear_referenced();
      } else {
         tp.set_referenced();
         tp.bump_reference_count();
      }

      if (modified) {
         tp.set_modified();
      } else {
         tp.clear_modified();
      }
      tp.payload = v;
      
      HIT_STAT_START;
      auto op_res = hot_btree.insert_might_fail(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      HIT_STAT_END;
      if (op_res != OP_RESULT::OK) {
         assert(op_res == OP_RESULT::NOT_ENOUGH_SPACE);
         return false;
      } else {
         hot_partition_item_count++;
         return true;
      }
   }

   // hot_or_cold: false => hot partition, true => cold partition
   void insert_hot_partition(Key k, Payload & v, bool modified = false, bool referenced = true) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_hot_bit(k);
      hot_partition_item_count++;
      TaggedPayload tp;
      if (referenced == false) {
         tp.clear_referenced();
      } else {
         tp.set_referenced();
         tp.bump_reference_count();
      }

      if (modified) {
         tp.set_modified();
      } else {
         tp.clear_modified();
      }
      tp.payload = v;
      
      HIT_STAT_START;
      auto op_res = hot_btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      assert(op_res == OP_RESULT::OK);
      HIT_STAT_END;
   }

   void insert_hot_partition_deleted_mark(Key k) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_hot_bit(k);
      hot_partition_item_count++;
      HIT_STAT_START;
      auto op_res = hot_btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(const_cast<unsigned char *>(&kDeletedBits)), sizeof(kDeletedBits));
      assert(op_res == OP_RESULT::OK);
      HIT_STAT_END;
   }

   void insert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);
      auto op_res = cold_btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&v), sizeof(Payload));
      assert(op_res == OP_RESULT::OK);
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

   bool remove([[maybe_unused]] Key k) {
      LockGuardProxy g(&lock_table, k);
      while (g.write_lock() == false);
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      DeferCodeWithContext cc([&, this](uint64_t old_reads) {
         auto new_reads = WorkerCounters::myCounters().io_reads.load();
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, WorkerCounters::myCounters().io_reads.load());
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      bool deleted = false;
      HIT_STAT_START;
      auto op_res = hot_btree.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         if (payload_length == 1) {
            assert(*(unsigned char*)payload == kDeletedBits);
            deleted = true;
         }
         });
      HIT_STAT_END;
      if (deleted == true) {
         return true;
      } else {
         if (op_res == OP_RESULT::NOT_FOUND) {
            admit_delete_mark_element(k);
         } else if (op_res == OP_RESULT::OK) {
            op_res = hot_btree.remove(key_bytes, fold(key_bytes, k));
            hot_partition_item_count--;
            assert(op_res == OP_RESULT::OK);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
            admit_delete_mark_element(k);
         } else {
            ensure(false);
         }
         return true;
      }
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
      bool deleted = false;
      HIT_STAT_START;
      auto op_res = hot_btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         if (payload_length == 1) {
            assert(*(unsigned char*)(payload) == kDeletedBits);
            deleted = true;
         } else {
            TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
            tp->set_referenced();
            tp->set_modified();
            memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
         }
      }, Payload::wal_update_generator);
      HIT_STAT_END;
      hot_tree_ios += new_miss - old_miss;

      if (op_res == OP_RESULT::OK || deleted) {
         ++lookups_hit_top;
         return;
      }

      Payload old_v;
      auto cold_key = tag_with_cold_bit(k);
      //OLD_HIT_STAT_START;
      auto res __attribute__((unused)) = cold_btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         Payload *pl =  const_cast<Payload*>(reinterpret_cast<const Payload*>(payload));
         memcpy(&old_v, pl, sizeof(Payload)); 
         }) ==
            OP_RESULT::OK;
      assert(res);
      //OLD_HIT_STAT_END;

      if (should_migrate()) {
         // move to hot partition
         bool res = false;
         OLD_HIT_STAT_START;
         if (cache_under_pressure_soft()) {
            res = admit_element_might_fail(k, v);
         } else {
            admit_element(k, v, true, false);
            res = true;
         }
         OLD_HIT_STAT_END;
         if (res) {
            ++upward_migrations;
         } else {
            ++failed_upward_migrations;
         }
         if (inclusive == false && res) { // If exclusive, remove the tuple from the on-disk B-Tree
            //OLD_HIT_STAT_START;
            // remove from the cold partition
            auto op_res __attribute__((unused)) = cold_btree.remove(key_bytes, fold(key_bytes, cold_key));
            //OLD_HIT_STAT_END;
         }
      } else {
         cold_btree.updateSameSize(key_bytes, fold(key_bytes, cold_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
            Payload *pl =  reinterpret_cast<Payload*>(payload);
            memcpy(pl, &v, sizeof(Payload)); 
         }, Payload::wal_update_generator);
      }
   }


   void put(Key k, Payload& v) override {
      retry:
      try_eviction();
      LockGuardProxy g(&lock_table, k);
      while (g.write_lock() == false);
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      DeferCodeWithContext cc([&, this](uint64_t old_reads) {
         auto new_reads = WorkerCounters::myCounters().io_reads.load();
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, WorkerCounters::myCounters().io_reads.load());
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
      // move to hot partition
      admit_element(k, v, true, false);

      if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
         auto cold_key = tag_with_cold_bit(k);
         //OLD_HIT_STAT_START;
         // remove from the cold partition
         auto op_res __attribute__((unused)) = cold_btree.remove(key_bytes, fold(key_bytes, cold_key));
         assert(op_res == OP_RESULT::OK);
         //OLD_HIT_STAT_END;
      }
   }

   double hot_btree_utilization(u64 entries, u64 pages) {
      u64 minimal_pages = (entries) * (sizeof(Key) + sizeof(TaggedPayload) + sizeof(leanstore::storage::btree::BTreeNode::Slot)) / (leanstore::storage::PAGE_SIZE - sizeof(leanstore::storage::btree::BTreeNodeHeader));
      return minimal_pages / (pages + 0.0);
   }

   double cold_btree_utilization(u64 entries, u64 pages) {
      u64 minimal_pages = (entries) * (sizeof(Key) + sizeof(Payload) + sizeof(leanstore::storage::btree::BTreeNode::Slot)) / (leanstore::storage::PAGE_SIZE - sizeof(leanstore::storage::btree::BTreeNodeHeader));
      return minimal_pages / (pages + 0.0);
   }

   void report([[maybe_unused]] u64 entries, u64 pages) override {
      assert(this->buf_mgr->dirty_page_flushes >= this->bp_dirty_page_flushes_snapshot);
      auto io_writes = this->buf_mgr->dirty_page_flushes - this->bp_dirty_page_flushes_snapshot;
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
      auto minimal_pages = (num_btree_entries) * (sizeof(Key) + sizeof(Payload) + sizeof(leanstore::storage::btree::BTreeNode::Slot)) / (leanstore::storage::PAGE_SIZE - sizeof(leanstore::storage::btree::BTreeNodeHeader));
      std::cout << "BTree average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double btree_hit_rate = btree_buffer_hit / (btree_buffer_hit + btree_buffer_miss + 1.0);
      std::cout << "BTree buffer hits/misses " <<  btree_buffer_hit << "/" << btree_buffer_miss << std::endl;
      std::cout << "BTree buffer hit rate " <<  btree_hit_rate << " miss rate " << (1 - btree_hit_rate) << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << " io_rws/eviction " << (eviction_io_reads + io_writes) / (eviction_items  + 1.0) << std::endl;
      std::cout << total_lookups<< " lookups, " << io_reads.get() << " i/o reads, " << io_writes << " i/o writes, " << io_reads / (total_lookups + 0.00) << " i/o reads/lookup, "  << (io_reads + io_writes) / (total_lookups + 0.00) << " ios/lookup, " << lookups_hit_top << " lookup hit top" << " hot_tree_ios " << hot_tree_ios<< " ios/tophit " << hot_tree_ios / (lookups_hit_top + 0.00)  << std::endl;
      std::cout << upward_migrations << " upward_migrations, "  << downward_migrations << " downward_migrations, "<< failed_upward_migrations << " failed_upward_migrations" << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01) << std::endl;
      std::cout << "Average eviction bounding time " << eviction_bounding_time / (eviction_bounding_n + 1) << std::endl;
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