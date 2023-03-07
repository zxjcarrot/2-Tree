#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/heap/HeapFile.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "leanstore/utils/DistributedCounter.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
using IHF_OP_RESULT = leanstore::storage::heap::OP_RESULT;
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
         heap_buffer_hit++; \
      } else { \
         heap_buffer_miss += new_miss - old_miss; \
      }

#define OLD_HIT_STAT_START \
   old_miss = WorkerCounters::myCounters().io_reads.load();

#define OLD_HIT_STAT_END \
      new_miss = WorkerCounters::myCounters().io_reads.load(); \
      assert(new_miss >= old_miss); \
      if (old_miss == new_miss) { \
         heap_buffer_hit++; \
      } else { \
         heap_buffer_miss += new_miss - old_miss; \
      }

template <typename Key, typename Payload>
struct TwoIHeapAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::heap::IndexedHeapFile hot_iheap;
   leanstore::storage::heap::IndexedHeapFile cold_iheap;

   DistributedCounter<> hot_iheap_ios;
   DistributedCounter<> heap_buffer_miss;
   DistributedCounter<> heap_buffer_hit;
   // alignas(64) uint64_t hot_iheap_ios = 0;
   // alignas(64) uint64_t heap_buffer_miss = 0;
   // alignas(64) uint64_t heap_buffer_hit = 0;
   DTID dt_id;

   OptimisticLockTable lock_table;
   
   DistributedCounter<> hot_partition_size_bytes;
   DistributedCounter<> scan_ops;
   DistributedCounter<> io_reads_scan;
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

   leanstore::storage::BufferManager * buf_mgr = nullptr;

   // alignas(64) std::atomic<uint64_t> hot_partition_item_count = 0;
   // alignas(64) std::atomic<uint64_t> upward_migrations = 0;
   // alignas(64) std::atomic<uint64_t> downward_migrations = 0;
   // alignas(64) std::atomic<uint64_t> eviction_bounding_time = 0;
   // alignas(64) std::atomic<uint64_t> eviction_bounding_n = 0;

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

   TwoIHeapAdapter(leanstore::storage::heap::HeapFile& hot_heap_file, leanstore::storage::btree::BTreeLL& hot_btree_index,
            leanstore::storage::heap::HeapFile& cold_heap_file, leanstore::storage::btree::BTreeLL& cold_btree_index, 
            double hot_partition_size_gb, bool inclusive = false, int lazy_migration_sampling_rate = 100) : 
            hot_iheap(hot_heap_file, hot_btree_index), cold_iheap(cold_heap_file, cold_btree_index), 
            hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL), 
            inclusive(inclusive), lazy_migration(lazy_migration_sampling_rate < 100) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      eviction_round_ref_counter[0] = eviction_round_ref_counter[1] = 0;
   }

   bool cache_under_pressure() {
      if (!hot_iheap.is_heap_growable()) {
         return hot_iheap.utilization() >= 0.9;
      }
      if (buf_mgr->hot_pages.load() * leanstore::storage::PAGE_SIZE >= hot_partition_capacity_bytes) {
         if (hot_iheap.is_heap_growable()) {
            hot_iheap.disable_heap_growth();
         }
         return hot_iheap.utilization() >= 0.9;
      } else {
         return false;
      }
   }

   bool cache_under_pressure_soft() {
      return buf_mgr->hot_pages.load() * leanstore::storage::PAGE_SIZE >=  hot_partition_capacity_bytes * 0.9; 
   }

   void clear_stats() override {
      heap_buffer_miss = heap_buffer_hit = 0;
      eviction_items = eviction_io_reads = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      lookups_hit_top = total_lookups = 0;
      hot_iheap_ios = 0;
      upward_migrations = downward_migrations = 0;
      io_reads_scan = 0;
      scan_ops = 0;
      io_reads = 0;
      eviction_bounding_time = eviction_bounding_n = 0;
   }

   void evict_all() override {
      while (hot_partition_item_count > 0) {
         evict_a_bunch();
      }
   }

   static constexpr u16 kClockWalkSteps = 10;
   alignas(64) std::atomic<u64> eviction_round_ref_counter[2];
   alignas(64) std::atomic<u64> eviction_round{0};
   alignas(64) std::mutex eviction_mutex;
   void evict_a_bunch(int steps_to_walk = kClockWalkSteps) {
      u64 this_eviction_round;
      u32 start_page;
      u32 end_page;
      {
         std::lock_guard<std::mutex> g(eviction_mutex);
         start_page = clock_hand;
         end_page = clock_hand + kClockWalkSteps;
         u32 n_heap_pages = hot_iheap.getHeapPages();
         this_eviction_round = eviction_round.load();
         
         if (eviction_round_ref_counter[1 - (this_eviction_round % 2)] != 0) {
            return; // There are evictors working in the previous round, retry later
         }

         if (end_page > n_heap_pages) { // rewind
            end_page = n_heap_pages;
            clock_hand = 0;
            eviction_round++; // end of the bucket scan, switch to the next round
         } else {
            clock_hand = end_page;
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
      for (u32 pid = start_page; pid != end_page; ++pid) {
         auto evict_keys_size_snapshot = evict_keys.size();
         hot_iheap.scanHeapPage(sizeof(Key), pid, [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
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
            });
      }
   
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         heap_buffer_hit++;
      } else {
         heap_buffer_miss += new_miss - old_miss;
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
               auto res = hot_iheap.lookup(key_bytes, fold(key_bytes, key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
                  TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
                  evict_payloads[i] = *tp;
                  }) == leanstore::storage::heap::OP_RESULT::OK;
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
            auto op_res = hot_iheap.remove(key_bytes, fold(key_bytes, key));
            hot_partition_item_count--;
            assert(op_res == leanstore::storage::heap::OP_RESULT::OK);
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
   
   static constexpr s64 kEvictionCheckInterval = 30;
   DistributedCounter<> eviction_count_down {kEvictionCheckInterval};
   void try_eviction() {
      if (cache_under_pressure()) {
         evict_a_bunch();
      }
   }

   void try_eviction_direct() {
      --eviction_count_down;
      if (eviction_count_down.load() <= 0) {
            evict_a_bunch();
         eviction_count_down += kEvictionCheckInterval;
      }
   }

   bool admit_element_might_fail(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      admit_element(k, v, dirty, referenced);
      return true;
      // if (insert_hot_partition_might_fail(strip_off_partition_bit(k), v, dirty, referenced)) {
      //    hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
      //    return true;
      // }
      // return false;
   }

   void admit_element(Key k, Payload & v, bool dirty = false, bool referenced = true) {
      insert_hot_partition(strip_off_partition_bit(k), v, dirty, referenced);
      hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
   }
   
   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]] int length) {
      // TODO
   }

   void set_buffer_manager(storage::BufferManager * buf_mgr) { 
      this->buf_mgr = buf_mgr;
   }

   bool lookup(Key k, Payload & v) {
      try_eviction();
      while (true) {
         LockGuardProxy g(&lock_table, k);
         if (!g.read_lock()) {
            continue;
         }
         bool res = lookup_internal(k, v, g);
         if (g.validate()) {
            return res;
         }
      }
   }


   bool lookup_internal(Key k, Payload& v)
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      // try hot partition first
      auto hot_key = k;
      
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = hot_iheap.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            leanstore::storage::heap::OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         heap_buffer_hit++;
      } else {
         heap_buffer_miss += new_miss - old_miss;
      }
      return res;
   }

   bool lookup_internal(Key k, Payload& v, LockGuardProxy & g)
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      // try hot partition first
      auto hot_key = tag_with_hot_bit(k);
      uint64_t old_miss, new_miss;
      OLD_HIT_STAT_START;
      bool mark_dirty = false;
      auto res = hot_iheap.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         if (tp->referenced() == false) {
            tp->set_referenced();
            mark_dirty = utils::RandomGenerator::getRandU64(0, 100) < 10;
         }
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            leanstore::storage::heap::OP_RESULT::OK;
      OLD_HIT_STAT_END;
      //hot_iheap_ios += new_miss - old_miss;
      if (res) {
         //++lookups_hit_top;
         return res;
      }

      auto cold_key = tag_with_cold_bit(k);
      res = cold_iheap.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused))) { 
         Payload *pl =  const_cast<Payload*>(reinterpret_cast<const Payload*>(payload));
         memcpy(&v, pl, sizeof(Payload)); 
         }) ==
            leanstore::storage::heap::OP_RESULT::OK;

      if (res) {
         if (should_migrate()) {
            if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
               return false;
            }
            
            // move to hot partition
            bool res = false;
            OLD_HIT_STAT_START;

            if (utils::RandomGenerator::getRandU64(0, 100) < 4) {
               admit_element(k, v);
               res = true;
            } else {
               res = admit_element_might_fail(k, v);
            }
            OLD_HIT_STAT_END;
            hot_iheap_ios += new_miss - old_miss;
            if (res) {
               ++upward_migrations;
            } else {
               ++failed_upward_migrations;
            }
            if (inclusive == false && res) {
               //OLD_HIT_STAT_START;
               // remove from the cold partition
               auto op_res __attribute__((unused))= cold_iheap.remove(key_bytes, fold(key_bytes, cold_key));
               assert(op_res == leanstore::storage::heap::OP_RESULT::OK);
               //OLD_HIT_STAT_END
            }

         }
      }
      return res;
   }

   void upsert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);

      auto op_res = cold_iheap.upsert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&v), sizeof(Payload));
      assert(op_res == leanstore::storage::heap::OP_RESULT::OK);
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
      }

      if (modified) {
         tp.set_modified();
      } else {
         tp.clear_modified();
      }
      tp.payload = v;
      
      HIT_STAT_START;
      auto op_res = hot_iheap.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      assert(op_res == leanstore::storage::heap::OP_RESULT::OK);
      HIT_STAT_END;
   }

   void insert_cold_partition(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      Key key = tag_with_cold_bit(k);
      auto op_res = cold_iheap.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&v), sizeof(Payload));
      assert(op_res == leanstore::storage::heap::OP_RESULT::OK);
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

   void update(Key k, Payload& v) override {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      try_eviction();
      LockGuardProxy g(&lock_table, k);
      while (g.write_lock() == false);
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = hot_iheap.lookupForUpdate(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->set_referenced();
         tp->set_modified();
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
         return true;
      });
      HIT_STAT_END;
      hot_iheap_ios += new_miss - old_miss;
      if (op_res == leanstore::storage::heap::OP_RESULT::OK) {
         ++lookups_hit_top;
         return;
      }

      Payload old_v;
      auto cold_key = tag_with_cold_bit(k);
      //OLD_HIT_STAT_START;
      auto res __attribute__((unused)) = cold_iheap.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length __attribute__((unused)) ) { 
         Payload *pl =  const_cast<Payload*>(reinterpret_cast<const Payload*>(payload));
         memcpy(&old_v, pl, sizeof(Payload)); 
         }) ==
            leanstore::storage::heap::OP_RESULT::OK;
      assert(res);
      //OLD_HIT_STAT_END;

      if (should_migrate()) {
         // move to hot partition
         bool res = false;
         OLD_HIT_STAT_START;
         if (cache_under_pressure_soft()) {
            res = admit_element_might_fail(k, v);
         } else {
            admit_element(k, v);
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
            auto op_res __attribute__((unused)) = cold_iheap.remove(key_bytes, fold(key_bytes, cold_key));
            //OLD_HIT_STAT_END;
         }
      } else {
         cold_iheap.lookupForUpdate(key_bytes, fold(key_bytes, cold_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
            Payload *pl =  reinterpret_cast<Payload*>(payload);
            memcpy(pl, &v, sizeof(Payload)); 
            return true;
         });
      }
   }


   void put(Key k, Payload& v) override {
      retry:
      try_eviction();
      LockGuardProxy g(&lock_table, k);
      while (g.write_lock() == false);
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      HIT_STAT_START;
      auto op_res = hot_iheap.lookupForUpdate(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length __attribute__((unused)) ) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->set_referenced();
         tp->set_modified();
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
         return true;
      });
      HIT_STAT_END;

      if (op_res == leanstore::storage::heap::OP_RESULT::OK) {
         return;
      }
      // move to hot partition
      admit_element(k, v, true);

      if (inclusive == false) { // If exclusive, remove the tuple from the on-disk B-Tree
         auto cold_key = tag_with_cold_bit(k);
         //OLD_HIT_STAT_START;
         // remove from the cold partition
         auto op_res __attribute__((unused)) = cold_iheap.remove(key_bytes, fold(key_bytes, cold_key));
         assert(op_res == leanstore::storage::heap::OP_RESULT::OK);
         //OLD_HIT_STAT_END;
      }
   }

   std::string iheap_stat(const std::string & name, leanstore::storage::heap::IndexedHeapFile& iheap, size_t tuple_size, size_t index_entry_size) {
      auto heap_pages = iheap.countHeapPages();
      auto heap_entries = iheap.countHeapEntries();
      auto index_pages = iheap.countIndexPages();
      auto index_entries = iheap.countIndexEntries();
      auto heap_index_height = iheap.indexHeight();
      auto minimal_heap_pages = heap_entries * tuple_size / leanstore::storage::PAGE_SIZE;
      auto minimal_index_pages = index_entries * index_entry_size / leanstore::storage::PAGE_SIZE;

      return name + " IHeap # tuples " + std::to_string(heap_entries) + "\n" +
             name + " IHeap # pages " + std::to_string(heap_pages) + "\n" +
             name + " IHeap # index entries " + std::to_string(index_entries) + "\n" +
             name + " IHeap # index pages " + std::to_string(index_pages) + "\n" +
             name + " IHeap index height " + std::to_string(heap_index_height) + "\n" +
             name + " IHeap heap average fill factor " + std::to_string((minimal_heap_pages + 0.0) / heap_pages) + "\n" +
             name + " IHeap index average fill factor " + std::to_string((minimal_index_pages + 0.0) / index_pages) + "\n";
   }

   void report([[maybe_unused]] u64, u64) override {
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::string hot_iheap_stats = iheap_stat("Hot", hot_iheap, sizeof(Key) + sizeof(TaggedPayload), (sizeof(Key) + sizeof(leanstore::storage::heap::HeapTupleId)));
      std::string cold_iheap_stats = iheap_stat("Cold", cold_iheap, sizeof(Key) + sizeof(Payload), (sizeof(Key) + sizeof(leanstore::storage::heap::HeapTupleId)));
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      std::cout << "Hot IHeap capacity " << hot_partition_capacity_bytes << std::endl;
      std::cout << hot_iheap_stats << cold_iheap_stats;
      double iheap_hit_rate = heap_buffer_hit / (heap_buffer_hit + heap_buffer_miss + 1.0);
      std::cout << "IHeap buffer hits/misses " <<  heap_buffer_hit << "/" << heap_buffer_miss << std::endl;
      std::cout << "IHeap buffer hit rate " <<  iheap_hit_rate << " miss rate " << (1 - iheap_hit_rate) << std::endl;
      std::cout << "Evicted " << eviction_items << " tuples, " << eviction_io_reads << " io_reads for these evictions, io_reads/eviction " << eviction_io_reads / (eviction_items  + 1.0) << std::endl;
      std::cout << total_lookups<< " lookups, " << io_reads.get() << " ios, " << io_reads / (total_lookups + 0.00) << " ios/lookup, " << lookups_hit_top << " lookup hit top" << " hot_iheap_ios " << hot_iheap_ios<< " ios/tophit " << hot_iheap_ios / (lookups_hit_top + 0.00)  << std::endl;
      std::cout << upward_migrations << " upward_migrations, "  << downward_migrations << " downward_migrations, "<< failed_upward_migrations << " failed_upward_migrations" << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01) << std::endl;
      std::cout << "Average eviction bounding time " << eviction_bounding_time / (eviction_bounding_n + 1) << std::endl;
   }
};

}  // namespace leanstore
