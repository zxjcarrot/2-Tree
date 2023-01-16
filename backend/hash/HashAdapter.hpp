#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/hashing/LinearHashing.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "stx/btree_map.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{

// -------------------------------------------------------------------------------------
using HT_OP_RESULT = leanstore::storage::hashing::OP_RESULT;

// class DeferCode {
// public:
//    DeferCode() = delete;
//    DeferCode(std::function<void()> f): f(f) {}
//    ~DeferCode() { 
//       f(); 
//    }
//    std::function<void()> f;
// };


template <typename Key, typename Payload>
struct HashVSAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::hashing::LinearHashTable & hash_table;

   std::size_t io_reads_snapshot = 0;
   std::size_t io_reads_now = 0;
   uint64_t ht_buffer_miss = 0;
   uint64_t ht_buffer_hit = 0;
   std::size_t scan_ops = 0;
   std::size_t io_reads_scan = 0;
   DTID dt_id;
   HashVSAdapter(leanstore::storage::hashing::LinearHashTable& ht, DTID dt_id = -1) : hash_table(ht), dt_id(dt_id) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      ht_buffer_miss = ht_buffer_hit = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      io_reads_scan = scan_ops = 0;
   }

   bool lookup(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = hash_table.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
            HT_OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         ht_buffer_hit++;
      } else {
         ht_buffer_miss += new_miss - old_miss;
      }
      return res;
   }
   void insert(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      hash_table.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         ht_buffer_hit++;
      } else {
         ht_buffer_miss += new_miss - old_miss;
      }
   }

   bool remove(Key key) {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = hash_table.remove(key_bytes, fold(key_bytes, key)) ==
            HT_OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         ht_buffer_hit++;
      } else {
         ht_buffer_miss += new_miss - old_miss;
      }
      return res;
   }
   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, int length) {
      scan_ops++;
      u8 key_bytes[sizeof(Key)];
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      // hash_table.scanAsc(key_bytes, fold(key_bytes, start_key),
      // [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
      //    auto real_key = unfold(*(Key*)(key));
      //    assert(key_length == sizeof(Key));
      //    assert(value_length == sizeof(Payload));
      //    const Payload * p = reinterpret_cast<const Payload*>(value);
      //    if (processor(real_key, *p)) {
      //       return false;
      //    }
      //    return true;
      // }, [](){});
      io_reads_scan += WorkerCounters::myCounters().io_reads.load() - io_reads_old;
   }

   void update(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto f = [&](u8* payload, u16 payload_length) -> bool {
         memcpy(payload, &v, payload_length); 
         return true;
      };
      auto op_res = hash_table.lookupForUpdate(key_bytes, fold(key_bytes, k), f);
      // auto op_res = hash_table.updateSameSize(key_bytes, fold(key_bytes, k), 
      //                                    ,
      //                                    Payload::wal_update_generator);
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         ht_buffer_hit++;
      } else {
         ht_buffer_miss += new_miss - old_miss;
      }
   }

   void put(Key k, Payload& v) {
      update(k, v);
   }

   void report(u64 entries, u64 pages) override {
      assert(io_reads_now >= io_reads_snapshot);
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      auto real_pages = hash_table.countPages();
      auto real_entries = hash_table.countEntries();
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      std::cout << "Hash Table # entries " << entries << std::endl;
      std::cout << "Hash Table # entries (real) " << real_entries << std::endl;
      std::cout << "Hash Table # pages " << pages << std::endl;
      std::cout << "Hash Table # pages (real) " << real_pages << std::endl;
      std::cout << "Hash Table # buckets " << hash_table.countBuckets() << std::endl;
      std::cout << "Hash Table power multiplier " << hash_table.powerMultiplier() << std::endl;
      auto minimal_pages = entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "Hash Table average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double ht_hit_rate = ht_buffer_hit / (ht_buffer_hit + ht_buffer_miss + 1.0);
      std::cout << "Hash Table buffer hits/misses " <<  ht_buffer_hit << "/" << ht_buffer_miss << std::endl;
      std::cout << "Hash Table buffer hit rate " <<  ht_hit_rate << " miss rate " << (1 - ht_hit_rate) << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01);
   }
};


}  // namespace leanstore
