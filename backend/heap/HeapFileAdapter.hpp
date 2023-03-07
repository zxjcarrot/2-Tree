#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/heap/HeapFile.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "stx/btree_map.h"
#include "leanstore/utils/DistributedCounter.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{

// -------------------------------------------------------------------------------------
using HF_OP_RESULT = leanstore::storage::heap::OP_RESULT;

template <typename Key, typename Payload>
struct HeapFileAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::heap::HeapFile & heap_file;

   DistributedCounter<> total_lookups = 0;
   DistributedCounter<> io_reads = 0;
   DistributedCounter<> io_reads_snapshot = 0;
   DistributedCounter<> io_reads_now = 0;
   DistributedCounter<> hf_buffer_miss = 0;
   DistributedCounter<> hf_buffer_hit = 0;
   DistributedCounter<> scan_ops = 0;
   DistributedCounter<> io_reads_scan = 0;
   DTID dt_id;
   struct Tuple {
      Key key;
      Payload paylod;
   };
   HeapFileAdapter(leanstore::storage::heap::HeapFile& hf, DTID dt_id = -1) : heap_file(hf), dt_id(dt_id) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      hf_buffer_miss = hf_buffer_hit = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      io_reads_scan = scan_ops = 0;
      io_reads = 0;
   }

   bool lookup(Key k, Payload& v) override
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = false;
      heap_file.scan([&](const u8 * value, u16 value_length, leanstore::storage::heap::HeapTupleId tuple_id) {
         const Tuple * t = reinterpret_cast<const Tuple*>(value);
         assert(sizeof(Tuple) == value_length);
         if (t->key == k) {
            v = t->paylod;
            res = true;
            return false; // end the scan
         }
         return true; // continue the scan
      });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         hf_buffer_hit++;
      } else {
         hf_buffer_miss += new_miss - old_miss;
      }
      return res;
   }
   void insert(Key k, Payload& v) override
   {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      Tuple t;
      t.key = k;
      t.paylod = v;
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      leanstore::storage::heap::HeapTupleId tuple_id;
      auto res = heap_file.insert(tuple_id, (u8*)&t, sizeof(t));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(res == HF_OP_RESULT::OK);
      if (old_miss == new_miss) {
         hf_buffer_hit++;
      } else {
         hf_buffer_miss += new_miss - old_miss;
      }
   }

   bool remove(Key key) {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = false;
      leanstore::storage::heap::HeapTupleId target_tuple_id;
      heap_file.scan([&](const u8 * value, u16 value_length, leanstore::storage::heap::HeapTupleId tuple_id) {
         const Tuple * t = reinterpret_cast<const Tuple*>(value);
         assert(sizeof(Tuple) == value_length);
         if (t->key == key) {
            target_tuple_id = tuple_id;
            res = true;
            return false; // end the scan
         }
         return true; // continue the scan
      });
      if (res) {
         res = heap_file.remove(target_tuple_id) == HF_OP_RESULT::OK;
      }
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         hf_buffer_hit++;
      } else {
         hf_buffer_miss += new_miss - old_miss;
      }

      return res;
   }
   void scan([[maybe_unused]] Key start_key, std::function<bool(const Key&, const Payload &)> processor, int length) {
      scan_ops++;
      [[maybe_unused]] u8 key_bytes[sizeof(Key)];
      auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      heap_file.scan([&](const u8 * value, u16 value_length, leanstore::storage::heap::HeapTupleId tuple_id) -> bool {
         const Tuple * t = reinterpret_cast<const Tuple*>(value);
         assert(sizeof(Tuple) == value_length);
         if (processor(t->key, t->paylod)) {
            return false;
         }
         return true;
      });
      io_reads_scan += WorkerCounters::myCounters().io_reads.load() - io_reads_old;
   }

   void update(Key k, Payload& v) override
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      ++total_lookups;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = false;
      leanstore::storage::heap::HeapTupleId target_tuple_id;
      heap_file.scan([&](const u8 * value, u16 value_length, leanstore::storage::heap::HeapTupleId tuple_id) {
         const Tuple * t = reinterpret_cast<const Tuple*>(value);
         assert(sizeof(Tuple) == value_length);
         if (t->key == k) {
            target_tuple_id = tuple_id;
            res = true;
            return false; // end the scan
         }
         return true; // continue the scan
      });
      if (res) {
         res = heap_file.lookupForUpdate(target_tuple_id, [&](u8 * value, u16 value_length){
            Tuple * t = reinterpret_cast<Tuple*>(value);
            if (t->key == k) {
               assert(sizeof(Tuple) == value_length);
               t->paylod = v;
               return true;
            }
            return false;
         }) == HF_OP_RESULT::OK;
         assert(res);
      }
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         hf_buffer_hit++;
      } else {
         hf_buffer_miss += new_miss - old_miss;
      }
   }

   void put(Key k, Payload& v) {
      update(k, v);
   }

   void report(u64 entries, u64 pages) override {
      assert(io_reads_now >= io_reads_snapshot);
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      auto real_pages = heap_file.countPages();
      auto real_entries = heap_file.countEntries();
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      std::cout << "Heap File # entries " << entries << std::endl;
      std::cout << "Heap File # entries (real) " << real_entries << std::endl;
      std::cout << "Heap File # pages " << pages << std::endl;
      std::cout << "Heap File # pages (real) " << real_pages << std::endl;
      std::cout << "Heap File # pages (fast) " << heap_file.getPages() << std::endl;
      std::cout << "Heap File bytes stored " << heap_file.dataStored() << std::endl;
      auto minimal_pages = entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "Heap File average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double ht_hit_rate = hf_buffer_hit / (hf_buffer_hit + hf_buffer_miss + 1.0);
      std::cout << "Heap File buffer hits/misses " <<  hf_buffer_hit << "/" << hf_buffer_miss << std::endl;
      std::cout << "Heap File buffer hit rate " <<  ht_hit_rate << " miss rate " << (1 - ht_hit_rate) << std::endl;
      std::cout << total_lookups<< " lookups, " << io_reads.get() << " ios, " << io_reads / (total_lookups + 0.00) << " ios/lookup" << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01) << std::endl;
   }
};


}  // namespace leanstore
