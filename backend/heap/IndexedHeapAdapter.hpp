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


template <typename Key, typename Payload>
struct IndexedHeapAdapter : StorageInterface<Key, Payload> {
   leanstore::storage::heap::IndexedHeapFile iheap;

   DistributedCounter<> total_lookups = 0;
   DistributedCounter<> io_reads = 0;
   DistributedCounter<> io_reads_snapshot = 0;
   DistributedCounter<> io_reads_now = 0;
   DistributedCounter<> iheap_buffer_miss = 0;
   DistributedCounter<> iheap_buffer_hit = 0;
   DistributedCounter<> scan_ops = 0;
   DistributedCounter<> io_reads_scan = 0;
   alignas(64) std::atomic<uint64_t> bp_dirty_page_flushes_snapshot = 0;
   leanstore::storage::BufferManager * buf_mgr = nullptr;

   void set_buffer_manager(storage::BufferManager * buf_mgr) override { 
      this->buf_mgr = buf_mgr;
      bp_dirty_page_flushes_snapshot.store(this->buf_mgr->dirty_page_flushes.load());
   }

   IndexedHeapAdapter(leanstore::storage::heap::HeapFile& heap_file, leanstore::storage::btree::BTreeLL& btree_index) : iheap(heap_file, btree_index) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }

   void clear_stats() override {
      iheap_buffer_miss = iheap_buffer_hit = 0;
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
      io_reads_scan = scan_ops = 0;
      io_reads = 0;
      bp_dirty_page_flushes_snapshot.store(this->buf_mgr->dirty_page_flushes.load());
   }

   bool lookup(Key k, Payload& v) override
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      total_lookups++;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = iheap.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
            IHF_OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         iheap_buffer_hit++;
      } else {
         iheap_buffer_miss += new_miss - old_miss;
      }
      return res;
   }
   void insert(Key k, Payload& v) override
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      //DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      iheap.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         iheap_buffer_hit++;
      } else {
         iheap_buffer_miss += new_miss - old_miss;
      }
   }


   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]]int length) {
      // scan_ops++;
      // u8 key_bytes[sizeof(Key)];
      // auto io_reads_old = WorkerCounters::myCounters().io_reads.load();
      // btree.scanAsc(key_bytes, fold(key_bytes, start_key),
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
      // io_reads_scan += WorkerCounters::myCounters().io_reads.load() - io_reads_old;
   }

   bool remove(Key key) {
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = iheap.remove(key_bytes, fold(key_bytes, key)) ==
            IHF_OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         iheap_buffer_hit++;
      } else {
         iheap_buffer_miss += new_miss - old_miss;
      }
      return res;
   }

   void update(Key k, Payload& v) override
   {
      leanstore::utils::IOScopedCounter cc([&](u64 ios){ this->io_reads += ios; });
      total_lookups++;
      DeferCode c([&, this](){io_reads_now = WorkerCounters::myCounters().io_reads.load();});
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      [[maybe_unused]] auto op_res = iheap.lookupForUpdate(key_bytes, fold(key_bytes, k), 
                                         [&](u8* payload, u16 payload_length) { 
                                          assert(payload_length == sizeof(Payload));
                                          memcpy(payload, &v, payload_length); 
                                          return true;
                                          });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         iheap_buffer_hit++;
      } else {
         iheap_buffer_miss += new_miss - old_miss;
      }
   }

   void put(Key k, Payload& v) {
      update(k, v);
   }

   void report(u64 entries, u64 pages) override {
      assert(this->buf_mgr->dirty_page_flushes >= this->bp_dirty_page_flushes_snapshot);
      auto io_writes = this->buf_mgr->dirty_page_flushes - this->bp_dirty_page_flushes_snapshot;
      assert(io_reads_now >= io_reads_snapshot);
      auto total_io_reads_during_benchmark = io_reads_now - io_reads_snapshot;
      std::cout << "Total IO reads during benchmark " << total_io_reads_during_benchmark << std::endl;
      auto heap_pages = iheap.countHeapPages();
      auto heap_entries = iheap.countHeapEntries();
      auto index_pages = iheap.countIndexPages();
      auto index_entries = iheap.countIndexEntries();
      std::cout << "IHeap # tuples " << heap_entries << std::endl;
      std::cout << "IHeap # pages " << heap_pages << std::endl;
      std::cout << "IHeap # index entries " << index_entries << std::endl;
      std::cout << "IHeap # index pages " << index_pages << std::endl;
      std::cout << "IHeap index height " << iheap.indexHeight() << std::endl;
      auto minimal_heap_pages = heap_entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "IHeap heap average fill factor " <<  (minimal_heap_pages + 0.0) / heap_pages << std::endl;
      auto minimal_index_pages = index_entries * (sizeof(Key) + sizeof(leanstore::storage::heap::HeapTupleId)) / leanstore::storage::PAGE_SIZE;
      std::cout << "IHeap index average fill factor " <<  (minimal_index_pages + 0.0) / index_pages << std::endl;
      double heap_buffer_hit_rate = iheap_buffer_hit / (iheap_buffer_hit + iheap_buffer_miss + 1.0);
      std::cout << "IHeap buffer hits/misses " <<  iheap_buffer_hit << "/" << iheap_buffer_miss << std::endl;
      std::cout << "IHeap buffer hit rate " <<  heap_buffer_hit_rate << " miss rate " << (1 - heap_buffer_hit_rate) << std::endl;
      std::cout << total_lookups<< " lookups, " << io_reads.get() << " i/o reads, " << io_writes << " i/o writes, " << io_reads / (total_lookups + 0.00) << " i/o reads/lookup, " <<  (io_reads + io_writes) / (total_lookups + 0.00) << " ios/lookup" << std::endl;
      std::cout << "Scan ops " << scan_ops << ", ios_read_scan " << io_reads_scan << ", #ios/scan " <<  io_reads_scan/(scan_ops + 0.01);
   }
};

}  // namespace leanstore
