#pragma once
#include<functional>

#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "common/DistributedCounter.hpp"
struct IOStats {
   DistributedCounter<> reads = 0;
   DistributedCounter<> writes = 0;
   DistributedCounter<> eviction_reads = 0;
   void Clear() {
      reads.store(0);
      writes.store(0);
      eviction_reads.store(0);
   }
};

namespace leanstore {
template <typename Key, typename Payload>
struct StorageInterface {
   virtual ~StorageInterface(){}
   virtual bool lookup([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) = 0;
   virtual void insert([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) = 0;
   virtual void update([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) = 0;
   virtual void put([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) {}
   virtual void bulk_load(const std::vector<Key> & keys, const std::vector<Payload> & v) {}
   virtual bool remove([[maybe_unused]] Key k) { return false; }
   virtual void scan([[maybe_unused]] Key start_key, [[maybe_unused]] std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]] int length) {}
   virtual void report(u64, u64){}
   virtual void report_cache(){}
   virtual void clear_stats() {}
   virtual void clear_io_stats() {}
   virtual void evict_all() {}
   virtual void set_buffer_manager(storage::BufferManager *) {}
   virtual std::vector<std::string> stats_column_names() { return {}; }
   virtual std::vector<std::string> stats_columns() { return {}; }
   virtual IOStats exchange_stats(IOStats s) { return IOStats(); }
};


}