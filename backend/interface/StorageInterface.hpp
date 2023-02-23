#pragma once
#include<functional>

#include "leanstore/storage/buffer-manager/BufferManager.hpp"
namespace leanstore {

template <typename Key, typename Payload>
struct StorageInterface {
   virtual ~StorageInterface(){}
   virtual bool lookup([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) = 0;
   virtual void insert([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) = 0;
   virtual void update([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) = 0;
   virtual void put([[maybe_unused]] Key k, [[maybe_unused]] Payload& v) {}
   virtual bool remove([[maybe_unused]] Key k) { return false; }
   virtual void scan([[maybe_unused]] Key start_key, [[maybe_unused]] std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]] int length) {}
   virtual void report(u64, u64){}
   virtual void report_cache(){}
   virtual void clear_stats() {}
   virtual void clear_io_stats() {}
   virtual void evict_all() {}
   virtual void set_buffer_manager(storage::BufferManager *) {}
};


}