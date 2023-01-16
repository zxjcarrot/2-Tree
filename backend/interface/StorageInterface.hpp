#pragma once
#include<functional>
namespace leanstore {

template <typename Key, typename Payload>
struct StorageInterface {
   virtual bool lookup(Key k, Payload& v) = 0;
   virtual void insert(Key k, Payload& v) = 0;
   virtual void update(Key k, Payload& v) = 0;
   virtual void put(Key k, Payload& v) {}
   virtual bool remove(Key k) {}
   virtual void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, int length) {}
   virtual void report(u64, u64){}
   virtual void report_cache(){}
   virtual void clear_stats() {}
   virtual void evict_all() {}
};


}