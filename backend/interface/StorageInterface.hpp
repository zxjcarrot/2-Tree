#pragma once

namespace leanstore {
   
template <typename Key, typename Payload>
struct BTreeInterface {
   virtual bool lookup(Key k, Payload& v) = 0;
   virtual void insert(Key k, Payload& v) = 0;
   virtual void update(Key k, Payload& v) = 0;
   virtual void put(Key k, Payload& v) {}
   virtual void report(u64, u64){}
   virtual void report_cache(){}
   virtual void clear_stats() {}
   virtual void evict_all() {}
};


}