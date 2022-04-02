#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/stx/btree_map.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
unsigned fold(uint8_t* writer, const s32& x)
{
   *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
   return sizeof(x);
}

unsigned fold(uint8_t* writer, const s64& x)
{
   *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
   return sizeof(x);
}

unsigned fold(uint8_t* writer, const u64& x)
{
   *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x);
   return sizeof(x);
}

unsigned fold(uint8_t* writer, const u32& x)
{
   *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x);
   return sizeof(x);
}

u64 unfold(const u64 &x) {
   return __builtin_bswap64(x);
}

u32 unfold(const u32 &x) {
   return __builtin_bswap32(x);
}
// -------------------------------------------------------------------------------------
template <typename Key, typename Payload>
struct BTreeInterface {
   virtual bool lookup(Key k, Payload& v) = 0;
   virtual void insert(Key k, Payload& v) = 0;
   virtual void update(Key k, Payload& v) = 0;
   virtual void report(u64, u64){}
   virtual void clear_stats() {}
};
// -------------------------------------------------------------------------------------
using OP_RESULT = leanstore::storage::btree::OP_RESULT;
template <typename Key, typename Payload>
struct BTreeVSAdapter : BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;

   uint64_t btree_buffer_miss = 0;
   uint64_t btree_buffer_hit = 0;
   DTID dt_id;
   BTreeVSAdapter(leanstore::storage::btree::BTreeInterface& btree, DTID dt_id = -1) : btree(btree), dt_id(dt_id) {}

   void clear_stats() override {
      btree_buffer_miss = btree_buffer_hit = 0;
   }

   bool lookup(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
            OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }
      return res;
   }
   void insert(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      btree.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }
   }
   void update(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }
   }

   void report(u64 entries, u64 pages) override {
      std::cout << "BTree # entries " << entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "BTree height " << btree.getHeight() << std::endl;
      auto minimal_pages = entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double btree_hit_rate = btree_buffer_hit / (btree_buffer_hit + btree_buffer_miss + 1.0);
      std::cout << "BTree buffer hits/misses " <<  btree_buffer_hit << "/" << btree_buffer_miss << std::endl;
      std::cout << "BTree buffer hit rate " <<  btree_hit_rate << " miss rate " << (1 - btree_hit_rate) << std::endl;
   }
};


template <typename Key, typename Payload>
struct BTreeVSHotColdPartitionedAdapter : BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;

   uint64_t btree_buffer_miss = 0;
   uint64_t btree_buffer_hit = 0;
   DTID dt_id;
   std::size_t hot_partition_capacity_bytes;
   std::size_t hot_partition_size_bytes = 0;
   static constexpr double eviction_threshold = 0.99;

   struct TaggedPayload {
      Payload payload;
      bool referenced = false;
   };
   
   Key clock_hand = std::numeric_limits<Key>::max();
   constexpr static u64 PartitionBitPosition = 63;
   constexpr static u64 PartitionBitMask = 0x8000000000000000;
   Key tag_with_hot_bit(Key key) {
      return key;
   }
   
   bool is_in_hot_partition(Key key) {
      return (key & PartitionBitMask) == 0;
   }

   Key tag_with_cold_bit(Key key) {
      return key | (1ul << PartitionBitPosition);
   }

   Key strip_off_partition_bit(Key key) {
      return key & ~(PartitionBitMask);
   }

   BTreeVSHotColdPartitionedAdapter(leanstore::storage::btree::BTreeInterface& btree, DTID dt_id,  double hot_partition_size_gb) : btree(btree), dt_id(dt_id), hot_partition_capacity_bytes(hot_partition_size_gb * 1024ULL * 1024ULL * 1024ULL){}

   bool cache_under_pressure() {
      return hot_partition_size_bytes >= hot_partition_capacity_bytes * eviction_threshold;
   }

   void clear_stats() override {
      btree_buffer_miss = btree_buffer_hit = 0;
   }

   void evict_one() {
      Key start_key = clock_hand;
      if (start_key == std::numeric_limits<Key>::max()) {
         start_key = tag_with_hot_bit(std::numeric_limits<Key>::min());
      }

      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      std::vector<Payload> evict_payloads;
      Key evict_key;
      Payload evict_payload;
      bool victim_found = false;
      assert(is_in_hot_partition(start_key));
      btree.scanAsc(key_bytes, fold(key_bytes, start_key),
      [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
         auto real_key = unfold(*(Key*)(key));
         assert(key_length == sizeof(Key));
         if (is_in_hot_partition(real_key) == false) {
            clock_hand = std::numeric_limits<Key>::max();
            return false;
         }
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(value));
         assert(value_length == sizeof(TaggedPayload));
         if (tp->referenced == true) {
            tp->referenced = false;
            return true;
         }
         evict_key = real_key;
         clock_hand = real_key;
         evict_payload = tp->payload;
         victim_found = true;
         evict_keys.push_back(real_key);
         evict_payloads.emplace_back(evict_payload);
         if (evict_keys.size() > 100) {
            return false;
         }
         return true;
      }, [](){});

      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            assert(is_in_hot_partition(key));
            auto op_res = btree.remove(key_bytes, fold(key_bytes, key));
            assert(op_res == OP_RESULT::OK);
            hot_partition_size_bytes -= sizeof(Key) + sizeof(TaggedPayload);
            insert_partition(strip_off_partition_bit(key), evict_payloads[i], true); // Cold partition
         }
      } else {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }


   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_one();
      }
   }


   void admit_element(Key k, Payload & v) {
      if (cache_under_pressure())
         evict_till_safe();
      insert_partition(strip_off_partition_bit(k), v, false); // Hot partition
      hot_partition_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
   }

   bool lookup(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      // try hot partition first
      auto hot_key = tag_with_hot_bit(k);
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto res = btree.lookup(key_bytes, fold(key_bytes, hot_key), [&](const u8* payload, u16 payload_length) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         tp->referenced = true;
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      assert(new_miss >= old_miss);
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }
      if (res) {
         return res;
      }

      auto cold_key = tag_with_cold_bit(k);
      old_miss = WorkerCounters::myCounters().io_reads.load();
      res = btree.lookup(key_bytes, fold(key_bytes, cold_key), [&](const u8* payload, u16 payload_length) { 
         TaggedPayload *tp =  const_cast<TaggedPayload*>(reinterpret_cast<const TaggedPayload*>(payload));
         memcpy(&v, tp->payload.value, sizeof(tp->payload)); 
         }) ==
            OP_RESULT::OK;
      new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }

      if (res) {
         old_miss = WorkerCounters::myCounters().io_reads.load();
         // remove from the cold partition
         auto op_res = btree.remove(key_bytes, fold(key_bytes, cold_key));
         assert(op_res == OP_RESULT::OK);
         new_miss = WorkerCounters::myCounters().io_reads.load();
         if (old_miss == new_miss) {
            btree_buffer_hit++;
         } else {
            btree_buffer_miss += new_miss - old_miss;
         }
         // move to hot partition
         admit_element(k, v);
      }
      return res;
   }

   // hot_or_cold: false => hot partition, true => cold partition
   void insert_partition(Key k, Payload & v, bool hot_or_cold) {
      u8 key_bytes[sizeof(Key)];
      Key key = hot_or_cold == false ? tag_with_hot_bit(k) : tag_with_cold_bit(k);
      TaggedPayload tp;
      tp.referenced = true;
      tp.payload = v;
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      btree.insert(key_bytes, fold(key_bytes, key), reinterpret_cast<u8*>(&tp), sizeof(tp));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }
   }

   void insert(Key k, Payload& v) override
   {
      //admit_element(k, v);
      insert_partition(k, v, true);
   }

   void update(Key k, Payload& v) override
   {
      u8 key_bytes[sizeof(Key)];
      auto hot_key = tag_with_hot_bit(k);
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      auto op_res = btree.updateSameSize(key_bytes, fold(key_bytes, hot_key), [&](u8* payload, u16 payload_length) {
         TaggedPayload *tp =  reinterpret_cast<TaggedPayload*>(payload);
         tp->referenced = true;
         memcpy(tp->payload.value, &v, sizeof(tp->payload)); 
      });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }

      if (op_res == OP_RESULT::OK) {
         return;
      }

      auto cold_key = tag_with_cold_bit(k);
      old_miss = WorkerCounters::myCounters().io_reads.load();
      // remove from the cold partition
      btree.remove(key_bytes, fold(key_bytes, cold_key));
      new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss == new_miss) {
         btree_buffer_hit++;
      } else {
         btree_buffer_miss += new_miss - old_miss;
      }
      // move to hot partition
      admit_element(k, v);
   }

   void report(u64 entries, u64 pages) override {
      std::cout << "Hot partition capacity in bytes " << hot_partition_capacity_bytes << std::endl;
      std::cout << "Hot partition size in bytes " << hot_partition_size_bytes << std::endl;
      std::cout << "BTree # entries " << entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "BTree height " << btree.getHeight() << std::endl;
      auto minimal_pages = entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " <<  (minimal_pages + 0.0) / pages << std::endl;
      double btree_hit_rate = btree_buffer_hit / (btree_buffer_hit + btree_buffer_miss + 1.0);
      std::cout << "BTree buffer hits/misses " <<  btree_buffer_hit << "/" << btree_buffer_miss << std::endl;
      std::cout << "BTree buffer hit rate " <<  btree_hit_rate << " miss rate " << (1 - btree_hit_rate) << std::endl;
   }
};

template <typename Key, typename Payload>
struct BTreeCachedNoninlineVSAdapter : BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;
   
   static constexpr double eviction_threshold = 0.99;
   std::size_t cache_capacity_bytes;
   std::size_t hit_count = 0;
   std::size_t miss_count = 0;
   
   struct TaggedPayload {
      Payload payload;
      bool referenced = false;
   };
   typedef std::pair<Key, TaggedPayload*> Element;

   stx::btree_map<Key, TaggedPayload*> cache;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   u64 lazy_migration_threshold = 10;
   BTreeCachedNoninlineVSAdapter(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, bool lazy_migration = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration) {}

   void clear_stats() override {
      hit_count = miss_count = 0;
   }

   inline size_t get_cache_btree_size() {
      auto & stat = cache.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size() + sizeof(TaggedPayload) * stat.itemcount;
   }

   bool cache_under_pressure() {
      return get_cache_btree_size() >= cache_capacity_bytes * eviction_threshold;
   }

   void evict_one() {
      Key key = clock_hand;
      typename stx::btree_map<Key, TaggedPayload*>::iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }
      while (it != cache.end()) {
         if (it.data()->referenced == true) {
            it.data()->referenced = false;
            clock_hand = it.key();
            ++it;
         } else {
            auto tmp = it;
            ++tmp;
            if (tmp == cache.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               clock_hand = it.key();
            }
            insert_btree(it.key(), it.data()->payload);
            delete it.data();
            cache.erase(it);
            break;
         }
      }
      if (it == cache.end()) {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }

   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_one();
      }
   }

   void admit_element(Key k, Payload & v) {
      if (cache_under_pressure())
         evict_till_safe();
      assert(cache.find(k) == cache.end());
      cache[k] = new TaggedPayload{v, true};
   }

   bool lookup(Key k, Payload& v) override
   {
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data()->referenced = true;
         v = it.data()->payload;
         hit_count++;
         return true;
      }
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
            OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (res) {
         if (should_migrate()) {
            admit_element(k, v);
            old_miss = WorkerCounters::myCounters().io_reads.load();
            btree.remove(key_bytes, fold(key_bytes, k));
            new_miss = WorkerCounters::myCounters().io_reads.load();
            if (old_miss != new_miss) {
               miss_count += new_miss - old_miss;
            }
         }
      }
      return res;
   }

   void insert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      btree.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
   }

   void insert(Key k, Payload& v) override
   {
      admit_element(k, v);
      //u8 key_bytes[sizeof(Key)];
      //btree.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
   }

   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void update(Key k, Payload& v) override
   {
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data()->referenced = true;
         it.data()->payload = v;
         hit_count++;
         return;
      }
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (should_migrate()) {
         admit_element(k, v);
         old_miss = WorkerCounters::myCounters().io_reads.load();
         btree.remove(key_bytes, fold(key_bytes, k));
         new_miss = WorkerCounters::myCounters().io_reads.load();
         if (old_miss != new_miss) {
            miss_count += new_miss - old_miss;
         }
      }
   }

   void report(u64 entries, u64 pages) override {
      std::cout << "Cache capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Cache size bytes " << get_cache_btree_size() << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "Cache BTree average leaf fill factor " << cache.get_stats().avgfill_leaves() << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Cache hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Cache miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
      auto btree_entries = (entries - cache.size());
      std::cout << "BTree # entries " << btree_entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "BTree height " << btree.getHeight() << std::endl;
      auto minimal_pages = btree_entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " << minimal_pages / (pages + 0.0) << std::endl;
   }
};

template <typename Key, typename Payload>
struct BTreeCachedVSAdapter : BTreeInterface<Key, Payload> {
   leanstore::storage::btree::BTreeInterface& btree;
   
   static constexpr double eviction_threshold = 0.99;
   std::size_t cache_capacity_bytes;
   std::size_t hit_count = 0;
   std::size_t miss_count = 0;
   typedef std::pair<Key, Payload> Element;
   struct TaggedPayload {
      Payload payload;
      bool referenced = false;
   };

   stx::btree_map<Key, TaggedPayload> cache;
   Key clock_hand = std::numeric_limits<Key>::max();
   bool lazy_migration;
   u64 lazy_migration_threshold = 10;
   BTreeCachedVSAdapter(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, bool lazy_migration = false) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), lazy_migration(lazy_migration) {}

   void clear_stats() override {
      hit_count = miss_count = 0;
   }

   inline size_t get_cache_btree_size() {
      auto & stat = cache.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size();
   }

   bool cache_under_pressure() {
      return get_cache_btree_size() >= cache_capacity_bytes * eviction_threshold;
   }

   void evict_one() {
      Key key = clock_hand;
      typename stx::btree_map<Key, TaggedPayload>::iterator it;
      if (key == std::numeric_limits<Key>::max()) {
         it = cache.begin();
      } else {
         it = cache.lower_bound(key);
      }
      while (it != cache.end()) {
         if (it.data().referenced == true) {
            it.data().referenced = false;
            clock_hand = it.key();
            ++it;
         } else {
            auto tmp = it;
            ++tmp;
            if (tmp == cache.end()) {
               clock_hand = std::numeric_limits<Key>::max();
            } else {
               clock_hand = tmp.key();
            }
            insert_btree(it.key(), it.data().payload);
            cache.erase(it);
            break;
         }
      }
      if (it == cache.end()) {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }

   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_one();
      }
   }

   void admit_element(Key k, Payload & v) {
      if (cache_under_pressure())
         evict_till_safe();
      assert(cache.find(k) == cache.end());
      cache[k] = {v, true};
   }

   bool lookup(Key k, Payload& v) override
   {
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data().referenced = true;
         v = it.data().payload;
         hit_count++;
         return true;
      }
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      bool res = btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); }) ==
            OP_RESULT::OK;
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (res) {
         if (should_migrate()) {
            admit_element(k, v);
            old_miss = WorkerCounters::myCounters().io_reads.load();
            btree.remove(key_bytes, fold(key_bytes, k));
            new_miss = WorkerCounters::myCounters().io_reads.load();
            if (old_miss != new_miss) {
               miss_count += new_miss - old_miss;
            }
         }
      }
      return res;
   }

   void insert_btree(Key k, Payload& v)
   {
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      btree.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
   }

   void insert(Key k, Payload& v) override
   {
      admit_element(k, v);
      //u8 key_bytes[sizeof(Key)];
      //btree.insert(key_bytes, fold(key_bytes, k), reinterpret_cast<u8*>(&v), sizeof(v));
   }

   bool should_migrate() {
      if (lazy_migration) {
         return utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void update(Key k, Payload& v) override
   {
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data().referenced = true;
         it.data().payload = v;
         hit_count++;
         return;
      }
      u8 key_bytes[sizeof(Key)];
      auto old_miss = WorkerCounters::myCounters().io_reads.load();
      btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
      auto new_miss = WorkerCounters::myCounters().io_reads.load();
      if (old_miss != new_miss) {
         miss_count += new_miss - old_miss;
      }
      if (should_migrate()) {
         admit_element(k, v);
         old_miss = WorkerCounters::myCounters().io_reads.load();
         btree.remove(key_bytes, fold(key_bytes, k));
         new_miss = WorkerCounters::myCounters().io_reads.load();
         if (old_miss != new_miss) {
            miss_count += new_miss - old_miss;
         }
      }
   }

   void report(u64 entries, u64 pages) override {
      std::cout << "Cache capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Cache size bytes " << get_cache_btree_size() << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      std::cout << "Cache BTree average leaf fill factor " << cache.get_stats().avgfill_leaves() << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Cache hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Cache miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
      auto btree_entries = (entries - cache.size());
      std::cout << "BTree # entries " << btree_entries << std::endl;
      std::cout << "BTree # pages " << pages << std::endl;
      std::cout << "BTree height " << btree.getHeight() << std::endl;
      auto minimal_pages = btree_entries * (sizeof(Key) + sizeof(Payload)) / leanstore::storage::PAGE_SIZE;
      std::cout << "BTree average fill factor " << minimal_pages / (pages + 0.0) << std::endl;
   }
};
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload {
   u8 value[size];
   BytesPayload() {}
   bool operator==(BytesPayload& other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
   bool operator!=(BytesPayload& other) { return !(operator==(other)); }
   BytesPayload(const BytesPayload& other) { std::memcpy(value, other.value, sizeof(value)); }
   BytesPayload& operator=(const BytesPayload& other)
   {
      std::memcpy(value, other.value, sizeof(value));
      return *this;
   }
};
}  // namespace leanstore
