#pragma once
#include "interface/StorageInterface.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "Units.hpp"
#include "stx/btree_map.h"

// -------------------------------------------------------------------------------------
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>

template<typename Key, typename Payload>
class AntiCacheBTreeAdapter: public leanstore::BTreeInterface<Key, Payload> {
public:
   leanstore::storage::btree::BTreeInterface& btree;
   rocksdb::DB* block_table;
   rocksdb::Options options;
   static constexpr double eviction_threshold = 0.99;
   std::size_t cache_capacity_bytes;
   std::size_t hit_count = 0;
   std::size_t miss_count = 0;
   std::size_t eviction_items = 0;
   typedef std::pair<Key, Payload> Element;
   double maintain_lru_sampling_threshold = 100;
   std::size_t payload_num = 0;
   std::size_t tombstone_num = 0;
   struct TombstoneRecord {
      uint32_t block_id;
      uint32_t tuple_offset;
   };

   struct TaggedPayload {
      Key key;
      Payload payload;
      TaggedPayload * prev = nullptr;
      TaggedPayload * next = nullptr;
   };

   TaggedPayload lru_head;

   void LRU_remove_self(TaggedPayload * node) {
      if (node->prev && node->next) {
         node->prev->next = node->next;
         node->next->prev = node->prev;
         node->next = node->prev = nullptr;
      }
   }

   void LRU_move_to_head(TaggedPayload * node) {
      LRU_remove_self(node);
      node->next = lru_head.next;
      node->prev = &lru_head;
      lru_head.next->prev = node;
      lru_head.next = node;
   }

   void LRU_move_to_tail(TaggedPayload * node) {
      LRU_remove_self(node);
      node->next = &lru_head;
      node->prev = lru_head.prev;
      lru_head.prev->next = node;
      lru_head.prev = node;
   }

   typedef stx::btree_map<Key, TaggedPayload*, std::less<Key>> cache_type;
   typedef stx::btree_map<Key, TombstoneRecord, std::less<Key>> evict_table_type;
   cache_type cache;
   evict_table_type evict_table;
   Key clock_hand = std::numeric_limits<Key>::max();
   AntiCacheBTreeAdapter(leanstore::storage::btree::BTreeInterface& btree, double cache_size_gb, double maintain_lru_sampling_threshold = 1 ) : btree(btree), cache_capacity_bytes(cache_size_gb * 1024ULL * 1024ULL * 1024ULL), maintain_lru_sampling_threshold(maintain_lru_sampling_threshold){
      // options.write_buffer_size = 4 * 1024 * 1024;
      // std::cout << "RocksDB block_storage_dir " << block_storage_dir << std::endl;
      // std::cout << "RocksDB write_buffer_size " << options.write_buffer_size << std::endl;
      // std::cout << "RocksDB max_write_buffer_number " << options.max_write_buffer_number << std::endl;
      // rocksdb::DestroyDB(block_storage_dir,options);
      // options.manual_wal_flush = false;
      // options.use_direct_reads = true;
      // options.create_if_missing = true;
      // options.stats_dump_period_sec = 3000;
      // options.compression = rocksdb::kNoCompression;
      // options.use_direct_io_for_flush_and_compaction = true;
      // rocksdb::BlockBasedTableOptions table_options;
      // table_options.block_size = 16 * 1024;
      // table_options.cache_index_and_filter_blocks = true;
      // table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
      // table_options.block_cache = rocksdb::NewLRUCache(storage_block_cache_size_gb * 1024ULL * 1024ULL * 1024ULL - options.write_buffer_size * options.max_write_buffer_number, 0, false, 0);
      // options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
      // rocksdb::Status status =
      //    rocksdb::DB::Open(options, block_storage_dir, &block_table);
      // assert(status.ok());


      lru_head.next = lru_head.prev = &lru_head;
   }

   void clear_stats() override {
      hit_count = miss_count = 0;
   }

   inline size_t get_cache_size() {
      auto & stat = cache.get_stats();
      auto leaves = stat.leaves;
      auto innernodes = stat.innernodes;
      return leaves * stat.leaf_node_size() + innernodes * stat.inner_node_size() + stat.itemcount * sizeof(TaggedPayload);
   }

   inline size_t get_evict_table_size() {
      auto & evict_table_stat = evict_table.get_stats();
      return evict_table_stat.leaves * evict_table_stat.leaf_node_size() + evict_table_stat.innernodes * evict_table_stat.inner_node_size();
   }


   bool cache_under_pressure() {
      return (get_cache_size() + get_evict_table_size()) >= cache_capacity_bytes * eviction_threshold;
   }

   static constexpr std::size_t evict_block_size = 1024;

   uint32_t block_id_counter = 0;

   void evict_all() override {
      while (cache.size() > 0) {
         evict_bunch();
      }
   }

   void evict_bunch() {
      std::vector<char> block;
      block.reserve(evict_block_size);
      // Start from tail and move backwards
      auto p = lru_head.prev; 
      uint32_t tuple_offset = 0;
      uint32_t block_id = block_id_counter++;
      auto tuple_size = sizeof(TaggedPayload) - sizeof(TaggedPayload::prev) - sizeof(TaggedPayload::next);
      while (&lru_head != p) {
         if (block.size() + tuple_size > evict_block_size) {
            break;
         }
         assert(evict_table.find(p->key) == evict_table.end());
         evict_table[p->key] = TombstoneRecord{block_id, tuple_offset};
         auto old_size = block.size();
         auto new_size = old_size + tuple_size;
         block.resize(new_size);
         memcpy((&block[0]) + old_size, (const char*)p, tuple_size);
         tuple_offset += tuple_size;

         auto t = p->prev;
         LRU_remove_self(p);
         cache.erase(p->key);
         delete p;
         p = t;
      }


      u8 key_bytes[sizeof(block_id)];

      //rocksdb::WriteOptions options;
      //options.disableWAL = true;
      auto key_len = leanstore::fold(key_bytes, block_id);
      //auto status = block_table->Put(options, rocksdb::Slice((const char *)key_bytes, key_len), rocksdb::Slice((const char *)&block[0], block.size()));
      //assert(status == rocksdb::Status::OK());
      auto op_res = btree.insert(key_bytes, key_len, reinterpret_cast<u8*>(block.data()), block.size());
      assert(op_res == leanstore::storage::btree::OP_RESULT::OK);
   }

   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_bunch();
      }
   }

   void admit_element_no_eviction(Key k, Payload & v, bool head = true) {
      assert(cache.find(k) == cache.end());
      //assert(evict_table.find(k) == evict_table.end());
      auto new_node = new TaggedPayload{k, v};
      cache[k] = new_node;
      if (head) {
         LRU_move_to_head(new_node);
      } else {
         LRU_move_to_tail(new_node);
      }
   }

   void admit_element(Key k, Payload & v, bool head = true) {
      if (cache_under_pressure())
         evict_till_safe();
      admit_element_no_eviction(k, v, head);
   }

   void bring_tuple_from_block_table(Key k, const TombstoneRecord & trec) {
      assert(evict_table.find(k) != evict_table.end());
      auto block_id = trec.block_id;

      //rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(block_id)];
      auto key_len = leanstore::fold(key_bytes, block_id);
      std::string block;
      block.reserve(evict_block_size);
      auto status = btree.lookup(key_bytes, key_len, [&](const u8* payload, u16 payload_length) { 
         block.resize(payload_length); 
         memcpy(&block[0], payload, payload_length); });

      assert(status == leanstore::storage::btree::OP_RESULT::OK);
      //assert(evict_block_size >= block.size());

      auto tuple_size = sizeof(TaggedPayload) - sizeof(TaggedPayload::prev) - sizeof(TaggedPayload::next);

      // Strategy #1: Merge all tuples in the block back to cache
      for (std::size_t off = 0; off < block.size(); off += tuple_size) {
         Key new_key;
         Payload payload;
         memcpy(&new_key, &block[off], sizeof(Key));
         memcpy(&payload, &block[off + sizeof(Key)], sizeof(Payload));
         assert(sizeof(Key) + sizeof(Payload) == tuple_size);
         if (new_key == k) {
            // Put the tuple we are about to access near the head of the LRU list
            admit_element_no_eviction(new_key, payload, true); 
         } else {
            // Put the tuple near the end of the LRU list
            admit_element_no_eviction(new_key, payload, false); 
         }
         assert(evict_table.find(new_key) != evict_table.end());
         evict_table.erase(new_key);
      }

      // Erase the block from the block table
      // rocksdb::WriteOptions write_options;
      // write_options.disableWAL = true;
      // status = block_table->Delete(write_options, rocksdb::Slice((const char *)key_bytes, key_len));
      // assert(status == rocksdb::Status::OK());
      auto op_res __attribute__((unused)) = btree.remove(key_bytes, key_len);
      assert(op_res == leanstore::storage::btree::OP_RESULT::OK);

      if (cache_under_pressure())
         evict_till_safe();
   }

   bool lookup(Key k, Payload& v) override
   {
      auto it = cache.find(k);
      if (it != cache.end()) {
         v = it.data()->payload;
         if (maintain_lru()) {
            LRU_move_to_head(it.data());
         }
         hit_count++;
         return true;
      }
      auto evict_it = evict_table.find(k);
      if (evict_it == evict_table.end()) {
         return false;
      }
      miss_count++;
      bring_tuple_from_block_table(k, evict_it->second);

      return lookup(k, v);
   }

   void insert(Key k, Payload& v) override
   {
      admit_element(k, v);
   }

   bool maintain_lru() {
      return leanstore::utils::RandomGenerator::getRandU64(0, 100) <= maintain_lru_sampling_threshold;
   }

   void update(Key k, Payload& v) override
   {
      auto it = cache.find(k);
      if (it != cache.end()) {
         it.data()->payload = v;
         if (maintain_lru()) {
            LRU_move_to_head(it.data());
         }
         hit_count++;
         return;
      }
      auto evict_it = evict_table.find(k);
      if (evict_it != evict_table.end()) {
         miss_count++;
         bring_tuple_from_block_table(k, evict_it->second);
         update(k, v);
         return;
      }

      admit_element(k, v);
   }

   void put(Key k, Payload& v) override
   {
      update(k, v);
   }

   void report_cache() {
      std::cout << "Cache capacity bytes " << cache_capacity_bytes << std::endl;
      std::cout << "Cache size bytes " << get_cache_size() << std::endl;
      std::cout << "Cache size # entries " << cache.size() << std::endl;
      std::cout << "Evict table size bytes " << get_evict_table_size() << std::endl;
      std::cout << "Evict table # entries " << evict_table.size() << std::endl;

      std::cout << "Cache hits/misses " << hit_count << "/" << miss_count << std::endl;
      double hit_rate = hit_count / (hit_count + miss_count + 1.0);
      std::cout << "Cache hit rate " << hit_rate * 100 << "%" << std::endl;
      std::cout << "Cache miss rate " << (1 - hit_rate) * 100  << "%"<< std::endl;
   }

   void report(u64, u64) override {
      report_cache();
   }
};