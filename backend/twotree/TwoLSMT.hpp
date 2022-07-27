#pragma once
#include <unordered_map>
#include <iostream>
#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
#include "ART/ARTIndex.hpp"
#include "stx/btree_map.h"
#include "leanstore/BTreeAdapter.hpp"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/filter_policy.h"

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{

template <typename Key, typename Payload>
struct TwoRocksDBAdapter : public leanstore::BTreeInterface<Key, Payload> {
   rocksdb::DB* top_db = nullptr;
   rocksdb::DB* bottom_db = nullptr;
   rocksdb::Options toptree_options;
   rocksdb::BlockBasedTableOptions toptree_table_options;
   rocksdb::Options bottom_options;
   bool lazy_migration;
   bool wal;
   bool inclusive;

   u64 lazy_migration_threshold = 1;

   std::size_t topdb_item_count = 0;
   std::size_t cache_size_bytes;
   std::size_t cache_capacity_bytes;
   static constexpr double eviction_threshold = 0.99;

   Key clock_hand = std::numeric_limits<Key>::max();

   struct TaggedPayload {
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };
   rocksdb::SstFileManager * top_file_manager;

   TwoRocksDBAdapter(const std::string & db_dir, double toptree_cache_budget_gib, double bottomtree_cache_budget_gib, bool wal = false, int lazy_migration_sampling_rate = 100, bool inclusive = false): lazy_migration(lazy_migration_sampling_rate < 100), wal(wal), inclusive(inclusive) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      this->cache_size_bytes = 0;
      this->cache_capacity_bytes = toptree_cache_budget_gib * 1024 * 1024 * 1024;
      toptree_options.write_buffer_size = 8 * 1024 * 1024;
      std::cout << "RocksDB top cache budget " << (toptree_cache_budget_gib) << " gib" << std::endl;
      std::cout << "RocksDB top write_buffer_size " << toptree_options.write_buffer_size << std::endl;
      std::cout << "RocksDB top max_write_buffer_number " << toptree_options.max_write_buffer_number << std::endl;
      std::size_t top_block_cache_size = (toptree_cache_budget_gib) * 1024ULL * 1024ULL * 1024ULL - toptree_options.write_buffer_size * toptree_options.max_write_buffer_number;
      std::cout << "RocksDB top block cache size " << top_block_cache_size / 1024.0 /1024.0/1024 << " gib" << std::endl;
      const std::string toptree_dir = db_dir + "/toptree";
      const std::string bottom_dir = db_dir + "/bottomtree";
      mkdir(toptree_dir.c_str(), 0777);
      mkdir(bottom_dir.c_str(), 0777);
      rocksdb::DestroyDB(toptree_dir,toptree_options);
      rocksdb::DestroyDB(bottom_dir, bottom_options);
      toptree_options.manual_wal_flush = false;
      toptree_options.use_direct_reads = true;
      toptree_options.create_if_missing = true;
      toptree_options.stats_dump_period_sec = 3000;
      toptree_options.compression = rocksdb::kNoCompression;
      toptree_options.use_direct_io_for_flush_and_compaction = true;
      toptree_options.sst_file_manager.reset(rocksdb::NewSstFileManager(rocksdb::Env::Default()));
      {
         toptree_table_options.block_cache = rocksdb::NewLRUCache(top_block_cache_size, 0, true, 0);
         toptree_table_options.cache_index_and_filter_blocks = true;
         toptree_table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
         toptree_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(toptree_table_options));
         rocksdb::Status status = rocksdb::DB::Open(toptree_options, toptree_dir, &top_db);
         assert(status.ok());
      }

      bottom_options = toptree_options;
      {
         bottom_options.sst_file_manager.reset();
         bottom_options.write_buffer_size = 8 * 1024 * 1024;
         rocksdb::BlockBasedTableOptions table_options;
         std::cout << "RocksDB bottom cache budget " << (bottomtree_cache_budget_gib) << " gib" << std::endl;
         std::cout << "RocksDB bottom write_buffer_size " << bottom_options.write_buffer_size << std::endl;
         std::cout << "RocksDB bottom max_write_buffer_number " << bottom_options.max_write_buffer_number << std::endl;
         std::size_t bottom_block_cache_size = (bottomtree_cache_budget_gib) * 1024ULL * 1024ULL * 1024ULL - bottom_options.write_buffer_size * bottom_options.max_write_buffer_number;
         std::cout << "RocksDB bottom block cache size " << bottom_block_cache_size / 1024.0 /1024.0/1024 << " gib" << std::endl;
         table_options.block_cache = rocksdb::NewLRUCache(bottom_block_cache_size, 0, true, 0);
         table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
         table_options.cache_index_and_filter_blocks = true;
         bottom_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
         rocksdb::Status status = rocksdb::DB::Open(bottom_options, bottom_dir, &bottom_db);
         assert(status.ok());
      }
   }
   
   void evict_all() override {
      evict_all_items();
      rocksdb::FlushOptions fopts;
      top_db->Flush(fopts);
      std::cout << "After deleting all entries, rocksdb files size " << toptree_options.sst_file_manager->GetTotalSize();
      rocksdb::CompactRangeOptions options;
      auto s = top_db->CompactRange(options, nullptr, nullptr);
      assert(s == rocksdb::Status::OK());
      std::cout << "After full compaction, rocksdb files size " << toptree_options.sst_file_manager->GetTotalSize();
      toptree_table_options.block_cache->EraseUnRefEntries();
   }

   void clear_stats() {
      top_db->ResetStats();
      bottom_db->ResetStats();
   }

   bool sample() {
      return leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
   }

   bool should_migrate() {
      if (lazy_migration) {
         return leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void evict_till_safe() {
      while (cache_under_pressure()) {
         evict_a_bunch();
      }
   }

   bool cache_under_pressure() {
      return //cache_size_bytes >= cache_capacity_bytes * eviction_threshold || 
             toptree_options.sst_file_manager->GetTotalSize() >= 2 * cache_capacity_bytes * eviction_threshold;
   }

   void evict_all_items() {
      Key start_key = std::numeric_limits<Key>::min();

      rocksdb::ReadOptions options;

      auto it = top_db->NewIterator(options);
      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      Key evict_key;
      Payload evict_payload;
      bool victim_found = false;

      leanstore::fold(key_bytes, start_key);
      for (it->Seek(rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes))); it->Valid(); it->Next()) {
         assert(it->value().size() == sizeof(TaggedPayload));
         auto tp = ((TaggedPayload*)(it->value().data()));
         auto real_key = leanstore::unfold(*(Key*)(it->key().data()));
         evict_key = real_key;
         clock_hand = real_key;
         victim_found = true;
         evict_keys.push_back(real_key);
         evict_payloads.emplace_back(*tp);
      }

      delete it;
      
      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            rocksdb::WriteOptions options;
            options.disableWAL = true;
            leanstore::fold(key_bytes, key);
            rocksdb::Status s = top_db->Delete(options, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)));
            assert(s == rocksdb::Status::OK());

            //cache_size_bytes -= (sizeof(Key) + sizeof(TaggedPayload));
            //--topdb_item_count;
            if (inclusive) {
               if (tagged_payload.modified) {
                  put_bottom_db(key, tagged_payload.payload); // put it back in the on-disk LSMT
               }
            } else { // exclusive, put it back in the on-disk LSMT
               put_bottom_db(key, tagged_payload.payload);
            }
         }
      } else {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }

   void evict_a_bunch() {
      Key start_key = clock_hand;
      if (start_key == std::numeric_limits<Key>::max()) {
         start_key = std::numeric_limits<Key>::min();
      }
      rocksdb::ReadOptions options;

      auto it = top_db->NewIterator(options);
      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      Key evict_key;
      Payload evict_payload;
      bool victim_found = false;

      leanstore::fold(key_bytes, start_key);
      for (it->Seek(rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes))); it->Valid(); it->Next()) {
         assert(it->value().size() == sizeof(TaggedPayload));
         auto tp = ((TaggedPayload*)(it->value().data()));
         if (tp->referenced == true) {
            TaggedPayload tp2 = *tp;
            tp2.referenced = false;
            rocksdb::WriteOptions options;
            options.disableWAL = true;
            auto s = top_db->Put(options, it->key(), rocksdb::Slice((const char *)&tp2, sizeof(TaggedPayload)));
            assert(s == rocksdb::Status::OK());
            continue;
         }
         auto real_key = leanstore::unfold(*(Key*)(it->key().data()));
         evict_key = real_key;
         clock_hand = real_key;
         victim_found = true;
         evict_keys.push_back(real_key);
         evict_payloads.emplace_back(*tp);
         if (evict_keys.size() >= 1000) {
            break;
         }
      }

      delete it;
      
      if (victim_found) {
         for (size_t i = 0; i< evict_keys.size(); ++i) {
            auto key = evict_keys[i];
            auto tagged_payload = evict_payloads[i];
            rocksdb::WriteOptions options;
            options.disableWAL = true;
            leanstore::fold(key_bytes, key);
            rocksdb::Status s = top_db->Delete(options, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)));
            assert(s == rocksdb::Status::OK());

            // cache_size_bytes -= (sizeof(Key) + sizeof(TaggedPayload));
            // --topdb_item_count;
            if (inclusive) {
               if (tagged_payload.modified) {
                  put_bottom_db(key, tagged_payload.payload); // put it back in the on-disk LSMT
               }
            } else { // exclusive, put it back in the on-disk LSMT
               put_bottom_db(key, tagged_payload.payload);
            }
         }
         // u8 begin_key_bytes[sizeof(Key)];
         // u8 end_key_bytes[sizeof(Key)];
         // rocksdb::CompactRangeOptions options;
         // leanstore::fold(begin_key_bytes, evict_keys[0]);
         // leanstore::fold(end_key_bytes, evict_keys.back());
         // rocksdb::Slice begin_key((const char *)begin_key_bytes, sizeof(begin_key_bytes));
         // rocksdb::Slice end_key((const char *)end_key_bytes, sizeof(end_key_bytes));
         // auto s = top_db->CompactRange(options, &begin_key, &end_key);
         // assert(s == rocksdb::Status::OK());
      } else {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }

   void put(Key k, Payload& v) {
      admit_element(k, v, true, false);
   }

   void put_bottom_db(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      rocksdb::WriteOptions options;
      options.disableWAL = true;
      leanstore::fold(key_bytes, k);
      rocksdb::Status s = bottom_db->Put(options, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)), rocksdb::Slice((const char *)&v, sizeof(Payload)));
      assert(s == rocksdb::Status::OK());
   }

   void admit_element(Key k, Payload & v, bool dirty = false, bool insert = false) {
      if (sample() && cache_under_pressure())
         evict_till_safe();
      rocksdb::WriteOptions options;
      options.disableWAL = true;
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      tp.payload = v;
      tp.modified = dirty;
      tp.referenced = true;
      auto key_len = leanstore::fold(key_bytes, k);
      auto status = top_db->Put(options, rocksdb::Slice((const char *)key_bytes, key_len), rocksdb::Slice((const char *)&tp, sizeof(tp)));
      assert(status == rocksdb::Status::OK());
      // if (insert) {
      //    cache_size_bytes += sizeof(Key) + sizeof(TaggedPayload);
      //    topdb_item_count++;
      // }
   }

   bool lookup(Key k, Payload& v) {
      rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto status = top_db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);

      if (status == rocksdb::Status::OK()) {
         assert(sizeof(TaggedPayload) == value.size());
         auto tp = ((TaggedPayload*)(value.data()));
         v = tp->payload;
         if (tp->referenced == false) {
            // set reference bit
            admit_element(k, v, tp->modified, false);
         }
         return true;
      }

      value.clear();
      status = bottom_db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);

      if (status == rocksdb::Status::OK()) {
         assert(sizeof(Payload) == value.size());
         v = *((Payload*)(value.data()));
         if (inclusive == false) {
            rocksdb::WriteOptions write_options;
            write_options.disableWAL = true;
            bottom_db->Delete(write_options, rocksdb::Slice((const char *)key_bytes, key_len));
         }
         admit_element(k, v, false, true /* insert */ );
         return true;
      }

      return false;
   }

   void insert(Key k, Payload& v) {
      admit_element(k, v, true/* dirty */, true/* insert */);
   }

   void update(Key k, Payload& v) {
      Payload t;
      //read
      auto status __attribute__((unused)) = lookup(k, t);
      //modify
      memcpy(&t, &v, sizeof(v));
      //write
      admit_element(k, t, true /* dirty */, false);
   }

   void put_top_db(Key k, Payload& v) {
      admit_element(k, v, true);
   }

   void report_db(rocksdb::DB * db, const string & db_tag) {
      std::string val;
      auto res __attribute__((unused)) = db->GetProperty("rocksdb.block-cache-usage", &val);
      assert(res);
      std::cout << db_tag << " RocksDB block-cache-usage " << val << std::endl;
      res = db->GetProperty("rocksdb.estimate-num-keys", &val);
      assert(res);
      std::cout << db_tag << " RocksDB est-num-keys " <<val << std::endl;

      res = db->GetProperty("rocksdb.stats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB stats " <<val << std::endl;

      res = db->GetProperty("rocksdb.dbstats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB dbstats " <<val << std::endl;
      res = db->GetProperty("rocksdb.levelstats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB levelstats\n" <<val << std::endl;
      res = db->GetProperty("rocksdb.block-cache-entry-stats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB block-cache-entry-stats " <<val << std::endl;
   }
   void report(u64, u64) override {
      report_db(top_db, "Top");
      report_db(bottom_db, "Bottom");
   }
};

}