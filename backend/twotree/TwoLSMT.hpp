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
#include "rocksdb/merge_operator.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

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
   bool inclusive;

   u64 lazy_migration_threshold = 10;

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

   TwoRocksDBAdapter(const std::string & db_dir, double toptree_cache_budget_gib, double bottomtree_cache_budget_gib, int lazy_migration_sampling_rate = 100, bool inclusive = false): lazy_migration(lazy_migration_sampling_rate < 100), inclusive(inclusive) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      this->cache_size_bytes = 0;
      this->cache_capacity_bytes = toptree_cache_budget_gib * 1024 * 1024 * 1024;
      toptree_options.write_buffer_size = 8 * 1024 * 1024;
      std::cout << "lazy_migration_threshold " << lazy_migration_threshold << std::endl;
      std::cout << "lazy_migration " << lazy_migration << std::endl;
      std::cout << "inclusive cache  " << inclusive << std::endl;
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

   int empty_slots = 0;
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
            empty_slots++;
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
      if (empty_slots <= 0 && cache_under_pressure())
         evict_a_bunch();
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
      if (empty_slots > 0) {
         empty_slots--;
      }
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

      assert(status == rocksdb::Status::OK());
      if (status == rocksdb::Status::OK()) {
         assert(sizeof(Payload) == value.size());
         v = *((Payload*)(value.data()));
         if (should_migrate()) {
            if (inclusive == false) {
               rocksdb::WriteOptions write_options;
               write_options.disableWAL = true;
               bottom_db->Delete(write_options, rocksdb::Slice((const char *)key_bytes, key_len));
            }
            admit_element(k, v, false, true /* insert */ );
         }
         return true;
      }

      return false;
   }

   void insert(Key k, Payload& v) {
      admit_element(k, v, true/* dirty */, true/* insert */);
   }

   void update(Key k, Payload& v) {
      Payload t;
      // //read
      // auto status __attribute__((unused)) = lookup(k, t);
      // //modify
      // memcpy(&t, &v, sizeof(v));
      // //write
      // admit_element(k, t, true /* dirty */, false);

      rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto status = top_db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);

      if (status == rocksdb::Status::OK()) {
         assert(sizeof(TaggedPayload) == value.size());
         // update top tree
         admit_element(k, v, /* dirty */ true, false);
         return;
      }

      value.clear();
      status = bottom_db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);

      if (status == rocksdb::Status::OK()) {
         if (should_migrate()) {
            assert(sizeof(Payload) == value.size());
            if (inclusive == false) {
               rocksdb::WriteOptions write_options;
               write_options.disableWAL = true;
               status = bottom_db->Delete(write_options, rocksdb::Slice((const char *)key_bytes, key_len));
               assert(status == rocksdb::Status::OK());
            }
            admit_element(k, v, /* dirty */ true, /* insert */ false  );
         } else {
            put_bottom_db(k, v);
         }
      }

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



template <typename Key, typename Payload>
struct RocksDBTwoCFAdapter : public leanstore::BTreeInterface<Key, Payload> {
   rocksdb::DB* db = nullptr;
   //rocksdb::DB* bottom_db = nullptr;
   rocksdb::Options db_options;
   rocksdb::BlockBasedTableOptions table_options;
   rocksdb::ColumnFamilyHandle* top_handle = nullptr;
   rocksdb::ColumnFamilyHandle* bottom_handle = nullptr;
   //rocksdb::Options bottom_options;
   bool lazy_migration;
   bool inclusive;

   u64 lazy_migration_threshold = 10;

   std::size_t topdb_item_count = 0;
   std::size_t cache_size_bytes;
   std::size_t cache_capacity_bytes;
   std::size_t lookup_hit_top = 0;
   std::size_t total_lookups = 0;
   std::size_t upward_migrations = 0;
   std::size_t downward_migrations = 0;
   std::size_t blocks_read = 0;
   static constexpr double eviction_threshold = 0.99;

   Key clock_hand = std::numeric_limits<Key>::max();

   struct TaggedPayload {
      Payload payload;
      bool modified = false;
      bool referenced = false;
   };
   rocksdb::SstFileManager * top_file_manager;

   class ToggleReferenceBitOperator : public rocksdb::AssociativeMergeOperator {
   public:
      virtual ~ToggleReferenceBitOperator() {}
      virtual bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
                         const rocksdb::Slice& value, std::string* new_value,
                         rocksdb::Logger* logger) const override {
         assert(existing_value != nullptr);
         assert(existing_value->size() == sizeof(TaggedPayload));
         TaggedPayload new_payload = *((TaggedPayload*)(existing_value->data()));
         char toggle_value = value.data()[0];
         assert(toggle_value == kToggleReferenceBitOff || toggle_value == kToggleReferenceBitOn);
         new_payload.referenced = (bool)toggle_value;
         *new_value = std::string((const char *)&new_payload, sizeof(new_payload));
         return true;// always return true for this, since we treat all errors as "zero".
      }

      virtual const char* Name() const override {
         return "ToggleReferenceBitOperator";
      }
   };
   RocksDBTwoCFAdapter(const std::string & db_dir, double toptree_cache_budget_gib, double dram_budget_gib, int lazy_migration_sampling_rate = 100, bool inclusive = false): lazy_migration(lazy_migration_sampling_rate < 100), inclusive(inclusive) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      this->cache_size_bytes = 0;
      this->cache_capacity_bytes = toptree_cache_budget_gib * 1024 * 1024 * 1024;
      db_options.write_buffer_size = 8 * 1024 * 1024;
      db_options.create_if_missing = true;
      std::cout << "lazy_migration_threshold " << lazy_migration_threshold << std::endl;
      std::cout << "lazy_migration " << lazy_migration << std::endl;
      std::cout << "inclusive cache  " << inclusive << std::endl;
      std::cout << "2LSMT top tree size " << (toptree_cache_budget_gib) << " gib" << std::endl;
      std::cout << "RocksDB cache budget " << (dram_budget_gib) << " gib" << std::endl;
      std::cout << "RocksDB write_buffer_size " << db_options.write_buffer_size << std::endl;
      std::cout << "RocksDB max_write_buffer_number " << db_options.max_write_buffer_number << std::endl;
   
      mkdir(db_dir.c_str(), 0777);
      rocksdb::DestroyDB(db_dir,db_options);

      rocksdb::Status status = rocksdb::DB::Open(db_options, db_dir, &db);
      // create column family for top tree
      // use default column family as the bottom tree
      rocksdb::ColumnFamilyHandle* cf;
      status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "t", &cf);
      assert(status.ok());

      // close DB
      status = db->DestroyColumnFamilyHandle(cf);
      assert(status.ok());
      delete db;

      db_options.manual_wal_flush = false;
      db_options.use_direct_reads = true;
      db_options.create_if_missing = true;
      db_options.stats_dump_period_sec = 3000;
      db_options.compression = rocksdb::kNoCompression;
      db_options.use_direct_io_for_flush_and_compaction = true;
      //db_options.sst_file_manager.reset(rocksdb::NewSstFileManager(rocksdb::Env::Default()));
      std::size_t block_cache_size = (dram_budget_gib) * 1024ULL * 1024ULL * 1024ULL - db_options.write_buffer_size * db_options.max_write_buffer_number;
      std::cout << "RocksDB block cache size " << block_cache_size / 1024.0 /1024.0/1024 << " gib" << std::endl;

      table_options.block_size = 16 * 1024;
      table_options.block_cache = rocksdb::NewLRUCache(block_cache_size, 0, true, 0);
      //table_options.pin_l0_filter_and_index_blocks_in_cache = true;
      table_options.cache_index_and_filter_blocks = true;
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
      db_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
      db_options.statistics = rocksdb::CreateDBStatistics();

      assert(status.ok());

      db_options.merge_operator.reset(new ToggleReferenceBitOperator());
      // open DB with two column families
      std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
      // have to open default column family
      auto top_cf_options = rocksdb::ColumnFamilyOptions(db_options);
      //top_cf_options.compaction_pri = rocksdb::kOldestLargestSeqFirst;
      assert(top_cf_options.merge_operator.get());
      //top_cf_options.memtable_prefix_bloom_size_ratio = 0.05;
      //top_cf_options.memtable_whole_key_filtering = true;
      auto bottom_cf_options = rocksdb::ColumnFamilyOptions(db_options);
      //bottom_cf_options.memtable_prefix_bloom_size_ratio = 0.05;
      //bottom_cf_options.memtable_whole_key_filtering = true;
      assert(bottom_cf_options.merge_operator.get());
      //bottom_cf_options.merge_operator.reset();
      std::cout << "RocksDB top cf write buffer size " << top_cf_options.write_buffer_size / 1024.0 /1024.0 << " mb" << std::endl;
      std::cout << "RocksDB bottom cf write buffer size " << top_cf_options.write_buffer_size / 1024.0 /1024.0 << " mb" << std::endl;
      column_families.push_back(rocksdb::ColumnFamilyDescriptor(
            ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, top_cf_options));
      // open the new one, too
      column_families.push_back(rocksdb::ColumnFamilyDescriptor(
            "t", bottom_cf_options));
      
      std::vector<rocksdb::ColumnFamilyHandle*> handles;
      status = rocksdb::DB::Open(db_options, db_dir, column_families, &handles, &db);
      top_handle = handles[1];
      bottom_handle = handles[0];
      assert(status.ok());
      rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
   }
   
   void evict_all() override {
      evict_all_items();
      rocksdb::CompactRangeOptions options;
      auto s = db->CompactRange(options, top_handle, nullptr, nullptr);
      assert(s == rocksdb::Status::OK());
      std::cout << "After full compaction, top column family size " << top_db_approximate_size();
      //rocksdb::FlushOptions fopts;
      // db->Flush(fopts);
      // std::cout << "After deleting all entries, rocksdb files size " << toptree_options.sst_file_manager->GetTotalSize();
      // rocksdb::CompactRangeOptions options;
      // auto s = top_db->CompactRange(options, nullptr, nullptr);
      // assert(s == rocksdb::Status::OK());
      // std::cout << "After full compaction, rocksdb files size " << toptree_options.sst_file_manager->GetTotalSize();
      // toptree_table_options.block_cache->EraseUnRefEntries();
   }

   void clear_stats() {
      db->ResetStats();
      lookup_hit_top = 0;
      total_lookups = 0;
      // rocksdb::FlushOptions fopts;
      // db->Flush(fopts);
      rocksdb::get_iostats_context()->Reset();
      rocksdb::get_perf_context()->Reset();
      downward_migrations = upward_migrations = 0;
      blocks_read = 0;
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

   std::size_t top_db_approximate_size() {
      std::array<rocksdb::Range, 1> ranges;
      std::array<uint64_t, 1> sizes;
      u8 start_key_bytes[sizeof(Key)];
      u8 end_key_bytes[sizeof(Key)];
      leanstore::fold(start_key_bytes, std::numeric_limits<Key>::min());
      leanstore::fold(end_key_bytes, std::numeric_limits<Key>::max());
      ranges[0].start = rocksdb::Slice((const char *)start_key_bytes, sizeof(Key));
      ranges[0].limit = rocksdb::Slice((const char *)end_key_bytes, sizeof(Key));

      rocksdb::SizeApproximationOptions options;
 
      options.include_memtables = true;
      options.files_size_error_margin = 0.05;
      rocksdb::Status s = db->GetApproximateSizes(options, top_handle, ranges.data(), 1, sizes.data());
      assert(s.ok());
      return sizes[0];
   }

   bool cache_under_pressure() {
      return top_db_approximate_size() >= 1 * cache_capacity_bytes * eviction_threshold;
   }

   void evict_all_items() {
      Key start_key = std::numeric_limits<Key>::min();

      rocksdb::ReadOptions options;

      auto it = db->NewIterator(options, top_handle);
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
            //rocksdb::Status s = db->Delete(options, top_handle, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)));
            //assert(s == rocksdb::Status::OK());
            empty_slots++;
            if (inclusive) {
               if (tagged_payload.modified) {
                  put_bottom_db(key, tagged_payload.payload); // put it back in the on-disk LSMT
               }
            } else { // exclusive, put it back in the on-disk LSMT
               put_bottom_db(key, tagged_payload.payload);
            }
         }
         rocksdb::WriteOptions options;
         options.disableWAL = true;
         u8 begin_key_bytes[sizeof(Key)];
         u8 end_key_bytes[sizeof(Key)];
         leanstore::fold(begin_key_bytes, std::numeric_limits<Key>::min());
         leanstore::fold(end_key_bytes, std::numeric_limits<Key>::max());
         rocksdb::Status s = db->DeleteRange(options, top_handle, 
                                             rocksdb::Slice((const char *)begin_key_bytes, sizeof(begin_key_bytes)), 
                                             rocksdb::Slice((const char *)end_key_bytes, sizeof(end_key_bytes)));
         assert(s.ok());
      } else {
         clock_hand = std::numeric_limits<Key>::max();
      }
   }
   
   constexpr static char kToggleReferenceBitOff = 0;
   constexpr static char kToggleReferenceBitOn = 1;
   int empty_slots = 0;
   void evict_a_bunch() {
      static constexpr int kClockWalkSteps = 512;
      int steps = kClockWalkSteps;
      Key start_key = clock_hand;
      if (start_key == std::numeric_limits<Key>::max()) {
         start_key = std::numeric_limits<Key>::min();
      }
      rocksdb::ReadOptions options;

      auto it = db->NewIterator(options, top_handle);
      u8 key_bytes[sizeof(Key)];
      std::vector<Key> evict_keys;
      std::vector<TaggedPayload> evict_payloads;
      Key evict_key;
      Payload evict_payload;
      bool victim_found = false;

      leanstore::fold(key_bytes, start_key);
      for (it->Seek(rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes))); it->Valid(); it->Next()) {
         if (--steps == 0) {
            break;
         }
         assert(it->value().size() == sizeof(TaggedPayload));
         auto tp = ((TaggedPayload*)(it->value().data()));
         if (tp->referenced == true) {
            // TaggedPayload tp2 = *tp;
            // tp2.referenced = false;
            rocksdb::WriteOptions options;
            options.disableWAL = true;
            //auto s = db->Put(options, top_handle, it->key(), rocksdb::Slice((const char *)&tp2, sizeof(TaggedPayload)));
            rocksdb::Status s = db->Merge(options, top_handle, it->key(), rocksdb::Slice(&kToggleReferenceBitOff, sizeof(kToggleReferenceBitOff)));
            assert(s == rocksdb::Status::OK());
            continue;
         }
         auto real_key = leanstore::unfold(*(Key*)(it->key().data()));
         evict_key = real_key;
         clock_hand = real_key;
         victim_found = true;
         evict_keys.push_back(real_key);
         evict_payloads.emplace_back(*tp);
         if (evict_keys.size() >= 256) {
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
            rocksdb::Status s = db->Delete(options, top_handle, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)));
            assert(s == rocksdb::Status::OK());
            empty_slots++;
            if (inclusive) {
               if (tagged_payload.modified) {
                  put_bottom_db(key, tagged_payload.payload); // put it back in the on-disk LSMT
               }
            } else { // exclusive, put it back in the on-disk LSMT
               put_bottom_db(key, tagged_payload.payload);
            }
            downward_migrations++;
         }
      } else {
         if (steps == kClockWalkSteps) {
            clock_hand = std::numeric_limits<Key>::max();
         }
      }
   }

   void put(Key k, Payload& v) {
      rocksdb::ReadOptions ropts;
      std::string value;
      u8 key_bytes[sizeof(Key)];
      leanstore::fold(key_bytes, k);
      if (should_migrate() || 
         db->KeyMayExist(ropts, top_handle, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)), &value, nullptr, nullptr)) {
         admit_element(k, v, true);
      } else {
         put_bottom_db(k, v);
      }
   }

   void put_bottom_db(Key k, Payload & v) {
      u8 key_bytes[sizeof(Key)];
      rocksdb::WriteOptions options;
      options.disableWAL = true;
      leanstore::fold(key_bytes, k);
      rocksdb::Status s = db->Put(options, bottom_handle, rocksdb::Slice((const char *)key_bytes, sizeof(key_bytes)), rocksdb::Slice((const char *)&v, sizeof(Payload)));
      assert(s == rocksdb::Status::OK());
   }

   static constexpr int kEvictionCheckInterval = 50;
   int measure_size_countdown = kEvictionCheckInterval;

   void try_eviction() {
      if (--measure_size_countdown == 0) {
         if (cache_under_pressure()) {
            evict_a_bunch();
         }
         measure_size_countdown = kEvictionCheckInterval;
      }
   }

   void admit_element(Key k, Payload & v, bool dirty = false) {
      if (empty_slots <= 0) {
         try_eviction();
      }
         
      rocksdb::WriteOptions options;
      options.disableWAL = true;
      u8 key_bytes[sizeof(Key)];
      TaggedPayload tp;
      tp.payload = v;
      tp.modified = dirty;
      tp.referenced = true;
      auto key_len = leanstore::fold(key_bytes, k);
      auto status = db->Put(options, top_handle, rocksdb::Slice((const char *)key_bytes, key_len), rocksdb::Slice((const char *)&tp, sizeof(tp)));
      assert(status == rocksdb::Status::OK());
      if (empty_slots > 0) {
         empty_slots--;
      }
   }

   void set_referenced_bit(Key k) {
      if (empty_slots <= 0) {
         try_eviction();
      }
      rocksdb::WriteOptions options;
      options.disableWAL = true;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      rocksdb::Status s = db->Merge(options, top_handle, rocksdb::Slice((const char*)key_bytes, key_len), 
                                    rocksdb::Slice(&kToggleReferenceBitOn, sizeof(kToggleReferenceBitOn)));
      assert(s == rocksdb::Status::OK());
   }

   bool lookup(Key k, Payload& v) {
      try_eviction();
      rocksdb::ReadOptions options;
      total_lookups++;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto blocks_read_old = rocksdb::get_perf_context()->block_read_count;
      auto status = db->Get(options, top_handle, rocksdb::Slice((const char *)key_bytes, key_len), &value);
      assert(rocksdb::get_perf_context()->block_read_count - blocks_read_old >= 0);
      blocks_read += rocksdb::get_perf_context()->block_read_count - blocks_read_old;
      if (status == rocksdb::Status::OK()) {
         lookup_hit_top++;
         assert(sizeof(TaggedPayload) == value.size());
         auto tp = ((TaggedPayload*)(value.data()));
         v = tp->payload;
         if (tp->referenced == false) {
            // set reference bit
            //admit_element(k, v, tp->modified, false);
            blocks_read_old = rocksdb::get_perf_context()->block_read_count;
            set_referenced_bit(k);
            assert(rocksdb::get_perf_context()->block_read_count - blocks_read_old >= 0);
            blocks_read += rocksdb::get_perf_context()->block_read_count - blocks_read_old;
         }
         // if (leanstore::utils::RandomGenerator::getRandU64(0, 100) < 1) {
         //    admit_element(k, tp->payload, tp->modified, false);
         // }
         return true;
      }

      value.clear();
      status = db->Get(options, bottom_handle, rocksdb::Slice((const char *)key_bytes, key_len), &value);

      assert(status == rocksdb::Status::OK());
      if (status == rocksdb::Status::OK()) {
         assert(sizeof(Payload) == value.size());
         v = *((Payload*)(value.data()));
         if (should_migrate()) {
            upward_migrations++;
            if (inclusive == false) {
               rocksdb::WriteOptions write_options;
               write_options.disableWAL = true;
               db->Delete(write_options, bottom_handle, rocksdb::Slice((const char *)key_bytes, key_len));
            }
            admit_element(k, v, false /* modified */);
         }
         return true;
      }

      return false;
   }

   void insert(Key k, Payload& v) {
      put(k, v);
      //admit_element(k, v, true/* dirty */, true/* insert */);
   }

   void update(Key k, Payload& v) {
      try_eviction();
      Payload t;
      // //read
      // auto status __attribute__((unused)) = lookup(k, t);
      // //modify
      // memcpy(&t, &v, sizeof(v));
      // //write
      // admit_element(k, t, true /* dirty */, false);

      rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto blocks_read_old = rocksdb::get_perf_context()->block_read_count;
      auto status = db->Get(options, top_handle, rocksdb::Slice((const char *)key_bytes, key_len), &value);
      assert(rocksdb::get_perf_context()->block_read_count - blocks_read_old >= 0);
      blocks_read += rocksdb::get_perf_context()->block_read_count - blocks_read_old;
      if (status == rocksdb::Status::OK()) {
         assert(sizeof(TaggedPayload) == value.size());
         // update top tree
         blocks_read_old = rocksdb::get_perf_context()->block_read_count;
         admit_element(k, v, /* modifed */ true);
         blocks_read += rocksdb::get_perf_context()->block_read_count - blocks_read_old;
         return;
      }

      value.clear();
      status = db->Get(options, bottom_handle, rocksdb::Slice((const char *)key_bytes, key_len), &value);

      if (status == rocksdb::Status::OK()) {
         if (should_migrate()) {
            upward_migrations++;
            assert(sizeof(Payload) == value.size());
            if (inclusive == false) {
               rocksdb::WriteOptions write_options;
               write_options.disableWAL = true;
               status = db->Delete(write_options, bottom_handle, rocksdb::Slice((const char *)key_bytes, key_len));
               assert(status == rocksdb::Status::OK());
            }
            admit_element(k, v, /* modified */ true);
         } else {
            put_bottom_db(k, v);
         }
      }

   }

   void put_top_db(Key k, Payload& v) {
      admit_element(k, v, true);
   }

   void report_db(rocksdb::DB * db, const string & db_tag, rocksdb::ColumnFamilyHandle * handle) {
      std::string val;
      auto res __attribute__((unused)) = db->GetProperty(handle, "rocksdb.block-cache-usage", &val);
      assert(res);
      std::cout << db_tag << " RocksDB block-cache-usage " << val << std::endl;
      res = db->GetProperty(handle, "rocksdb.estimate-num-keys", &val);
      assert(res);
      std::cout << db_tag << " RocksDB est-num-keys " <<val << std::endl;

      res = db->GetProperty(handle, "rocksdb.stats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB stats " <<val << std::endl;

      res = db->GetProperty(handle, "rocksdb.dbstats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB dbstats " <<val << std::endl;
      res = db->GetProperty(handle, "rocksdb.levelstats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB levelstats\n" <<val << std::endl;
      res = db->GetProperty(handle, "rocksdb.block-cache-entry-stats", &val);
      assert(res);
      std::cout << db_tag << " RocksDB block-cache-entry-stats " <<val << std::endl;
   }
   void report(u64, u64) override {
      report_db(db, "top", top_handle);
      report_db(db, "bottom", bottom_handle);
      std::cout << "perf stat " << rocksdb::get_perf_context()->ToString() << std::endl;
      std::cout << "io stat " << rocksdb::get_iostats_context()->ToString() << std::endl;
      std::cout << total_lookups<< " lookups, "  << lookup_hit_top << " lookup hit top " << " topdb blocks read " << blocks_read << std::endl;
      std::cout << upward_migrations << " upward_migrations, "  << downward_migrations << " downward_migrations" << std::endl;
   }
};

}