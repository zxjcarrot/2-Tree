#pragma once
// -------------------------------------------------------------------------------------
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>

// -------------------------------------------------------------------------------------
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
template <typename Key, typename Payload>
struct RocksDBAdapter : leanstore::BTreeInterface<Key, Payload> {
   rocksdb::DB* db;
   rocksdb::Options options;
   bool lazy_migration;
   bool wal;
   u64 lazy_migration_threshold = 1;
   RocksDBAdapter(const std::string & db_dir, double row_cache_memory_budget_gib, double block_cache_memory_budget_gib, bool wal = false, bool lazy_migration = false): lazy_migration(lazy_migration), wal(wal) {
      std::cout << "RocksDB cache budget " << (block_cache_memory_budget_gib + row_cache_memory_budget_gib) << "gib" << std::endl;
      std::cout << "RocksDB write_buffer_size " << options.write_buffer_size << std::endl;
      std::cout << "RocksDB max_write_buffer_number " << options.max_write_buffer_number << std::endl;
      rocksdb::DestroyDB(db_dir,options);
      options.manual_wal_flush = false;
      options.use_direct_reads = true;
      options.create_if_missing = true;
      options.stats_dump_period_sec = 3000;
      options.compression = rocksdb::kNoCompression;
      options.use_direct_io_for_flush_and_compaction = true;
      rocksdb::BlockBasedTableOptions table_options;
      table_options.block_cache = rocksdb::NewLRUCache((row_cache_memory_budget_gib + block_cache_memory_budget_gib) * 1024ULL * 1024ULL * 1024ULL - options.write_buffer_size * options.max_write_buffer_number, 0, false, 0);
      options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
      rocksdb::Status status =
         rocksdb::DB::Open(options, db_dir, &db);
      assert(status.ok());
   }
    
   void clear_stats() {
      db->ResetStats();
   }

   bool should_migrate() {
      if (lazy_migration) {
         return leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }
   bool lookup(Key k, Payload& v) {
      rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto status = db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);
      assert(status == rocksdb::Status::OK());
      assert(sizeof(v) == value.size());
      memcpy(&v, value.data(), value.size());
      if (status == rocksdb::Status::OK()) {
         if (should_migrate()) {
            put(k, v);
         }
         return true;
      }
      return false;
   }

   void insert(Key k, Payload& v) {
      put(k, v);
   }

   void update(Key k, Payload& v) {
      Payload t;
      //read
      auto status = lookup(k, t);
      //modify
      memcpy(&t, &v, sizeof(v));
      //write
      put(k, t);
   }

   void put(Key k, Payload& v) {
      rocksdb::WriteOptions options;
      options.disableWAL = true;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      auto status = db->Put(options, rocksdb::Slice((const char *)key_bytes, key_len), rocksdb::Slice((const char *)&v, sizeof(v)));
      assert(status == rocksdb::Status::OK());
   }

   void report(u64, u64) override {
      std::string val;
      auto res = db->GetProperty("rocksdb.block-cache-usage", &val);
      assert(res);
      std::cout << "RocksDB block-cache-usage " << val << std::endl;
      res = db->GetProperty("rocksdb.estimate-num-keys", &val);
      assert(res);
      std::cout << "RocksDB est-num-keys " <<val << std::endl;

      res = db->GetProperty("rocksdb.stats", &val);
      assert(res);
      std::cout << "RocksDB stats " <<val << std::endl;

      // res = db->GetProperty("rocksdb.cfstats", &val);
      // assert(res);
      // std::cout << "RocksDB cfstats " <<val << std::endl;
      // res = db->GetProperty("rocksdb.cfstats-no-file-histogram", &val);
      // assert(res);
      // std::cout << "RocksDB cfstats-no-file-histogram " <<val << std::endl;
      // res = db->GetProperty("rocksdb.dbstats", &val);
      // assert(res);
      // std::cout << "RocksDB dbstats " <<val << std::endl;
      res = db->GetProperty("rocksdb.levelstats", &val);
      assert(res);
      std::cout << "RocksDB levelstats " <<val << std::endl;
      res = db->GetProperty("rocksdb.block-cache-entry-stats", &val);
      assert(res);
      std::cout << "RocksDB block-cache-entry-stats " <<val << std::endl;
      
   }
};