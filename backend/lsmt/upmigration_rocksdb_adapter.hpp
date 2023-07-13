#pragma once
// -------------------------------------------------------------------------------------
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>

// -------------------------------------------------------------------------------------
#include "interface/StorageInterface.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "LockManager/LockManager.hpp"
#include "leanstore/utils/DistributedCounter.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
template <typename Key, typename Payload>
struct UpMigrationRocksDBAdapter : public leanstore::StorageInterface<Key, Payload> {
   leanstore::OptimisticLockTable lock_table;
   rocksdb::DB* db;
   rocksdb::Options options;
   bool lazy_migration;
   bool wal;
   u64 lazy_migration_threshold = 1;
   DistributedCounter<> up_migrations = 0;
   DistributedCounter<> hot_record_up_migrations = 0;
   DistributedCounter<> hot_up_migrations = 200;
   DistributedCounter<> total_lookups = 0;
   DistributedCounter<> lookups = 0;
   DistributedCounter<> level_accessed = 0;
   UpMigrationRocksDBAdapter(const std::string & db_dir, bool populate_block_cache_for_compaction_and_flush, double block_cache_memory_budget_gib, bool wal = false, int lazy_migration_sampling_rate = 100): lazy_migration(lazy_migration_sampling_rate < 100), wal(wal) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      options.write_buffer_size = 64 * 1024 * 1024;
      std::size_t write_buffer_count = options.max_write_buffer_number;
      //std::size_t write_buffer_count = 1;
      std::size_t block_cache_size = 0;
      std::cout << "RocksDB with upward migration " << lazy_migration_threshold << std::endl;
      std::cout << "RocksDB block cache budget " << (block_cache_memory_budget_gib) << "gib" << std::endl;
      block_cache_size = block_cache_memory_budget_gib * 1024ULL * 1024ULL * 1024ULL - options.write_buffer_size * write_buffer_count;

      std::cout << "RocksDB write_buffer_size " << options.write_buffer_size << std::endl;
      std::cout << "RocksDB max_write_buffer_number " << write_buffer_count << std::endl;
      
      std::cout << "RocksDB block cache size " << block_cache_size / 1024.0 /1024.0/1024 << " gib" << std::endl;
      rocksdb::DestroyDB(db_dir,options);
      options.manual_wal_flush = true;
      options.use_direct_reads = true;
      options.create_if_missing = true;
      options.stats_dump_period_sec = 3000;
      options.compression = rocksdb::kNoCompression;
      options.use_direct_io_for_flush_and_compaction = true;
      options.level_compaction_dynamic_level_bytes = true;
      rocksdb::BlockBasedTableOptions table_options;
      table_options.block_size = 16 * 1024;
      table_options.cache_index_and_filter_blocks = true;
      if (populate_block_cache_for_compaction_and_flush) {
         table_options.prepopulate_block_cache = rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushAndCompaction;
         std::cout << "RocksDB prepopulate block cache for both memtable flush and compaction" << std::endl;
      } else {
         table_options.prepopulate_block_cache = rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;
         std::cout << "RocksDB prepopulate block cache for memtable flush only" << std::endl;
      }
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
      table_options.block_cache = rocksdb::NewLRUCache(block_cache_size, 5, true, 0.9);
      options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
      options.statistics = rocksdb::CreateDBStatistics();
      rocksdb::Status status =
         rocksdb::DB::Open(options, db_dir, &db);
      assert(status.ok());
      rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
   }
    
   void evict_all() {
      rocksdb::FlushOptions fopts;
      db->Flush(fopts);
   }

   void clear_stats() {
      // rocksdb::FlushOptions fopts;
      // db->Flush(fopts);
      db->ResetStats();
      rocksdb::get_iostats_context()->Reset();
      rocksdb::get_perf_context()->Reset();
      total_lookups = 0;
      up_migrations = 0;
      hot_record_up_migrations = 0;
   }

   bool should_migrate() {
      if (lazy_migration) {
         return leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold;
      }
      return true;
   }

   void scan(Key start_key, std::function<bool(const Key&, const Payload &)> processor, [[maybe_unused]] int length) {
      rocksdb::ReadOptions ropts;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, start_key);
      rocksdb::Iterator * it = db->NewIterator(ropts);
      it->Seek(rocksdb::Slice((const char *)key_bytes, key_len));
      Key k;
      Payload p;
      while (it->Valid()) {
         assert(it->key().size() == sizeof(Key));
         k = leanstore::unfold(*(Key*)it->key().data());
         memcpy(&p, it->value().data(), it->value().size());
         if (processor(k, p)) {
            break;
         }
         it->Next();
      }
      delete it;
   }

   bool lookup_internal(Key k, Payload& v, leanstore::LockGuardProxy & g, bool from_update = false) {
      ++total_lookups;
      ++lookups;
      rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto old_block_read_count = rocksdb::get_perf_context()->block_read_count;
      //auto old_last_level_block_read_count = rocksdb::get_perf_context()->last_level_block_read_count;
      auto status = db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);
      assert(status == rocksdb::Status::OK());
      assert(sizeof(v) == value.size());
      memcpy(&v, value.data(), value.size());
      assert(status.GetLastLevel() != -2);
      auto level = status.GetLastLevel() + 1; // which level satisfied this Get operation 
      level_accessed += level;

      if (status == rocksdb::Status::OK()) {
         bool io = rocksdb::get_perf_context()->block_read_count - old_block_read_count > 0;
         if (from_update == false) {
            if (io) {
               if (should_migrate()) {
                  if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
                     return false;
                  }
                  up_migrations++;
                  put(k, v);
               }
            } else {
               // no io, make sure we place the hottest data near the top of the LSM hiearchy
               // if record is retrieved from memtables (level == -1), skip migration
               if (level != -1 && leanstore::utils::RandomGenerator::getRandU64(0, hot_up_migrations) < 1) {
                  if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
                     return false;
                  }
                  hot_record_up_migrations++;
                  put(k, v);
               }
            }
         }
         return true;
      }
      return false;
   }

   bool lookup(Key k, Payload& v) {
      while (true) {
         leanstore::LockGuardProxy g(&lock_table, k);
         if (!g.read_lock()) {
            continue;
         }
         bool res = lookup_internal(k, v, g);
         if (g.validate()) {
            return res;
         }
      }
   }

   void insert(Key k, Payload& v) {
      put(k, v);
   }

   void update(Key k, Payload& v) {
      Payload t;

      while (true) {
         leanstore::LockGuardProxy g(&lock_table, k);
         if (!g.read_lock()) {
            continue;
         }
         [[maybe_unused]] bool res = lookup_internal(k, t, g, true);
         if (g.validate()) {
            break;
         }
      }
      leanstore::LockGuardProxy g(&lock_table, k);
      
      while (g.write_lock() == false);

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
      auto res __attribute__((unused)) = db->GetProperty("rocksdb.block-cache-usage", &val);
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
      res = db->GetProperty("rocksdb.dbstats", &val);
      assert(res);
      std::cout << "RocksDB dbstats " <<val << std::endl;
      res = db->GetProperty("rocksdb.levelstats", &val);
      assert(res);
      std::cout << "RocksDB levelstats\n" <<val << std::endl;
      res = db->GetProperty("rocksdb.block-cache-entry-stats", &val);
      assert(res);
      std::cout << "RocksDB block-cache-entry-stats " <<val << std::endl;
      std::cout << total_lookups<< " lookups" << std::endl;
      std::cout << "perf stat " << rocksdb::get_perf_context()->ToString() << std::endl;
      std::cout << "io stat " << rocksdb::get_iostats_context()->ToString() << std::endl;
      std::cout << "cold up_migrations " << up_migrations << std::endl;
      std::cout << "hot up_migrations " << hot_record_up_migrations << std::endl;
      std::cout << "Stats "<< options.statistics->ToString() << std::endl;
   }

   std::vector<std::string> stats_column_names() override { 
      return {"hit_rate", "avg_lvl"}; 
   }

   std::vector<std::string> stats_columns() override { 
      auto hits = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT);
      auto misses = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS);
      auto levels = level_accessed.get();
      auto gets = lookups.get();
      level_accessed.store(0);
      lookups.store(0);
      return {std::to_string(hits / (0.0 + hits + misses)), std::to_string(levels / (gets + 0.0))};
   }
};