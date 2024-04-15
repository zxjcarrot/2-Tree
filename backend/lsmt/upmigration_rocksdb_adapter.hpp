#pragma once
// -------------------------------------------------------------------------------------
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>

// -------------------------------------------------------------------------------------
#include "LinearRegression.hpp"
#include "interface/StorageInterface.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "LockManager/LockManager.hpp"
#include "common/DistributedCounter.hpp"
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
   bool migration_enabled = true;
   bool wal;
   u64 lazy_migration_threshold = 1;
   DistributedCounter<> up_migrations = 0;
   DistributedCounter<> hot_record_up_migrations = 0;
   DistributedCounter<> in_memory_migration_prob = 50;
   DistributedCounter<> total_lookups = 0;
   DistributedCounter<> lookups = 0;
   DistributedCounter<> level_accessed = 0;
   bool adaptive_migration = false;
   bool in_memory_migration = true;
   UpMigrationRocksDBAdapter(const std::string & db_dir, bool adaptive_migration, bool in_memory_migration,  bool populate_block_cache_for_compaction_and_flush, double block_cache_memory_budget_gib, bool wal = false, int lazy_migration_sampling_rate = 100): lazy_migration(lazy_migration_sampling_rate < 100), wal(wal), hit_rate_window(hit_rate_window_limit), lr_slope_window(lr_slope_limit), adaptive_migration(adaptive_migration), moving_average_hit_rate_window(hit_rate_window_limit), in_memory_migration(in_memory_migration) {
      if (lazy_migration_sampling_rate < 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      options.write_buffer_size = 128 * 1024 * 1024;
      std::size_t write_buffer_count = options.max_write_buffer_number;
      //std::size_t write_buffer_count = 1;
      std::size_t block_cache_size = 0;
      std::cout << "Adaptive migration " << adaptive_migration << std::endl;
      std::cout << "In-memory migration " << in_memory_migration << std::endl;
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
      table_options.pin_l0_filter_and_index_blocks_in_cache = true;
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
      if (migration_enabled == false) {
         return false;
      }
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
   int MigrationThreshold(int level, double D, double T) {
      if (level == 0) return 0;
      else return D / std::pow(T, 4 - level);
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
            double T = 2;
            double D = 70;
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
               if (in_memory_migration && level != 0 && leanstore::utils::RandomGenerator::getRandU64(0, in_memory_migration_prob) < 1) {
                  if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
                     return false;
                  }
                  hot_record_up_migrations++;
                  put(k, v);
               }
               // if (in_memory_migration && leanstore::utils::RandomGenerator::getRandU64(0, 100) < MigrationThreshold(level, D, T)) {
               //    if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
               //        return false;
               //     }
               //     hot_record_up_migrations++;
               //     put(k, v);
               // }
            }
            //if (leanstore::utils::RandomGenerator::getRandU64(0, ) < MigrationThreshold(level, ))
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
      return {"hit_rate", "avg_lvl", "mv_hitrate", "slope"}; 
   }

   template<class T>
   class MovingWindow {
      public:
      MovingWindow(size_t w): w(w) {
         sum = 0;
      }

      void Add(const T & data_point) {
         sum += data_point;
         window.push_back(data_point);
         if (window.size() > w) {
            sum -= window.front();
            window.pop_front();
         }
      }
      
      T Average() {
         return sum / window.size();
      }

      T sum;
      size_t w;
      std::deque<T> window;
      
   };
   const size_t hit_rate_window_limit = 100;
   MovingWindow<double> hit_rate_window;
   double moving_average_hit_rate = 0;
   double avg_hit_rate_when_migration_disabled = 0;
   
   int lr_internal = 15; // interval in seconds to run linear regression on the current window
   int lr_countdown = lr_internal;
   const size_t lr_slope_limit = 5;
   MovingWindow<double> moving_average_hit_rate_window;
   MovingWindow<double> lr_slope_window;
   double last_trendline_slope = 0;

   double linear_regression(const std::deque<double> & data) {
      std::vector<double> x;
      std::vector<double> y;
      // scale timestamps down to [0, 1]
      for (int i = 0; i < data.size(); ++i) {
         x.push_back((i + 1) / (data.size() + 0.0f));
         y.push_back(data[i]);
      }

      regression lr(x, y);
      bool r = lr.coefficients();
      assert(r);
      return lr.b;
   }

   void process_hit_rate_measure(double hit_rate) {
      if (std::isnan(hit_rate))
         return;
      hit_rate_window.Add(hit_rate);
      moving_average_hit_rate = hit_rate_window.Average();
      moving_average_hit_rate_window.Add(moving_average_hit_rate);
      if (--lr_countdown == 0) {
         lr_countdown = lr_internal;
         if (moving_average_hit_rate_window.window.size() < moving_average_hit_rate_window.w)
            return;
         // do a regression on the current hit rate window to obtain the slope of the trendline
         auto lr_slope = linear_regression(moving_average_hit_rate_window.window);
         last_trendline_slope = lr_slope;
         lr_slope_window.Add(lr_slope);
         if (lr_slope_window.window.size() == lr_slope_window.w && adaptive_migration) {
            bool should_disable_migration = true;
            bool should_enable_migration = false;
            int above_threshold = 0;
            for (size_t i = 0; i < lr_slope_window.window.size(); ++i) {
               if (std::abs(lr_slope_window.window[i]) >= 0.01) {
                  should_disable_migration = false;
               }

               if (std::abs(lr_slope_window.window[i]) >= 0.03) {
                  above_threshold++;
               }
            }

            if (should_disable_migration) {
               if (migration_enabled == true) {
                  migration_enabled = false;
                  std::cout << "Observed " << lr_slope_window.window.size() << " slopes below 0.05, "<< " disabling migration" << std::endl;
                  avg_hit_rate_when_migration_disabled = moving_average_hit_rate;
               }
            }

            if (above_threshold >= 1 || moving_average_hit_rate < avg_hit_rate_when_migration_disabled * 0.95) {
               if (migration_enabled == false) {
                  migration_enabled = true;
                  std::cout << "Observed a hit rate slope above 0.1 twice or moving average falls below avg_hit_rate_when_migration_disabled " << avg_hit_rate_when_migration_disabled << ", enabling migration" << std::endl;
               }
            }
         }
      }
   }

   std::vector<std::string> stats_columns() override { 
      auto hits = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT);
      auto misses = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS);
      auto levels = level_accessed.get();
      auto gets = lookups.get();
      level_accessed.store(0);
      lookups.store(0);
      auto block_cache_hit_rate = hits / (0.0 + hits + misses);
      process_hit_rate_measure(block_cache_hit_rate);
      return {std::to_string(block_cache_hit_rate), std::to_string(levels / (gets + 0.0)), std::to_string(moving_average_hit_rate), std::to_string(last_trendline_slope)};
   }
};