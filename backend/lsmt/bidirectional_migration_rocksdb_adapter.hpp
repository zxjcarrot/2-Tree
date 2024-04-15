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

[[maybe_unused]] static u64 unfold(const u64 &x) {
   return __builtin_bswap64(x);
}

[[maybe_unused]] static u32 unfold(const u32 &x) {
   return __builtin_bswap32(x);
}

template <typename Key, typename Payload>
struct BidirectionalMigrationRocksDBAdapter : public leanstore::StorageInterface<Key, Payload> {
   leanstore::OptimisticLockTable lock_table;
   rocksdb::DB* db;
   rocksdb::Options options;
   bool lazy_migration;
   bool constant_migration_prob = false;
   bool migration_enabled = true;
   bool wal;
   u64 lazy_migration_threshold = 1;
   DistributedCounter<> up_migrations = 0;
   DistributedCounter<> hot_record_up_migrations = 0;
   DistributedCounter<> in_memory_migration_prob = 50;
   DistributedCounter<> migrations = 0;
   DistributedCounter<> total_lookups = 0;
   DistributedCounter<> lookups = 0;
   DistributedCounter<> lookups_with_io = 0;
   DistributedCounter<> accumulated_depth = 0;
   uint64_t migrations_threshold_before_adaptive_control = 30000000;
   bool adaptive_migration = false;
   bool in_memory_migration = true;
   bool bulk_migration_at_flush = false;
   int max_depth_seen = -1;
   std::mutex lock_table_snapshots_mtx;
   std::deque<leanstore::OptimisticLockTable*> lock_table_snapshots;
   IOStats stats;
   std::function<void (const rocksdb::Slice & key, rocksdb::Slice & value)> on_memtable_hit = nullptr;

   struct alignas(1)  TaggedPayload {
      Payload payload;
      bool ref = false;
      bool migrated = false;
   };

   struct FlushContext {
      leanstore::OptimisticLockTable* table = nullptr;
      size_t bytes_migrated = 0;
      size_t bytes_examined = 0;
      size_t write_lock_failed = 0;
      size_t memtable_throttled = 0;
      FlushContext() {}
      ~FlushContext() {
         delete table;
      }
   };
   static constexpr size_t kRocksDBWriteBufferSize = 256 * 1024 * 1024;

   BidirectionalMigrationRocksDBAdapter(const std::string & db_dir, bool bulk_migration_at_flush, bool adaptive_migration, bool in_memory_migration,  bool populate_block_cache_for_compaction_and_flush, double block_cache_memory_budget_gib, bool constant_migration_prob = false, bool wal = false, int lazy_migration_sampling_rate = 100): lazy_migration(lazy_migration_sampling_rate < 100), constant_migration_prob(constant_migration_prob), wal(wal), loss_value_window(loss_value_window_limit), lr_slope_window(lr_slope_limit), bulk_migration_at_flush(bulk_migration_at_flush), adaptive_migration(adaptive_migration), moving_average_loss_value_window(loss_value_window_limit), in_memory_migration(in_memory_migration) {
      if (lazy_migration_sampling_rate <= 100) {
         lazy_migration_threshold = lazy_migration_sampling_rate;
      }
      options.write_buffer_size = kRocksDBWriteBufferSize;
      std::size_t write_buffer_count = options.max_write_buffer_number;
      //std::size_t write_buffer_count = 1;
      std::size_t block_cache_size = 0;
      std::cout << "Constant migration probability " << constant_migration_prob << std::endl;
      std::cout << "Bulk migration at flush " << bulk_migration_at_flush << std::endl;
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
      options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleUniversal;
      rocksdb::BlockBasedTableOptions table_options;
      table_options.block_size = 16 * 1024;
      table_options.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      table_options.partition_filters = true;
      table_options.metadata_block_size = 4096;
      table_options.cache_index_and_filter_blocks = true;
      table_options.pin_top_level_index_and_filter = true;
      table_options.cache_index_and_filter_blocks_with_high_priority = true;
      if (populate_block_cache_for_compaction_and_flush) {
         table_options.prepopulate_block_cache = rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushAndCompaction;
         std::cout << "RocksDB prepopulate block cache for both memtable flush and compaction" << std::endl;
      } else {
         table_options.prepopulate_block_cache = rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;
         std::cout << "RocksDB prepopulate block cache for memtable flush only" << std::endl;
      }
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
      table_options.block_cache = rocksdb::NewLRUCache(block_cache_size, 5, true);
      options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
      options.statistics = rocksdb::CreateDBStatistics();

      if (bulk_migration_at_flush) {
         options.memtable_switch = [&, this](const rocksdb::MemTableInfo & info) {
            auto lock_table_snapshot = lock_table.Copy();
            lock_table_snapshots_mtx.lock();
            // At each memtable switch, we take a snapshot of the lock table.
            // So lock_table_snapshots stores snapshots ordered by the memtable id (time order) incresingly
            lock_table_snapshots.push_back(lock_table_snapshot);
            lock_table_snapshots_mtx.unlock();
         };

         options.memtable_flush_start = [&, this](const int num_memtables) -> void* {
            auto ctx = new FlushContext();
            lock_table_snapshots_mtx.lock();
            assert(lock_table_snapshots.size() >= num_memtables);
            int cnt = num_memtables - 1;
            while (cnt-- > 0) {
               delete lock_table_snapshots.front();
               lock_table_snapshots.pop_front();
            }
            assert(lock_table_snapshots.empty() == false);

            ctx->table = lock_table_snapshots.front(); // take `num_memtables` lock table snapshot as it relects version changes made by all `num_memtables` memtables
            lock_table_snapshots.pop_front();
            lock_table_snapshots_mtx.unlock();

            std::cout << "flushing " << num_memtables << " memtables..." << std::endl;
            return (void*)ctx;
         };

         options.memtable_flush_on_each_key_value = [&, this](const rocksdb::Slice & key, const rocksdb::Slice & value, void * context) -> bool {
            assert(key.size() == sizeof(Key));
            assert(value.size() == sizeof(TaggedPayload));
            FlushContext* ctx = reinterpret_cast<FlushContext*>(context);
            auto tagged_value = reinterpret_cast<TaggedPayload*>(const_cast<char*>(value.data()));
            ctx->bytes_examined += key.size() + 8 + value.size();
            if (tagged_value->migrated == false || tagged_value->ref == false || ctx->bytes_migrated >= kRocksDBWriteBufferSize / 2) {
               return true;
            }
            auto k = unfold(*(Key*)(key.data()));
            
            auto snapshot_version = ctx->table->get_key_version(k);
            if (snapshot_version & 1) { // write-locked
               ctx->write_lock_failed++;
               return true;
            }
            leanstore::LockGuardProxy g(&lock_table, k);
            g.set_snapshot_read_version(snapshot_version);
            if (g.upgrade_to_write_lock()) {
               tagged_value->ref = false;
               tagged_value->migrated = true;

               rocksdb::WriteOptions options;
               options.disableWAL = true;
               options.no_slowdown = true;
               auto status = db->Put(options, key, value);
               ctx->table->update_key_version(k, g.version_if_write_unlocked());
               if (status.IsIncomplete()) {
                  ctx->memtable_throttled++;
                  return true; // fail this migration and move it down the hiearchy
               } else {
                  ctx->bytes_migrated += key.size() + 8 + value.size();
                  return false; // migration succeeded, so don't let the record go down the LSM-hiearchy.
               }
            } else { // failed to obtain the write lock due to conflicts (could be false positive), give up and move the record down the hiearchy
               ctx->write_lock_failed++;
               return true;
            }
         };

         options.memtable_flush_end = [&, this](void * context) {
            FlushContext* ctx = reinterpret_cast<FlushContext*>(context);
            std::cout << ctx->bytes_examined << " examined, " << ctx->bytes_migrated << " bytes migrated upwards, " << ctx->write_lock_failed << " write lock fails, " << ctx->memtable_throttled << " memtable throttles." << std::endl;
            delete ctx;
         };


         on_memtable_hit = [&, this] (const rocksdb::Slice & key, rocksdb::Slice & value) {
            auto tagged_value = reinterpret_cast<TaggedPayload*>(const_cast<char*>(value.data()));
            if (tagged_value->migrated == false || tagged_value->ref == true) {
               return;
            }
            tagged_value->ref = true;
         };

      }
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
      migrations = 0;
   }

   bool should_migrate(int depth, int max_depth, int base) {
      if (migration_enabled == false) {
         return false;
      }
      if (lazy_migration) {
         if (constant_migration_prob) {
            return leanstore::utils::RandomGenerator::getRandU64(0, 100) <= lazy_migration_threshold;
         } else {
            return leanstore::utils::RandomGenerator::getRandU64(0, 100) < MigrationThreshold(depth, max_depth, base);
         }
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

   int MigrationThreshold(int depth, int max_depth, int base) {
      constexpr double R = 1.5;
      return std::min(lazy_migration_threshold, (uint64_t) (base * std::pow(R, depth)));
   }

   bool lookup_internal(Key k, Payload& v, leanstore::LockGuardProxy & g, bool from_update = false) {
      ++total_lookups;
      ++lookups;
      DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
         auto new_reads = rocksdb::get_perf_context()->block_read_count;
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
            lookups_with_io++;
         }
      }, rocksdb::get_perf_context()->block_read_count);
      rocksdb::ReadOptions options;
      options.on_memtable_hit = on_memtable_hit;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto old_block_read_count = rocksdb::get_perf_context()->block_read_count;
      //auto old_last_level_block_read_count = rocksdb::get_perf_context()->last_level_block_read_count;
      auto status = db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);
      assert(status == rocksdb::Status::OK());
      assert(sizeof(TaggedPayload) == value.size());
      assert(status.GetLastLevel() != -2);
      auto depth = status.GetLastLevel() + 1; // which level satisfied this Get operation 
      accumulated_depth += depth;
      if (depth > max_depth_seen) {
         max_depth_seen = depth;
      }

      if (status == rocksdb::Status::OK()) {
         auto tagged_payload = reinterpret_cast<const TaggedPayload*>(value.data());
         v = tagged_payload->payload;
         bool io = rocksdb::get_perf_context()->block_read_count - old_block_read_count > 0;
         if (from_update == false) {
            // if (migration_enabled && leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold) {
            // //if (migration_enabled && leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold) {
            //    if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
            //       return false;
            //    }
            //    up_migrations++;
            //    doPut(k, v, true);
            //    ++migrations;
            // }
            if (io) {
               //if (should_migrate(depth, max_depth_seen)) {
               if (migration_enabled && should_migrate(depth, max_depth_seen, 10)) {
               //if (migration_enabled && leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold) {
               //if (migration_enabled && leanstore::utils::RandomGenerator::getRandU64(0, 100) < lazy_migration_threshold) {
                  if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
                     return false;
                  }
                  up_migrations++;
                  doPut(k, v, true);
                  ++migrations;
               }
            } else {
               // no io, make sure we place the hottest data near the top of the LSM hiearchy
               // if record is retrieved from memtables (level == -1), skip migration
               // if (in_memory_migration && level != 0 && leanstore::utils::RandomGenerator::getRandU64(0, in_memory_migration_prob) < 1) {
               //    if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
               //       return false;
               //    }
               //    hot_record_up_migrations++;
               //    doPut(k, v, true);
               // }
               //if (migration_enabled && in_memory_migration && leanstore::utils::RandomGenerator::getRandU64(0, 100) < MigrationThreshold(depth, max_depth_seen)) {
               //if (in_memory_migration && migration_enabled && leanstore::utils::RandomGenerator::getRandU64(0, 100) <= depth){//should_migrate(depth, max_depth_seen, 1)) {
               if (in_memory_migration && migration_enabled && should_migrate(depth, max_depth_seen, 1)){//) {
                  if (g.upgrade_to_write_lock() == false) { // obtain a write lock for migration
                      return false;
                  }
                  hot_record_up_migrations++;
                  doPut(k, v, true);
                  ++migrations;
               }
            }
            //if (leanstore::utils::RandomGenerator::getRandU64(0, ) < MigrationThreshold(level, ))
         }
         return true;
      }
      return false;
   }

   void lookup_with_sst_count(Key k, Payload& v, int & sst_count) {
      rocksdb::ReadOptions options;
      options.on_memtable_hit = on_memtable_hit;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto status = db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);
      assert(status == rocksdb::Status::OK());
      assert(sizeof(TaggedPayload) == value.size());
      assert(status.GetLastLevel() != -2);
      sst_count = status.GetLastLevel() + 1; // number of SST tables read to complete this read.
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
      doPut(k, t, false);
   }

   void doPut(Key k, Payload & v, bool migrated = false) {
      rocksdb::WriteOptions options;
      options.disableWAL = true;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      TaggedPayload payload;
      payload.payload = v;
      payload.ref = false;
      payload.migrated = migrated;
      auto status = db->Put(options, rocksdb::Slice((const char *)key_bytes, key_len), rocksdb::Slice((const char *)&payload, sizeof(payload)));
      assert(status == rocksdb::Status::OK());
   }

   void put(Key k, Payload& v) {
      doPut(k ,v, false);
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

      void Clear() {
         sum = 0;
         window.clear();
      }
      T sum = 0;
      size_t w = 0;
      std::deque<T> window;
      
   };
   const size_t loss_value_window_limit = 100;
   double moving_average_loss_value = 0;
   MovingWindow<double> loss_value_window;
   int clock = 0;
   double avg_loss_value_when_migration_disabled = 0;
   
   int lr_internal = 15; // interval in seconds to run linear regression on the current window
   int lr_countdown = lr_internal;
   const size_t lr_slope_limit = 1;
   MovingWindow<double> moving_average_loss_value_window;
   MovingWindow<double> lr_slope_window;
   double last_trendline_slope = 0;
   double last_trendline_intercept = 0;

   std::tuple<double, double> linear_regression(const std::deque<double> & data) {
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
      return std::tuple<double, double>(lr.b, lr.a);
   }

   void process_loss_value_measure(double loss_value) {
      ++clock;
      if (clock >= 300) {
         max_depth_seen = -1;
         clock = 0;
      }
      if (std::isnan(loss_value))
         return;
      loss_value_window.Add(loss_value);
      moving_average_loss_value = loss_value_window.Average();
      moving_average_loss_value_window.Add(moving_average_loss_value);
      if (--lr_countdown == 0) {
         lr_countdown = lr_internal;
         if (moving_average_loss_value_window.window.size() < moving_average_loss_value_window.w)
            return;
         // do a regression on the current hit rate window to obtain the slope of the trendline
         auto lr_coeffs = linear_regression(moving_average_loss_value_window.window);
         last_trendline_slope = std::get<0>(lr_coeffs);
         last_trendline_intercept = std::get<1>(lr_coeffs);
         lr_slope_window.Add(last_trendline_slope);
         if (lr_slope_window.window.size() == lr_slope_window.w && adaptive_migration) {
            bool should_disable_migration = true;
            bool should_enable_migration = false;
            double disable_migration_threshold = 0.01;
            double reenable_migration_threshold = 0.1;
            int above_threshold = 0;
            for (size_t i = 0; i < lr_slope_window.window.size(); ++i) {
               if (std::abs(lr_slope_window.window[i]) >= disable_migration_threshold) {
                  should_disable_migration = false;
               }

               if (std::abs(lr_slope_window.window[i]) >= reenable_migration_threshold) {
                  above_threshold++;
               }
            }

            if (should_disable_migration && migrations > migrations_threshold_before_adaptive_control) {
               if (migration_enabled == true) {
                  migration_enabled = false;
                  std::cout << std::endl <<
                         migrations.get() << " migrations so far. Observed slope below " 
                         << disable_migration_threshold << ", disabling migration" << std::endl;
                  avg_loss_value_when_migration_disabled = moving_average_loss_value;
                  migrations = 0;
               }
            }

            if (above_threshold >= 1 || moving_average_loss_value >= avg_loss_value_when_migration_disabled / 0.9) {
               if (migration_enabled == false) {
                  migration_enabled = true;
                  std::cout << std::endl << "Observed a slope above " 
                            << reenable_migration_threshold <<  " or moving average falls below avg_loss_value_when_migration_disabled / 0.8 " 
                            << (avg_loss_value_when_migration_disabled / 0.8) << ", reenabling migration" << std::endl;
                  //moving_average_loss_value_window.Clear();
                  lr_slope_window.Clear();
               }
            }
         }
      }
   }


   std::vector<std::string> stats_column_names() override { 
      return {"hit_rate", "avg_lvl", "mv_loss", "slope", "intc", "io_r", "io_w_bw", "io migrations", "mem migrations"}; 
   }

   std::vector<std::string> stats_columns() override { 
      auto hits = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT);
      auto misses = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS);
      //hits += options.statistics->getAndResetTickerCount(rocksdb::Tickers::MEMTABLE_HIT);
      //misses += options.statistics->getAndResetTickerCount(rocksdb::Tickers::MEMTABLE_MISS);
      std::string level_stats;
      bool res = db->GetProperty("rocksdb.levelstats", &level_stats);
      assert(res);
      std::cout << level_stats << std::endl;
      auto files = accumulated_depth.get();
      auto io_reads = stats.reads;
      auto gets = lookups.get();
      auto gets_with_io = lookups_with_io.get();
      lookups_with_io.store(0);
      accumulated_depth.store(0);
      lookups.store(0);
      //auto block_cache_hit_rate = hits / (0.0 + hits + misses);
      //double memory_hit_rate = 1 - io_reads / (double) files;
      double avg_ios_per_get = io_reads / (double) gets;
      double memory_hit_rate = 1 - gets_with_io / (double) gets;
      
      double loss_value = avg_ios_per_get + files / (gets + 0.0);
      if (avg_ios_per_get >= 0.05) { // for the case where working set does not fit in memory, the loss function f(t) = average # I/O per get over the last second since time t-1
         loss_value = avg_ios_per_get;
      } else { // For the case where working set fit in memory the loss function f(t) = (average depth of point read + average # I/O per get) over the last second since time t-1
         loss_value = avg_ios_per_get + files / (gets + 0.0);
      }

      //avg_ios_per_get + files / (gets + 0.0)
      process_loss_value_measure(loss_value);
      // string val;
      // bool res = db->GetProperty("rocksdb.levelstats", &val);
      // assert(res);
      auto reads = stats.reads;
      stats = IOStats();
      auto write_bytes = options.statistics->getAndResetTickerCount(rocksdb::Tickers::COMPACT_WRITE_BYTES) +
                         options.statistics->getAndResetTickerCount(rocksdb::Tickers::FLUSH_WRITE_BYTES);
      return {std::to_string(memory_hit_rate), 
              std::to_string(files / (gets + 0.0)), 
              std::to_string(moving_average_loss_value), 
              std::to_string(last_trendline_slope), 
              std::to_string(last_trendline_intercept), 
              std::to_string(reads), 
              std::to_string(write_bytes),
              std::to_string(up_migrations.get()),
              std::to_string(hot_record_up_migrations.get())};
   }
};