#pragma once
// -------------------------------------------------------------------------------------
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>

// -------------------------------------------------------------------------------------
#include "interface/StorageInterface.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
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
struct RocksDBAdapter : public leanstore::StorageInterface<Key, Payload> {
   rocksdb::DB* db;
   rocksdb::Options options;
   bool wal;
   std::size_t total_lookups = 0;
   DistributedCounter<> lookups = 0;
   DistributedCounter<> level_accessed = 0;
   IOStats stats;
   RocksDBAdapter(bool use_row_cache, bool populate_block_cache_for_compaction_and_flush, const std::string & db_dir, double row_cache_memory_budget_gib, double block_cache_memory_budget_gib, bool wal = false): wal(wal) {
      options.write_buffer_size = 256 * 1024 * 1024;
      std::size_t block_cache_size = 0;
      std::size_t row_cache_size = 0;
      std::size_t write_buffer_count = options.max_write_buffer_number;
      if (use_row_cache) {
         std::cout << "RocksDB record cache budget " << (row_cache_memory_budget_gib) << "gib" << std::endl;   
         std::cout << "RocksDB block cache budget " << (block_cache_memory_budget_gib) << "gib" << std::endl;
         block_cache_size = block_cache_memory_budget_gib * 1024ULL * 1024ULL * 1024ULL;
         row_cache_size = row_cache_memory_budget_gib * 1024ULL * 1024ULL * 1024ULL - options.write_buffer_size * write_buffer_count;
      } else {
         std::cout << "RocksDB block cache budget " << (block_cache_memory_budget_gib + row_cache_memory_budget_gib) << "gib" << std::endl;
         block_cache_size = (row_cache_memory_budget_gib + block_cache_memory_budget_gib) * 1024ULL * 1024ULL * 1024ULL - options.write_buffer_size * write_buffer_count;
      }

      std::cout << "RocksDB write_buffer_size " << options.write_buffer_size << std::endl;
      std::cout << "RocksDB max_write_buffer_number " << write_buffer_count << std::endl;

      std::cout << "RocksDB row cache size " << row_cache_size / 1024.0 /1024.0/1024 << " gib" << std::endl;
      std::cout << "RocksDB block cache size " << block_cache_size / 1024.0 /1024.0/1024 << " gib" << std::endl;
      rocksdb::DestroyDB(db_dir,options);
      options.manual_wal_flush = true;
      options.use_direct_reads = true;
      options.create_if_missing = true;
      options.stats_dump_period_sec = 3000;
      options.compression = rocksdb::kNoCompression;
      options.use_direct_io_for_flush_and_compaction = true;
      options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleUniversal;
      if (use_row_cache) {
         options.row_cache = rocksdb::NewLRUCache(row_cache_size, 5, true);
      }
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
   }

   bool lookup(Key k, Payload& v) {
      ++total_lookups;
      ++lookups;
      DeferCodeWithContext ccc([&, this](uint64_t old_reads) {
         auto new_reads = rocksdb::get_perf_context()->block_read_count;
         if (new_reads > old_reads) {
            stats.reads += new_reads - old_reads;
         }
      }, rocksdb::get_perf_context()->block_read_count);
      rocksdb::ReadOptions options;
      u8 key_bytes[sizeof(Key)];
      auto key_len = leanstore::fold(key_bytes, k);
      std::string value;
      auto status = db->Get(options, rocksdb::Slice((const char *)key_bytes, key_len), &value);
      assert(status == rocksdb::Status::OK());
      assert(sizeof(v) == value.size());
      memcpy(&v, value.data(), value.size());
      assert(status.GetLastLevel() != -2);
      auto level = status.GetLastLevel() + 1; // which level satisfied this Get operation 
      level_accessed += level;
      if (status == rocksdb::Status::OK()) {
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
      auto status __attribute__((unused)) = lookup(k, t);
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
      std::cout << "Stats "<< options.statistics->ToString() << std::endl;
   }

   std::vector<std::string> stats_column_names() override { 
      return {"hit_rate", "avg_lvl", "io_r", "io_w_bw"}; 
   }

   std::vector<std::string> stats_columns() override { 
      auto hits = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT);
      auto misses = options.statistics->getAndResetTickerCount(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS);
      hits += options.statistics->getAndResetTickerCount(rocksdb::Tickers::ROW_CACHE_HIT);
      misses += options.statistics->getAndResetTickerCount(rocksdb::Tickers::ROW_CACHE_MISS);
      hits += options.statistics->getAndResetTickerCount(rocksdb::Tickers::MEMTABLE_HIT);
      misses += options.statistics->getAndResetTickerCount(rocksdb::Tickers::MEMTABLE_MISS);
      auto levels = level_accessed.get();
      auto gets = lookups.get();
      level_accessed.store(0);
      lookups.store(0);
      auto write_bytes = options.statistics->getAndResetTickerCount(rocksdb::Tickers::COMPACT_WRITE_BYTES) +
                         options.statistics->getAndResetTickerCount(rocksdb::Tickers::FLUSH_WRITE_BYTES);
      auto reads = stats.reads;
      stats = IOStats();
      return {std::to_string(hits / (0.0 + hits + misses)), 
              std::to_string(levels / (gets + 0.0)),
              std::to_string(reads),
              std::to_string(write_bytes)};
   }
};