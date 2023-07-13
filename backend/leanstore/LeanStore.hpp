#pragma once
#include "Config.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/hashing/LinearHashing.hpp"
#include "storage/hashing/LinearHashingWithOverflowHeap.hpp"
#include "storage/heap/HeapFile.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "rapidjson/document.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
class LeanStore
{
   using statCallback = std::function<void(ostream&)>;
   struct GlobalStats {
      u64 accumulated_tx_counter = 0;
   };
   // -------------------------------------------------------------------------------------
  public:
   // Poor man catalog
   std::unordered_map<string, storage::btree::BTreeLL> btrees_ll;
   std::unordered_map<string, storage::hashing::LinearHashTable> hts;
   std::unordered_map<string, storage::hashing::LinearHashTableWithOverflowHeap> htwohs;
   std::unordered_map<string, storage::heap::HeapFile> hfs;
   // -------------------------------------------------------------------------------------
   s32 ssd_fd;
   // -------------------------------------------------------------------------------------
   unique_ptr<cr::CRManager> cr_manager;
   unique_ptr<storage::BufferManager> buffer_manager;
   // -------------------------------------------------------------------------------------
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   profiling::ConfigsTable configs_table;
   u64 config_hash = 0;
   GlobalStats global_stats;
   // -------------------------------------------------------------------------------------
  public:
   LeanStore();
   // -------------------------------------------------------------------------------------
   template <typename T>
   void registerConfigEntry(string name, T value)
   {
      configs_table.add(name, std::to_string(value));
   }
   u64 getConfigHash();
   GlobalStats getGlobalStats();
   // -------------------------------------------------------------------------------------
   storage::btree::BTreeLL& registerBTreeLL(string name, bool keep_in_memory = false);
   storage::btree::BTreeLL& retrieveBTreeLL(string name, [[maybe_unused]] bool keep_in_memory = false) { return btrees_ll[name]; }
   // -------------------------------------------------------------------------------------
   storage::hashing::LinearHashTable& registerHashTable(string name, bool keep_in_memory = false);
   storage::hashing::LinearHashTable& retrieveHashTable(string name, bool keep_in_memory = false) { return hts[name]; }
   // -------------------------------------------------------------------------------------
   storage::hashing::LinearHashTableWithOverflowHeap& registerHashTableWOH(string name, storage::heap::HeapFile&, bool keep_in_memory = false);
   storage::hashing::LinearHashTableWithOverflowHeap& retrieveHashTableWOH(string name, bool keep_in_memory = false) { return htwohs[name]; }
   // -------------------------------------------------------------------------------------
   storage::heap::HeapFile& registerHeapFile(string name, bool keep_in_memory = false);
   storage::heap::HeapFile& retrieveHeapFile(string name, bool keep_in_memory = false) { return hfs[name]; }
   // -------------------------------------------------------------------------------------
   storage::BufferManager& getBufferManager() { return *buffer_manager; }
   cr::CRManager& getCRManager() { return *cr_manager; }
   // -------------------------------------------------------------------------------------
   void startProfilingThread(const std::vector<std::string> & additional_columns = {}, std::function<std::vector<string>()> func = [](){ return std::vector<std::string>{}; });
   void persist();
   void restore();
   // -------------------------------------------------------------------------------------
   ~LeanStore();
  // State
  private:
   static std::list<std::tuple<string, fLS::clstring*>> persisted_string_flags;
   static std::list<std::tuple<string, s64*>> persisted_s64_flags;
   void serializeFlags(rapidjson::Document& d);
   void deserializeFlags();
   void serializeState();
   void deserializeState();
public:
     static void addStringFlag(string name, fLS::clstring* flag) { persisted_string_flags.push_back(std::make_tuple(name, flag)); }
   static void addS64Flag(string name, s64* flag) { persisted_s64_flags.push_back(std::make_tuple(name, flag)); }

};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
