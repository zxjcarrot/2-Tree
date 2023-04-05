#pragma once
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/FNVHash.hpp"
#include "leanstore/utils/XXHash.hpp"
#include "LockManager/LockManager.hpp"
#include <shared_mutex>

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace heap
{

enum class OP_RESULT : u8 { OK = 0, NOT_FOUND = 1, DUPLICATE = 2, ABORT_TX = 3, NOT_ENOUGH_SPACE = 4, OTHER = 5 };

class HeapPage;
using SwipType=Swip<HeapPage>;
// -------------------------------------------------------------------------------------

static constexpr int kDirNodeEntryCount = EFFECTIVE_PAGE_SIZE / (sizeof(u64) * 2) - 10;

struct HeapDirectoryNode {
   struct Pointer{
      SwipType ptr;
      u32 freeSpace;
      u32 reserved;
   };
   Pointer dataPagePointers[kDirNodeEntryCount];
   u16 node_count;
   u8 padding[EFFECTIVE_PAGE_SIZE - sizeof(dataPagePointers) - sizeof(node_count)];
   HeapDirectoryNode() {
      node_count = 0;
      memset(dataPagePointers, 0, sizeof(dataPagePointers));
   }
};

struct HeapMetaPage {
   u64 head;
   u64 tail;
   u8 padding[EFFECTIVE_PAGE_SIZE - sizeof(head) - sizeof(head)];

   HeapMetaPage(): head(0), tail(0) {}
};

// Map logical bucket ids to pages allocated from buffer manager
class MappingTable {
public:
   u64 numDirNodes() {
      return idx.load();
   }

   u64 numSlots() {
      return idx.load() * kDirNodeEntryCount;
   }

   BufferFrame* getDirNode(u32 page_id) {
      while (page_id < numSlots()) {
         if (dirNodes[page_id / kDirNodeEntryCount] == nullptr) {
            continue;
         }
         return dirNodes[page_id / kDirNodeEntryCount];
      }
      while (page_id >= numSlots()) {
         while (true) {
            jumpmuTry() 
            {
               auto write_guard_h = leanstore::storage::HybridPageGuard<HeapDirectoryNode>(dt_id, true, true);
               auto write_guard = ExclusivePageGuard<HeapDirectoryNode>(std::move(write_guard_h));
               write_guard.init();
               auto bf = write_guard.bf();
               bf->header.keep_in_memory = true;
               bf->page.hot_data = true;
               int j = idx.fetch_add(1);
               dirNodes[j] = (bf);
               jumpmu_break;
            }
            jumpmuCatch() 
            {
               WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
            }
         }
      }
      return dirNodes[page_id / kDirNodeEntryCount];
   }

   MappingTable(DTID dt_id) : dt_id(dt_id) {
      memset(dirNodes, 0, sizeof(dirNodes));
   }
private:
   std::mutex lock;
   static constexpr int kArraySize = 8192;
   std::atomic<u64> idx {0};
   BufferFrame* dirNodes[kArraySize]; // keep_in_memory = true
   DTID dt_id;
};

struct HeapTupleId {
   u32 data;
   static constexpr u64 kPageIdOffset = 10;
   static constexpr u64 kSlotIdMask = 0x3ff;
   HeapTupleId(u32 d = 0): data(d) {}

   operator u32()const { return data; }

   static HeapTupleId MakeTupleId(u32 heap_page_id, u16 slot_id) {
      return HeapTupleId((((u64)heap_page_id) << kPageIdOffset) | slot_id);
   }

   u32 heap_page_id() {
      return data >> kPageIdOffset;
   }

   u16 slot_id() {
      return data & kSlotIdMask;
   }
};

class HeapFile
{
public:
   ~HeapFile() {}
   // -------------------------------------------------------------------------------------
   OP_RESULT lookup(HeapTupleId id, std::function<void(const u8*, u16)> payload_callback);
   OP_RESULT lookupForUpdate(HeapTupleId id, std::function<bool(u8*, u16)> payload_callback);
   OP_RESULT insert(HeapTupleId & ouput_tuple_id, const u8* value, u16 value_length);
   OP_RESULT remove(HeapTupleId id);
   OP_RESULT scan(std::function<bool(const u8*, u16, HeapTupleId)> callback);
   OP_RESULT scanPage(u32 page_id, std::function<bool(const u8*, u16, HeapTupleId)> callback);
   // -------------------------------------------------------------------------------------
   u64 countPages();
   u64 countEntries();
   u64 getHeapPages() { return data_pages; }
   u64 getPages() { return data_pages.load() + table->numDirNodes(); }
   u64 userDataStored() { return data_stored.load(); }
   // -------------------------------------------------------------------------------------
   static void iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);
   static struct ParentSwipHandler findParent(void * ht_object, BufferFrame& to_find);
   static bool isBTreeLeaf(void* btree_object, BufferFrame& to_find);
   static bool keepInMemory(void* btree_object);
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static std::unordered_map<std::string, std::string> serialize(void* ht_object);
   static bool checkSpaceUtilization(void* ht_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler);
   static void deserialize(void* ht_object, std::unordered_map<std::string, std::string> serialized);
   static void checkpoint(void*, BufferFrame& bf, u8* dest);
   static DTRegistry::DTMeta getMeta();
   void add_heap_pages(u32 n_pages);
   void find_pages_with_free_space();
   u32 get_page_with_space(u32 space_required);
   void update_free_space_in_dir_node(u32 page_id, u32 free_space, bool remove_from_free_page_list);
   void create(DTID dtid);
   double current_load_factor();

   std::function<u64(const u8*, u16)> hash_func;

   void register_hash_function(std::function<u64(const u8*, u16)> f) {
      hash_func = f;
   }
   void disable_heap_growth() { heap_growable.store(false); }
   bool is_heap_growable() { return heap_growable;}

   u64 dataStored() { return data_stored.load(); }

   void setFreeSpaceThreshold(double threshold) { free_space_threshold = threshold; }
   double getFreeSpaceThreshold() { return free_space_threshold; }
private:
   constexpr static u32 N = 32; // number of pages that have space to keep in `pages_with_free_space`
   double free_space_threshold = 0.05;
   std::atomic<u64> data_stored{0};
   std::atomic<s64> data_pages{0};
   MappingTable * table = nullptr;

   std::shared_mutex mtx;
   std::vector<u64> pages_with_free_space;
   u64 next_page_to_scan = 0;
   std::atomic<bool> heap_growable {true};
   static thread_local int this_thread_idx;
   PadInt pages_that_might_have_space[1024];
public:
   DTID dt_id;
   bool hot_partition = false;
};


class IndexedHeapFile
{
public:
   ~IndexedHeapFile() {}
   IndexedHeapFile(HeapFile & heap_file, leanstore::storage::btree::BTreeLL & btree_index) : heap_file(heap_file), btree_index(btree_index) {}
   // -------------------------------------------------------------------------------------
   OP_RESULT lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback);
   OP_RESULT lookupForUpdate(u8* key, u16 key_length, std::function<bool(u8*, u16)> payload_callback);
   OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length);
   OP_RESULT upsert(u8* key, u16 key_length, u8* value, u16 value_length);
   OP_RESULT remove(u8* key, u16 key_length);
   OP_RESULT scanHeapPage(u16 key_length, u32 page_id, std::function<bool(const u8*, u16, const u8*, u16)> callback);
   OP_RESULT scanAsc(u8* key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback);
   // -------------------------------------------------------------------------------------
   u64 getHeapPages() { return heap_file.getHeapPages(); }
   u64 countHeapPages();
   u64 countHeapEntries();
   u64 countIndexPages();
   u64 countIndexEntries();
   u64 indexHeight() { return btree_index.getHeight(); }
   u64 dataPages() { return data_pages.load(); }
   u64 dataStored() { return data_stored.load(); }
   void disable_heap_growth() { heap_file.disable_heap_growth(); }
   bool is_heap_growable() {return heap_file.is_heap_growable(); }
   double utilization() { return heap_file.current_load_factor(); }
   leanstore::storage::btree::BTreeLL & get_index() { return btree_index; };
private:
   OP_RESULT doInsert(u8* key, u16 key_length, u8* value, u16 value_length);
   OP_RESULT doLookupForUpdate(u8* key, u16 key_length, std::function<bool(u8*, u16)> payload_callback);
   std::atomic<u64> data_stored{0};
   std::atomic<s64> data_pages{0};
   MappingTable * table = nullptr;

   HeapFile & heap_file;
   leanstore::storage::btree::BTreeLL & btree_index;
   OptimisticLockTable lock_table;
};

struct HeapPageHeader {
   static const u16 underFullSize = EFFECTIVE_PAGE_SIZE * 0.5;
   static const u16 overfullFullSize = EFFECTIVE_PAGE_SIZE * 0.9;

   u32 page_id = 0;
   u16 count = 0;  // count number of keys
   u16 space_used = 0;  // does not include the header, but includes fences !!!!!
   u16 data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);

   HeapPageHeader(u32 page_id): page_id(page_id) {}
   ~HeapPageHeader() {}

   inline u8* ptr() { return reinterpret_cast<u8*>(this); }
};
using HeadType = u16;
// -------------------------------------------------------------------------------------
enum SlotStatus {
   FREE     = 0,
   OCCUPIED = 1,
   REMOVED  = 2
   // A slot once allocated will not be freed until GC.
   // It transitions into either Occupied or Removed status.
   // An occupied slot states that the tuple is valid.
   // For a removed slot, the offset and payload_len represent the data of the previous tuple; 
   // Its space can be reused for a later insertion. 
};
struct HeapPage : public HeapPageHeader {
   struct __attribute__((packed)) Slot {
      // Layout:  key wihtout prefix | Payload
      u16 offset;
      u16 payload_len: 14;
      u16 status: 2;
   };
   static constexpr u64 pure_slots_capacity = (EFFECTIVE_PAGE_SIZE - sizeof(HeapPageHeader)) / (sizeof(Slot));
   static constexpr u64 left_space_to_waste = (EFFECTIVE_PAGE_SIZE - sizeof(HeapPageHeader)) % (sizeof(Slot));
   Slot slot[pure_slots_capacity];
   u8 padding[left_space_to_waste];

   HeapPage(u32 page_id) : HeapPageHeader(page_id) {
      memset(slot, 0, sizeof(slot));
   }

   void clear() {
      this->count = 0;
      this->space_used = 0;
      this->data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);
   }

   u16 freeSpaceWithoutRemovedTuples() { 
      assert(EFFECTIVE_PAGE_SIZE - data_offset == space_used);
      return EFFECTIVE_PAGE_SIZE - (reinterpret_cast<u8*>(slot + count) - ptr()) - space_used;
   }

   u16 freeSpace() { 
      assert(EFFECTIVE_PAGE_SIZE - data_offset == space_used);
      u64 metadata_used = reinterpret_cast<u8*>(slot + count) - ptr();
      // assert(data_offset >= metadata_used);
      // return data_offset - metadata_used; 
      // find the slot that points to the leftmost position wihtin the page.
      u16 left_most_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);
      s16 left_most_slot = -1;
      u64 space_for_removed_tuples = 0;
      for (s16 i = 0; i < count; ++i) {
         assert(slot[i].status != SlotStatus::FREE);
         if (left_most_offset > slot[i].offset) {
            left_most_slot = i;
            left_most_offset = slot[i].offset;
         }
         if (slot[i].status == SlotStatus::REMOVED) {
            assert(slot[i].payload_len);
            space_for_removed_tuples += slot[i].payload_len;
         }
      }
      assert(left_most_offset >= metadata_used);
      assert(EFFECTIVE_PAGE_SIZE >= metadata_used + space_used);
      return EFFECTIVE_PAGE_SIZE - metadata_used - space_used + space_for_removed_tuples;
   }

   //u16 freeSpaceAfterCompaction() { return EFFECTIVE_PAGE_SIZE - (reinterpret_cast<u8*>(slot + count) - ptr()) - space_used; }
   //u16 spaceUsedForData() { return (reinterpret_cast<u8*>(slot + count) - ptr()) + space_used; }
   // -------------------------------------------------------------------------------------
   //double fillFactorAfterCompaction() { return (1 - (freeSpaceAfterCompaction() * 1.0 / EFFECTIVE_PAGE_SIZE)); }
   // -------------------------------------------------------------------------------------
   bool hasEnoughSpaceFor(u32 space_needed) { return (space_needed <= freeSpace()); }
   // ATTENTION: this method has side effects !
   bool requestSpaceFor(u16 space_needed)
   {
      if (space_needed <= freeSpace())
         return true;
      return false;
   }
   // -------------------------------------------------------------------------------------
   inline u16 getPayloadLength(u16 slotId) { return slot[slotId].payload_len; }
   inline u8* getPayload(u16 slotId) { return ptr() + slot[slotId].offset; }
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   static inline bool cmpKeys(const u8* a, const u8* b, u16 aLength, u16 bLength)
   {
      if (aLength != bLength) 
        return false;
      return memcmp(a, b, aLength) == 0;
   }

   // -------------------------------------------------------------------------------------
   s32 insert(const u8* payload, u16 payload_len);
   u16 spaceNeeded(u16 payload_len);
   bool canInsert(u16 payload_len);
   bool prepareInsert(u16 payload_len);
   // -------------------------------------------------------------------------------------
   bool update(u16 slot_id, u8* payload, u16 payload_length);
   // -------------------------------------------------------------------------------------
   //void compactify();
   // -------------------------------------------------------------------------------------
   u32 spaceUsedBySlot(u16 slot_id);
   SlotStatus slotStatus(u16 slot_id) { return (SlotStatus)slot[slot_id].status; }
   // -------------------------------------------------------------------------------------
   // store key/value pair at slotId
   void storeKeyValue(u16 slotId,const u8* payload, u16 payload_len);
   bool removeSlot(u16 slotId);
};

// -------------------------------------------------------------------------------------
static_assert(sizeof(HeapPage) == EFFECTIVE_PAGE_SIZE, "sizeof(HeapPage) must be equal to one page");
}  // namespace hashing
}  // namespace storage
}  // namespace leanstore
