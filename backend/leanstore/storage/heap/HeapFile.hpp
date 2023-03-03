#pragma once
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/FNVHash.hpp"
#include "leanstore/utils/XXHash.hpp"
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
   u64 data;
   static constexpr u64 kSlotIdMask = 0xffffffff;
   HeapTupleId(u64 d = 0): data(d) {}

   operator u64()const { return data; }

   static HeapTupleId MakeTupleId(u32 heap_page_id, u32 slot_id) {
      return HeapTupleId((((u64)heap_page_id) << 32) | slot_id);
   }

   u32 heap_page_id() {
      return data >> 32;
   }

   u32 slot_id() {
      return data & kSlotIdMask;
   }
};

class HeapFile
{
  public:
  public:
   ~HeapFile() {}
   // -------------------------------------------------------------------------------------
   OP_RESULT lookup(HeapTupleId id, std::function<void(u8*, u16)> payload_callback);
   OP_RESULT lookupForUpdate(HeapTupleId id, std::function<bool(u8*, u16)> payload_callback);
   OP_RESULT insert(HeapTupleId & ouput_tuple_id, u8* value, u16 value_length);
   OP_RESULT remove(HeapTupleId id);
   // iterate over all data records in bucket
   OP_RESULT scan(std::function<bool(const u8*, u16, HeapTupleId)> callback);
   // -------------------------------------------------------------------------------------
   u64 countPages();
   u64 countEntries();
   u64 dataPages() { return data_pages.load(); }
   u64 dataStored() { return data_stored.load(); }
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
private:
   constexpr static double kSplitLoadFactor = 0.8;
   constexpr static u32 N = 64; // number of pages that have space to keep in `pages_with_free_space`
   constexpr static double kFreeSpaceThreshold = 0.1;
   constexpr static u64 kPowerBits = 8;
   constexpr static u64 kPowerOffset = 64 - 8;
   constexpr static u64 kPowerMask = ((1ull << kPowerBits) - 1)  << kPowerOffset;
   constexpr static u64 kBuddyBucketOffset = 0;;
   constexpr static u64 kBuddyBucketBits = 64 - kPowerBits;
   constexpr static u64 kBuddyBucketMask = ((1ull << kBuddyBucketBits) - 1)  << kBuddyBucketOffset;
   //std::atomic<u64> i_{0};
   //std::atomic<u64> s_{0};
   std::atomic<u64> data_stored{0};
   std::atomic<s64> data_pages{0};
   MappingTable * table = nullptr;

   std::shared_mutex mtx;
   std::vector<u64> pages_with_free_space;
   u64 next_dir_node_to_scan = 0;
public:
   DTID dt_id;
   bool hot_partition = false;
};


struct HeapPageHeader {
   static const u16 underFullSize = EFFECTIVE_PAGE_SIZE * 0.5;
   static const u16 overfullFullSize = EFFECTIVE_PAGE_SIZE * 0.9;

   SwipType next = nullptr;
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
struct HeapPage : public HeapPageHeader {
   struct __attribute__((packed)) Slot {
      // Layout:  key wihtout prefix | Payload
      u16 offset;
      u16 payload_len : 15;
   };
   static constexpr u64 pure_slots_capacity = (EFFECTIVE_PAGE_SIZE - sizeof(HeapPageHeader)) / (sizeof(Slot));
   static constexpr u64 left_space_to_waste = (EFFECTIVE_PAGE_SIZE - sizeof(HeapPageHeader)) % (sizeof(Slot));
   Slot slot[pure_slots_capacity];
   u8 padding[left_space_to_waste];

   HeapPage(u32 page_id) : HeapPageHeader(page_id) {}

   void clear() {
      this->count = 0;
      this->space_used = 0;
      this->data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);
   }

   u16 freeSpace() { 
      u64 metadata_used = reinterpret_cast<u8*>(slot + count) - ptr();
      assert(data_offset >= metadata_used);
      return data_offset - metadata_used; 
   }
   u16 freeSpaceAfterCompaction() { return EFFECTIVE_PAGE_SIZE - (reinterpret_cast<u8*>(slot + count) - ptr()) - space_used; }
   u16 spaceUsedForData() { return (reinterpret_cast<u8*>(slot + count) - ptr()) + space_used; }
   // -------------------------------------------------------------------------------------
   double fillFactorAfterCompaction() { return (1 - (freeSpaceAfterCompaction() * 1.0 / EFFECTIVE_PAGE_SIZE)); }
   // -------------------------------------------------------------------------------------
   bool hasEnoughSpaceFor(u32 space_needed) { return (space_needed <= freeSpace() || space_needed <= freeSpaceAfterCompaction()); }
   // ATTENTION: this method has side effects !
   bool requestSpaceFor(u16 space_needed)
   {
      if (space_needed <= freeSpace())
         return true;
      if (space_needed <= freeSpaceAfterCompaction()) {
         compactify();
         return true;
      }
      return false;
   }
   // -------------------------------------------------------------------------------------
   inline u16 getPayloadLength(u16 slotId) { return slot[slotId].payload_len; }
   inline u8* getPayload(u16 slotId) { return ptr() + slot[slotId].offset; }
   inline SwipType & getNextNode() {return this->next; }
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
   void compactify();
   // -------------------------------------------------------------------------------------
   u32 spaceUsedBySlot(u16 slot_id);
   // -------------------------------------------------------------------------------------
   // store key/value pair at slotId
   void storeKeyValue(u16 slotId,const u8* payload, u16 payload_len);
   // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   void copyKeyValueRange(HeapPage* dst, u16 dstSlot, u16 srcSlot, u16 count);
   void copyKeyValue(u16 srcSlot, HeapPage* dst, u16 dstSlot);
   // -------------------------------------------------------------------------------------
   bool removeSlot(u16 slotId);
};

// -------------------------------------------------------------------------------------
static_assert(sizeof(HeapPage) == EFFECTIVE_PAGE_SIZE, "sizeof(HeapPage) must be equal to one page");
}  // namespace hashing
}  // namespace storage
}  // namespace leanstore
