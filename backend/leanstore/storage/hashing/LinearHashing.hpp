#pragma once
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/FNVHash.hpp"
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
namespace hashing
{

enum class OP_RESULT : u8 { OK = 0, NOT_FOUND = 1, DUPLICATE = 2, ABORT_TX = 3, NOT_ENOUGH_SPACE = 4, OTHER = 5 };

class LinearHashingNode;
using SwipType=Swip<LinearHashingNode>;
// -------------------------------------------------------------------------------------

static constexpr int kDirNodeBucketPtrCount = EFFECTIVE_PAGE_SIZE / sizeof(u64) / 4;
struct DirectoryNode {
   SwipType bucketPtrs[kDirNodeBucketPtrCount];
   u8 padding[EFFECTIVE_PAGE_SIZE - sizeof(bucketPtrs)];
   DirectoryNode() {
      memset(bucketPtrs, 0, sizeof(bucketPtrs));
   }
};

// Map logical bucket ids to pages allocated from buffer manager
class MappingTable {
public:
   
   u64 numSlots() {
      return idx.load() * kDirNodeBucketPtrCount;
   }

   BufferFrame* getDirNode(u64 bucket_id) {
      if (bucket_id < numSlots()) {
         return dirNodes[bucket_id / kDirNodeBucketPtrCount];
      }
      while (bucket_id >= numSlots()) {
         while (true) {
            jumpmuTry() 
            {
               auto write_guard_h = leanstore::storage::HybridPageGuard<DirectoryNode>(dt_id, true, true);
               auto write_guard = ExclusivePageGuard<DirectoryNode>(std::move(write_guard_h));
               write_guard.init();
               auto bf = write_guard.bf();
               bf->header.keep_in_memory = true;
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
      return dirNodes[bucket_id / kDirNodeBucketPtrCount];
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

class LinearHashTable
{
  public:
   ~LinearHashTable() {}
   // -------------------------------------------------------------------------------------
   OP_RESULT lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback, bool & mark_dirty);
   OP_RESULT lookupForUpdate(u8* key, u16 key_length, std::function<bool(u8*, u16)> payload_callback);
   OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length);
   OP_RESULT upsert(u8* key, u16 key_length, u8* value, u16 value_length);
   // virtual OP_RESULT upsert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   //OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0}) override;
   OP_RESULT remove(u8* key, u16 key_length);
   // iterate over all data records in bucket
   OP_RESULT iterate(u64 bucket, 
                     std::function<bool(const u8*, u16, const u8*, u16)> callback,
                     std::function<void()> restart_iterate_setup_context);
   // -------------------------------------------------------------------------------------
   u64 countPages();
   u64 countEntries();
   u64 countBuckets() { return sp.load_buddy_bucket(); }
   u64 powerMultiplier()  { return sp.load_power(); }
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
   u64 hash(const u8 *key, u16 key_length, int i);
   void split();
   void merge_chain(u64 bucket, BufferFrame* dirNode);
   void create(DTID dtid);
   double current_load_factor();
private:
   constexpr static double kSplitLoadFactor = 0.8;
   constexpr static u32 N = 128;
   constexpr static u64 kPowerBits = 8;
   constexpr static u64 kPowerOffset = 64 - 8;
   constexpr static u64 kPowerMask = ((1ull << kPowerBits) - 1)  << kPowerOffset;
   constexpr static u64 kBuddyBucketOffset = 0;;
   constexpr static u64 kBuddyBucketBits = 64 - kPowerBits;
   constexpr static u64 kBuddyBucketMask = ((1ull << kBuddyBucketBits) - 1)  << kBuddyBucketOffset;

   
   struct split_pointer {
      std::atomic<u64> w{N};

      u64 make_word(u32 power, u32 buddy_bucket) {
         return ((u64)power) << kPowerOffset | buddy_bucket;
      }

      void set(u32 power, u32 buddy_bucket) {
         w.store(make_word(power, buddy_bucket));
      }

      std::pair<u32, u32> load_power_and_buddy_bucket() {
         auto v = w.load();
         return std::pair<u32, u32>((kPowerMask & v) >> kPowerOffset, (kBuddyBucketMask & v) >> kBuddyBucketOffset);
      }

      u32 load_buddy_bucket() {
         auto v = w.load();
         return (kBuddyBucketMask & v) >> kBuddyBucketOffset;
      }

      u32 load_power() {
         auto v = w.load();
         return (kPowerMask & v) >> kPowerOffset;
      }

      bool compare_swap(u32 old_power, u32 old_buddy_bucket, u32 new_power, u32 new_buddy_bucket) {
         u64 old_v = make_word(old_power, old_buddy_bucket);
         u64 new_v = make_word(new_power, new_buddy_bucket);
         return w.compare_exchange_strong(old_v, new_v);
      }
   };
   std::atomic<u64> this_round_started_splits{0};
   std::atomic<u64> this_round_finished_splits{0};
   std::atomic<u64> this_round_split_target{N};
   //std::atomic<u64> i_{0};
   //std::atomic<u64> s_{0};
   std::atomic<u64> data_stored{0};
   //std::atomic<u64> s_buddy{N}; // invariant: s + N * 2^i = s_buddy.
   struct split_pointer sp;
   std::shared_mutex split_mtx;
   MappingTable * table = nullptr;
public:
   DTID dt_id;
   bool hot_partition = false;
};


struct LinearHashingNodeHeader {
   static const u16 underFullSize = EFFECTIVE_PAGE_SIZE * 0.6;
   static const u16 overfullFullSize = EFFECTIVE_PAGE_SIZE * 0.9;

   SwipType overflow = nullptr;

   bool is_overflow = true;
   u64 bucket = 0;
   u16 count = 0;  // count number of keys
   u16 space_used = 0;  // does not include the header, but includes fences !!!!!
   u16 data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);

   LinearHashingNodeHeader(u64 bucket, bool is_overflow) : is_overflow(is_overflow), bucket(bucket) {}
   ~LinearHashingNodeHeader() {}

   inline u8* ptr() { return reinterpret_cast<u8*>(this); }
   inline bool isOverflow() { return !is_overflow; }
};
using HeadType = u16;
// -------------------------------------------------------------------------------------
struct LinearHashingNode : public LinearHashingNodeHeader {
   struct __attribute__((packed)) Slot {
      // Layout:  key wihtout prefix | Payload
      u16 offset;
      u16 key_len;
      u16 payload_len : 15;
      union {
         HeadType fingerprint;
         u8 head_bytes[2];
      };
   };
   static constexpr u64 pure_slots_capacity = (EFFECTIVE_PAGE_SIZE - sizeof(LinearHashingNodeHeader)) / (sizeof(Slot));
   static constexpr u64 left_space_to_waste = (EFFECTIVE_PAGE_SIZE - sizeof(LinearHashingNodeHeader)) % (sizeof(Slot));
   Slot slot[pure_slots_capacity];
   u8 padding[left_space_to_waste];

   LinearHashingNode(bool is_overflow, u64 bucket) : LinearHashingNodeHeader(bucket, is_overflow) {}

   void clear() {
      this->count = 0;
      this->space_used = 0;
      this->data_offset = static_cast<u16>(EFFECTIVE_PAGE_SIZE);
      this->bucket = 0;
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
   inline u8* getKey(u16 slotId) { return ptr() + slot[slotId].offset; }
   inline u16 getKeyLen(u16 slotId) { return slot[slotId].key_len; }
   inline u16 getPayloadLength(u16 slotId) { return slot[slotId].payload_len; }
   inline u8* getPayload(u16 slotId) { return ptr() + slot[slotId].offset + slot[slotId].key_len; }
   inline SwipType & getOverflowNode() {return this->overflow; }
   // -------------------------------------------------------------------------------------
   inline void copyFullKey(u16 slotId, u8* out)
   {
      memcpy(out, getKey(slotId), getKeyLen(slotId));
   }
   // -------------------------------------------------------------------------------------
   static inline bool cmpKeys(const u8* a, const u8* b, u16 aLength, u16 bLength)
   {
      if (aLength != bLength) 
        return false;
      return memcmp(a, b, aLength) == 0;
   }

   static inline HeadType fingerprint(const u8* key, u16& keyLength)
   {
      return leanstore::utils::FNV::hash(key, keyLength);
   }
   // -------------------------------------------------------------------------------------
   s32 find(const u8 * key, u16 key_len);
   s32 insert(const u8* key, u16 key_len, const u8* payload, u16 payload_len);
   static u16 spaceNeeded(u16 keyLength, u16 payload_len, u16 prefixLength);
   u16 spaceNeeded(u16 key_length, u16 payload_len);
   bool canInsert(u16 key_length, u16 payload_len);
   bool prepareInsert(u16 keyLength, u16 payload_len);
   // -------------------------------------------------------------------------------------
   bool update(u16 slot_id, u8* key, u16 keyLength, u8* payload, u16 payload_length);
   // -------------------------------------------------------------------------------------
   void compactify();
   // -------------------------------------------------------------------------------------
   // merge right node into this node
   u32 spaceUsedBySlot(u16 slot_id);
   // -------------------------------------------------------------------------------------
   // store key/value pair at slotId
   void storeKeyValue(u16 slotId, const u8* key, u16 key_len, const u8* payload, u16 payload_len);
   // ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   void copyKeyValueRange(LinearHashingNode* dst, u16 dstSlot, u16 srcSlot, u16 count);
   void copyKeyValue(u16 srcSlot, LinearHashingNode* dst, u16 dstSlot);
   // -------------------------------------------------------------------------------------
   // Not synchronized or todo section
   bool removeSlot(u16 slotId);
   bool remove(const u8* key, const u16 keyLength);
};

// -------------------------------------------------------------------------------------
static_assert(sizeof(LinearHashingNode) == EFFECTIVE_PAGE_SIZE, "LinearHashingNode must be equal to one page");
}  // namespace hashing
}  // namespace storage
}  // namespace leanstore
