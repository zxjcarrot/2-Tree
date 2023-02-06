#include "LinearHashing.hpp"

#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace hashing
{

bool LinearHashingNode::prepareInsert(u16 key_len, u16 payload_len)
{
   const u16 space_needed = spaceNeeded(key_len, payload_len);
   if (!requestSpaceFor(space_needed))
      return false;  // no space, insert fails
   else
      return true;
}

// -------------------------------------------------------------------------------------
u16 LinearHashingNode::spaceNeeded(u16 key_len, u16 payload_len, u16 prefix_len)
{
   return sizeof(Slot) + (key_len - prefix_len) + payload_len;
}
// -------------------------------------------------------------------------------------
u16 LinearHashingNode::spaceNeeded(u16 key_length, u16 payload_len)
{
   return spaceNeeded(key_length, payload_len, 0);
}

bool LinearHashingNode::canInsert(u16 key_len, u16 payload_len)
{
   const u16 space_needed = spaceNeeded(key_len, payload_len);
   if (!hasEnoughSpaceFor(space_needed))
      return false;  // no space, insert fails
   else
      return true;
}

s32 LinearHashingNode::find(const u8 * key, u16 key_len) {
   HeadType f = fingerprint(key, key_len);
   for (size_t i = 0; i < this->count; ++i) {
      if (slot[i].fingerprint == f) {
         if (cmpKeys(key, getKey(i), key_len, getKeyLen(i))) {
               return i;
         }
      }
   }
   return -1;
}


// -------------------------------------------------------------------------------------
bool LinearHashingNode::removeSlot(u16 slotId)
{
   space_used -= getKeyLen(slotId) + getPayloadLength(slotId);
   memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
   count--;
   assert(count >= 0);
   return true;
}
// -------------------------------------------------------------------------------------
bool LinearHashingNode::remove(const u8* key, const u16 keyLength)
{
   int slotId = find(key, keyLength);
   if (slotId == -1)
      return false;  // key not found
   return removeSlot(slotId);
}

bool LinearHashingNode::update(u16 slotId, u8* key, u16 keyLength, u8* payload, u16 payload_length) {
    assert(slotId != -1);
    assert(keyLength == getKeyLen(slotId));
    assert(payload_length == getPayloadLength(slotId));
    assert(memcmp(key, getKey(slotId), keyLength) == 0);
    memcpy(getPayload(slotId), payload, payload_length);
    return true;
}

void LinearHashingNode::compactify()
{
   u16 should = freeSpaceAfterCompaction();
   static_cast<void>(should);
   LinearHashingNode tmp(is_overflow, bucket);
   copyKeyValueRange(&tmp, 0, 0, count);
   memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(LinearHashingNode));
   assert(freeSpace() == should);  // TODO: why should ??
}

void LinearHashingNode::storeKeyValue(u16 slotId, const u8* key, u16 key_len, const u8* payload, const u16 payload_len)
{
   // -------------------------------------------------------------------------------------
   // Fingerprint
   slot[slotId].fingerprint = fingerprint(key, key_len);
   slot[slotId].key_len = key_len;
   slot[slotId].payload_len = payload_len;
   // Value
   const u16 space = key_len + payload_len;
   data_offset -= space;
   space_used += space;
   slot[slotId].offset = data_offset;
   // -------------------------------------------------------------------------------------
   memcpy(getKey(slotId), key, key_len);
   // -------------------------------------------------------------------------------------
   memcpy(getPayload(slotId), payload, payload_len);
   assert(ptr() + data_offset >= reinterpret_cast<u8*>(slot + count));
}
// ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void LinearHashingNode::copyKeyValueRange(LinearHashingNode* dst, u16 dstSlot, u16 srcSlot, u16 count)
{
    // Fast path
    memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
    DEBUG_BLOCK()
    {
        u32 total_space_used = 0;
        for (u16 i = 0; i < this->count; i++) {
        total_space_used += getKeyLen(i) + getPayloadLength(i);
        }
        assert(total_space_used == this->space_used);
    }
    for (u16 i = 0; i < count; i++) {
        u32 kv_size = getKeyLen(srcSlot + i) + getPayloadLength(srcSlot + i);
        dst->data_offset -= kv_size;
        dst->space_used += kv_size;
        dst->slot[dstSlot + i].offset = dst->data_offset;
        dst->slot[dstSlot + i].fingerprint = this->slot[i].fingerprint;
        DEBUG_BLOCK()
        {
        [[maybe_unused]] s64 off_by = reinterpret_cast<u8*>(dst->slot + dstSlot + count) - (dst->ptr() + dst->data_offset);
        assert(off_by <= 0);
        }
        memcpy(dst->ptr() + dst->data_offset, ptr() + slot[srcSlot + i].offset, kv_size);
    }
   dst->count += count;
   assert((dst->ptr() + dst->data_offset) >= reinterpret_cast<u8*>(dst->slot + dst->count));
}

s32 LinearHashingNode::insert(const u8* key, u16 key_len, const u8* payload, u16 payload_len) {
   DEBUG_BLOCK()
   {
      assert(canInsert(key_len, payload_len));
      s32 exact_pos = find(key, key_len);
      static_cast<void>(exact_pos);
      assert(exact_pos == -1);  // assert for duplicates
   }
   prepareInsert(key_len, payload_len);
   s32 slotId = count;
   //memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
   storeKeyValue(slotId, key, key_len, payload, payload_len);
   count++;
   return slotId;
   // -------------------------------------------------------------------------------------
   DEBUG_BLOCK()
   {
      s32 exact_pos = find(key, key_len);
      static_cast<void>(exact_pos);
      assert(exact_pos == slotId);  // assert for duplicates
   }
}
void LinearHashTable::create(DTID dtid) {
   this->dt_id = dtid;
   this->table = new MappingTable(dtid);
   this->sp.set(0, N);
   // pre-allocate N buckets
   for(int b = 0; b < N; ++b) {
      BufferFrame * dirNode = table->getDirNode(b);
      HybridPageGuard<DirectoryNode> p_guard(dirNode);
      p_guard.toExclusive();
      HybridPageGuard<LinearHashingNode>  target_x_guard(dt_id, true, this->hot_partition);
      target_x_guard.toExclusive();
      target_x_guard.init(false, b);
      target_x_guard.incrementGSN();
      p_guard->bucketPtrs[b % kDirNodeBucketPtrCount] = target_x_guard.swip();
      p_guard.incrementGSN();
   }
}

static inline u64 power2(int i) {
    constexpr std::array<unsigned long long, 16> LUT = {
     1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768 };
    return LUT[i];
}

u64 LinearHashTable::hash(const u8 *key, u16 key_length, int i) {
    u64 h = leanstore::utils::FNV::hash(key, key_length);
    return h % (power2(i) * N);
}


OP_RESULT LinearHashTable::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback, bool & mark_dirty) {
    volatile u32 mask = 1;

    while (true) {
      jumpmuTry()
      {
         //split_mtx.lock_shared();
         auto pair1 = this->sp.load_power_and_buddy_bucket();
         auto local_i = pair1.first;
         auto buddy_bucket = pair1.second;
         auto first_hash_bucket = hash(key, key_length, local_i);
         u64 bucket = first_hash_bucket;
         assert(buddy_bucket >= power2(local_i) * N);
         auto s_ = buddy_bucket - power2(local_i) * N;
         if (bucket < s_) {
            bucket = hash(key, key_length, local_i + 1);
         }
         //split_mtx.unlock_shared();

         BufferFrame* dirNode = table->getDirNode(bucket);
         
         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         if (hash_node.bf == nullptr) {
            jumpmu::jump(); // Split is still ongoing, retry
         }
         HybridPageGuard<LinearHashingNode> target_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::SPIN);
         p_guard.unlock();
         s32 slot_in_node = target_guard->find(key, key_length);
         if (slot_in_node != -1) {
            payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node));
            target_guard.recheck();
            if (mark_dirty) {
               target_guard.incrementGSN();
            }
            jumpmu_return OP_RESULT::OK;
         }

         if (target_guard->overflow.raw()) {
            HybridPageGuard<LinearHashingNode> pp_guard;
            while (true) {
               Swip<LinearHashingNode>& c_swip = target_guard->overflow;
               pp_guard = std::move(target_guard);
               target_guard = HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::SPIN);

               s32 slot_in_node = target_guard->find(key, key_length);
               pp_guard.recheck();
               if (slot_in_node != -1) {
                  payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node));
                  target_guard.recheck();
                  jumpmu_return OP_RESULT::OK;
               }
               if (target_guard->overflow.raw() == 0) {
                  target_guard.recheck();
                  break;
               }
            }
         }

         {
            auto pair2 = this->sp.load_power_and_buddy_bucket();
            if (pair2 != pair1) {
               jumpmu::jump();
            }
         }

         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}


OP_RESULT LinearHashTable::lookupForUpdate(u8* key, u16 key_length, std::function<bool(u8*, u16)> payload_callback) {
    volatile u32 mask = 1;
    
    while (true) {
      jumpmuTry()
      {
         auto pair1 = this->sp.load_power_and_buddy_bucket();
         auto local_i = pair1.first;
         auto buddy_bucket = pair1.second;
         auto first_hash_bucket = hash(key, key_length, local_i);
         u64 bucket = first_hash_bucket;
         assert(buddy_bucket >= power2(local_i) * N);
         auto s_ = buddy_bucket - power2(local_i) * N;
         if (bucket < s_) {
            bucket = hash(key, key_length, local_i + 1);
         }

         BufferFrame* dirNode = table->getDirNode(bucket);

         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         if (hash_node.bf == nullptr) {
            jumpmu_continue;
         }
         HybridPageGuard<LinearHashingNode> target_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_guard.toExclusive();
         p_guard.recheck();
         {
            auto pair2 = this->sp.load_power_and_buddy_bucket();
            if (pair2 != pair1) {
               jumpmu_continue;
            }
         }
         s32 slot_in_node = target_guard->find(key, key_length);
         if (slot_in_node != -1) {
            bool updated = payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node));
            target_guard.recheck();
            if (updated) {
               target_guard.incrementGSN();
            }
            jumpmu_return OP_RESULT::OK;
         }

         if (target_guard->overflow.raw()) {
            HybridPageGuard<LinearHashingNode> pp_guard;
            while (true) {
               Swip<LinearHashingNode>& c_swip = target_guard->overflow;
               pp_guard = std::move(target_guard);
               target_guard = HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::EXCLUSIVE);

               s32 slot_in_node = target_guard->find(key, key_length);
               if (slot_in_node != -1) {
                  bool updated = payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node));
                  target_guard.recheck();
                  if (updated) {
                     target_guard.incrementGSN();
                  }
                  jumpmu_return OP_RESULT::OK;
               }
               if (target_guard->overflow.raw() == 0) {
                  target_guard.recheck();
                  break;
               }
            }
         }

         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}


u64 LinearHashTable::countPages() {
   u64 count = 0;
   auto buddy_bucket = sp.load_buddy_bucket();
   for (u64 b = 0; b < buddy_bucket; ++b) {
      BufferFrame* dirNode = table->getDirNode(b);

      while (true) {
         u64 sum = 0;
         jumpmuTry()
         {
            HybridPageGuard<DirectoryNode> p_guard(dirNode);
            Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[b % kDirNodeBucketPtrCount];
            if (hash_node.bf == nullptr) {
               jumpmu_break;
            }
            HybridPageGuard<LinearHashingNode> target_s_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::SPIN);
            p_guard.recheck();
            sum += 1;
            if (target_s_guard->overflow.raw()) {
               HybridPageGuard<LinearHashingNode> pp_guard;
               while (true) {
                  Swip<LinearHashingNode>& c_swip = target_s_guard->overflow;
                  pp_guard = std::move(target_s_guard);
                  target_s_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::SPIN));
                  sum += 1;
                  pp_guard.recheck();
                  if (target_s_guard->overflow.raw() == 0) {
                     break;
                  }
               }
            }
            count += sum;
            jumpmu_break;
         }
         jumpmuCatch() 
         {
            WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         }
      }
   }

   return count;   
}

OP_RESULT LinearHashTable::iterate(u64 bucket, 
   std::function<bool(const u8*, u16, const u8*, u16)> callback,
   std::function<void()> restart_iterate_setup_context) {
   BufferFrame* dirNode = table->getDirNode(bucket);

   while (true) {
      u64 sum = 0;
      jumpmuTry()
      {
         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         if (hash_node.bf == nullptr) {
            jumpmu_break;
         }
         HybridPageGuard<LinearHashingNode> target_s_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::SHARED);
         target_s_guard.toShared();
         p_guard.recheck();
         u64 count = target_s_guard->count;
         for (std::size_t i = 0; i < count; ++i) {
            bool should_continue = callback(target_s_guard->getKey(i), target_s_guard->getKeyLen(i),
                     target_s_guard->getPayload(i), target_s_guard->getPayloadLength(i));
            if (should_continue == false) {
               target_s_guard.recheck();
               jumpmu_return OP_RESULT::OK;
            }
         }
         if (target_s_guard->overflow.raw()) {
            HybridPageGuard<LinearHashingNode> pp_guard;
            while (true) {
               Swip<LinearHashingNode>& c_swip = target_s_guard->overflow;
               pp_guard = std::move(target_s_guard);
               target_s_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::SHARED));
               pp_guard.toShared();
               target_s_guard.toShared();
               count = target_s_guard->count;
               for (std::size_t i = 0; i < count; ++i) {
                  bool should_continue = callback(target_s_guard->getKey(i), target_s_guard->getKeyLen(i),
                           target_s_guard->getPayload(i), target_s_guard->getPayloadLength(i));
                  if (should_continue == false) {
                     jumpmu_return OP_RESULT::OK;
                  }
               }
               if (target_s_guard->overflow.raw() == 0) {
                  break;
               }
            }
         }
         jumpmu_break;
      }
      jumpmuCatch() 
      {
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         restart_iterate_setup_context();
      }
   }

   return OP_RESULT::OK;
}

u64 LinearHashTable::countEntries() {
   u64 count = 0;
   auto buddy_bucket = sp.load_buddy_bucket();
   for (u64 b = 0; b < buddy_bucket; ++b) {
      BufferFrame* dirNode = table->getDirNode(b);

      while (true) {
         u64 sum = 0;
         jumpmuTry()
         {
            HybridPageGuard<DirectoryNode> p_guard(dirNode);
            Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[b % kDirNodeBucketPtrCount];
            if (hash_node.bf == nullptr) {
               jumpmu_break;
            }
            HybridPageGuard<LinearHashingNode> target_s_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::SPIN);
            p_guard.recheck();
            sum += target_s_guard->count;
            if (target_s_guard->overflow.raw()) {
               HybridPageGuard<LinearHashingNode> pp_guard;
               while (true) {
                  Swip<LinearHashingNode>& c_swip = target_s_guard->overflow;
                  pp_guard = std::move(target_s_guard);
                  target_s_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::SPIN));
                  sum += target_s_guard->count;
                  pp_guard.recheck();
                  if (target_s_guard->overflow.raw() == 0) {
                     break;
                  }
               }
            }
            count += sum;
            jumpmu_break;
         }
         jumpmuCatch() 
         {
            WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         }
      }
   }

   return count;   
}

OP_RESULT LinearHashTable::remove(u8* key, u16 key_length) {
   volatile u32 mask = 1;

   bool try_merge = false;

   OP_RESULT ret;
   while (true) {
      jumpmuTry()
      {
         //split_mtx.lock_shared();
         auto pair1 = this->sp.load_power_and_buddy_bucket();
         auto local_i = pair1.first;
         auto buddy_bucket = pair1.second;
         auto first_hash_bucket = hash(key, key_length, local_i);
         u64 bucket = first_hash_bucket;
         assert(buddy_bucket >= power2(local_i) * N);
         auto s_ = buddy_bucket - power2(local_i) * N;
         if (bucket < s_) {
            bucket = hash(key, key_length, local_i + 1);
         }
         //split_mtx.unlock_shared();

         BufferFrame* dirNode = table->getDirNode(bucket);

         try_merge = false;
         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         if (hash_node.bf == nullptr) {
            jumpmu_continue;
         }
         HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         p_guard.recheck();
         {
            auto pair2 = this->sp.load_power_and_buddy_bucket();
            if (pair2 != pair1) {
               jumpmu::jump();
            }
            auto power = pair2.first;
            auto bucket2 = hash(key, key_length, power);
            auto s_2 = pair2.second - N * power2(power);
            if (bucket2 < s_2) {
               power = power + 1;
               bucket2 = hash(key, key_length, power);
            }
            if (bucket2 != bucket) {
               jumpmu::jump();
            }
         }
         if (target_x_guard->freeSpaceAfterCompaction() > LinearHashingNode::underFullSize) {
            try_merge = true;
         }
         s32 slot_in_node = target_x_guard->find(key, key_length);
         if (slot_in_node != -1) {
            data_stored.fetch_sub(target_x_guard->getKeyLen(slot_in_node) + target_x_guard->getPayloadLength(slot_in_node));
            bool res = target_x_guard->removeSlot(slot_in_node);
            target_x_guard.incrementGSN();
            assert(res);
            ret = OP_RESULT::OK; 
            jumpmu_break;
         } else if (target_x_guard->overflow.raw() == 0) {
            try_merge = false;
            ret = OP_RESULT::NOT_FOUND;
            jumpmu_break;
         }

         if (target_x_guard->overflow.raw()) {
            HybridPageGuard<LinearHashingNode> pp_guard;
            while (true) {
               Swip<LinearHashingNode>& c_swip = target_x_guard->overflow;
               pp_guard = std::move(target_x_guard);
               target_x_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::EXCLUSIVE));
               target_x_guard.toExclusive();
               p_guard.recheck();
               if (target_x_guard->freeSpaceAfterCompaction() > LinearHashingNode::underFullSize) {
                  try_merge = true;
               }
               s32 slot_in_node = target_x_guard->find(key, key_length);
               if (slot_in_node != -1) {
                  data_stored.fetch_sub(target_x_guard->getKeyLen(slot_in_node) + target_x_guard->getPayloadLength(slot_in_node));
                  bool res = target_x_guard->removeSlot(slot_in_node);
                  target_x_guard.incrementGSN();
                  assert(res);
                  ret = OP_RESULT::OK; 
                  break;
               }
               if (target_x_guard->overflow.raw() == 0) {
                  ret = OP_RESULT::NOT_FOUND;
                  break;
               }
            }
         }
         jumpmu_break;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
   //try_merge = false;
   if (try_merge) {
      //split_mtx.lock_shared();
      auto pair1 = this->sp.load_power_and_buddy_bucket();
      auto local_i = pair1.first;
      auto buddy_bucket = pair1.second;
      auto first_hash_bucket = hash(key, key_length, local_i);
      u64 bucket = first_hash_bucket;
      assert(buddy_bucket >= power2(local_i) * N);
      auto s_ = buddy_bucket - power2(local_i) * N;
      if (bucket < s_) {
         bucket = hash(key, key_length, local_i + 1);
      }
      //split_mtx.unlock_shared();

      BufferFrame* dirNode = table->getDirNode(bucket);
      merge_chain(bucket, dirNode);
   }
   return ret;
}

double LinearHashTable::current_load_factor() {
   return data_stored.load() / (sp.load_buddy_bucket() * sizeof(LinearHashingNode) + 0.0001);
   //return data_stored.load() / (data_pages.load() * sizeof(LinearHashingNode) + 0.00001);
}

void LinearHashTable::merge_chain(u64 bucket, BufferFrame* dirNode) {
   volatile u32 mask = 1;
   jumpmuTry()
   {
      HybridPageGuard<DirectoryNode> p_guard(dirNode);
      Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
      HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
      target_x_guard.toExclusive();
      p_guard.unlock();
      if (target_x_guard->overflow.raw()) {
         HybridPageGuard<LinearHashingNode> pp_guard;
         while (true) {
            Swip<LinearHashingNode>& c_swip = target_x_guard->overflow;
            pp_guard = std::move(target_x_guard);
            target_x_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::EXCLUSIVE));
            pp_guard.toExclusive();
            target_x_guard.toExclusive();
            if (pp_guard->hasEnoughSpaceFor(target_x_guard->spaceUsedForData())) {
               // merge
               pp_guard->requestSpaceFor(target_x_guard->spaceUsedForData());
               if (target_x_guard->count) {
                  target_x_guard->copyKeyValueRange(pp_guard.ptr(), pp_guard->count, 0, target_x_guard->count);
               }
               pp_guard->overflow = target_x_guard->overflow;
               pp_guard.incrementGSN();
               target_x_guard.incrementGSN();
               target_x_guard.guard.latch->assertExclusivelyLatched();
               // release the page back to the storage manager
               target_x_guard.reclaim();
               this->data_pages--;
               assert(this->data_pages >= 0);
               break;
            }

            if (target_x_guard->overflow.raw() == 0) {
               break;
            }
         }
      }
   } jumpmuCatch()
   {
      BACKOFF_STRATEGIES()
      WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
   }
}

class DeferCode {
public:
   DeferCode() = delete;
   DeferCode(std::function<void()> f): f(f) {}
   ~DeferCode() { 
      f(); 
   }
   std::function<void()> f;
};

OP_RESULT LinearHashTable::upsert(u8* key, u16 key_length, u8* value, u16 value_length) {
   volatile u32 mask = 1;
   OP_RESULT ret = OP_RESULT::OK;

   while (true) {
      jumpmuTry()
      {
         //split_mtx.lock_shared();
         auto pair1 = this->sp.load_power_and_buddy_bucket();
         auto local_i = pair1.first;
         auto buddy_bucket = pair1.second;
         auto first_hash_bucket = hash(key, key_length, local_i);
         u64 bucket = first_hash_bucket;
         assert(buddy_bucket >= power2(local_i) * N);
         auto s_ = buddy_bucket - power2(local_i) * N;
         if (bucket < s_) {
            bucket = hash(key, key_length, local_i + 1);
         }
         //split_mtx.unlock_shared();

         BufferFrame* dirNode = table->getDirNode(bucket);

         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         if (hash_node.bf == nullptr) {
            jumpmu_continue;
         }
         HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         p_guard.recheck();
         {
            auto pair2 = this->sp.load_power_and_buddy_bucket();
            if (pair2 != pair1) {
               jumpmu::jump();
            }
            auto power = pair2.first;
            auto bucket2 = hash(key, key_length, power);
            auto s_2 = pair2.second - N * power2(power);
            if (bucket2 < s_2) {
               power = power + 1;
               bucket2 = hash(key, key_length, power);
            }
            if (bucket2 != bucket) {
               jumpmu::jump();
            }
         }
         s32 slot_in_node = target_x_guard->find(key, key_length);
         if (slot_in_node != -1) {
            data_stored.fetch_sub(target_x_guard->getKeyLen(slot_in_node) + target_x_guard->getPayloadLength(slot_in_node));
            target_x_guard->update(slot_in_node, key, key_length, value, value_length);
            target_x_guard.incrementGSN();
            data_stored.fetch_add(key_length + value_length);
            ret = OP_RESULT::OK;
            jumpmu_break;
         } else if (target_x_guard->overflow.raw() == 0) {
            if (target_x_guard->canInsert(key_length, value_length)) {
               target_x_guard->insert(key, key_length, value, value_length);   
               target_x_guard.incrementGSN();
               data_stored.fetch_add(key_length + value_length);
               ret = OP_RESULT::OK;
               jumpmu_break;
            }
         }

         if (target_x_guard->overflow.raw()) {
            HybridPageGuard<LinearHashingNode> pp_guard;
            bool jump_break_out = false;
            while (true) {
               Swip<LinearHashingNode>& c_swip = target_x_guard->overflow;
               pp_guard = std::move(target_x_guard);
               target_x_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::EXCLUSIVE));
               target_x_guard.toExclusive();

               s32 slot_in_node = target_x_guard->find(key, key_length);
               if (slot_in_node != -1) {
                  data_stored.fetch_sub(target_x_guard->getKeyLen(slot_in_node) + target_x_guard->getPayloadLength(slot_in_node));
                  target_x_guard->update(slot_in_node, key, key_length, value, value_length);
                  target_x_guard.incrementGSN();
                  data_stored.fetch_add(key_length + value_length);
                  ret = OP_RESULT::OK;
                  jump_break_out = true;
                  break;
               }
               if (target_x_guard->overflow.raw() == 0) {
                  if (target_x_guard->canInsert(key_length, value_length)) {
                     target_x_guard->insert(key, key_length, value, value_length);
                     data_stored.fetch_add(key_length + value_length);
                     target_x_guard.incrementGSN();
                     ret = OP_RESULT::OK; 
                     jump_break_out = true;
                  }
                  break;
               }
            }
            if (jump_break_out) {
               jumpmu_break;
            }
         }
          
         // add an overflow node and trigger split
         HybridPageGuard<LinearHashingNode> node(dt_id, true, this->hot_partition);
         auto overflow_node_guard = ExclusivePageGuard<LinearHashingNode>(std::move(node));
         overflow_node_guard.init(true, bucket);
         assert(overflow_node_guard->canInsert(key_length, value_length) == true);
         s32 slot_idx = overflow_node_guard->insert(key, key_length, value, value_length);
         data_stored.fetch_add(key_length + value_length);
         assert(slot_idx != -1);
         assert(target_x_guard->overflow.raw() == 0);
         target_x_guard->overflow = overflow_node_guard.swip();
         target_x_guard.incrementGSN();
         ret = OP_RESULT::OK; 
         this->data_pages++;
         assert(this->data_pages >= 0);
         jumpmu_break;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }


   // Controlled Split
   if (current_load_factor() > kSplitLoadFactor) {
      split();
   }
   
   return OP_RESULT::OK;
}

OP_RESULT LinearHashTable::insert(u8* key, u16 key_length, u8* value, u16 value_length) {
   volatile u32 mask = 1;
   OP_RESULT ret = OP_RESULT::OK;

   while (true) {
      jumpmuTry()
      {
         //split_mtx.lock_shared();
         auto pair1 = this->sp.load_power_and_buddy_bucket();
         auto local_i = pair1.first;
         auto buddy_bucket = pair1.second;
         auto first_hash_bucket = hash(key, key_length, local_i);
         u64 bucket = first_hash_bucket;
         assert(buddy_bucket >= power2(local_i) * N);
         auto s_ = buddy_bucket - power2(local_i) * N;
         if (bucket < s_) {
            bucket = hash(key, key_length, local_i + 1);
         }
         //split_mtx.unlock_shared();

         BufferFrame* dirNode = table->getDirNode(bucket);

         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         if (hash_node.bf == nullptr) {
            jumpmu::jump();
         }
         HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         {
            auto pair2 = this->sp.load_power_and_buddy_bucket();
            if (pair2 != pair1) {
               jumpmu::jump();
            }
            auto power = pair2.first;
            auto bucket2 = hash(key, key_length, power);
            auto s_2 = pair2.second - N * power2(power);
            if (bucket2 < s_2) {
               power = power + 1;
               bucket2 = hash(key, key_length, power);
            }
            if (bucket2 != bucket) {
               jumpmu::jump();
            }
         }
         p_guard.unlock();
         
         s32 slot_in_node = target_x_guard->find(key, key_length);
         if (slot_in_node != -1) {
            ret = OP_RESULT::DUPLICATE;
            jumpmu_break;
         } else if (target_x_guard->overflow.raw() == 0) {
            if (target_x_guard->canInsert(key_length, value_length)) {
               p_guard.recheck();
               target_x_guard->insert(key, key_length, value, value_length);   
               target_x_guard.incrementGSN();
               data_stored.fetch_add(key_length + value_length);
               ret = OP_RESULT::OK;
               jumpmu_break;
            }
         }

         if (target_x_guard->overflow.raw()) {
            HybridPageGuard<LinearHashingNode> pp_guard;
            bool jump_break_out = false;
            while (true) {
               Swip<LinearHashingNode>& c_swip = target_x_guard->overflow;
               pp_guard = std::move(target_x_guard);
               target_x_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::EXCLUSIVE));
               target_x_guard.toExclusive();

               s32 slot_in_node = target_x_guard->find(key, key_length);
               if (slot_in_node != -1) {
                  ret = OP_RESULT::DUPLICATE;
                  jump_break_out = true;
                  break;
               }
               if (target_x_guard->overflow.raw() == 0) {
                  if (target_x_guard->canInsert(key_length, value_length)) {
                     p_guard.recheck();
                     target_x_guard->insert(key, key_length, value, value_length);
                     target_x_guard.incrementGSN();
                     data_stored.fetch_add(key_length + value_length);
                     ret = OP_RESULT::OK;
                     jump_break_out = true;
                  }
                  break;
               }
            }
            if (jump_break_out) {
               jumpmu_break;
            }
         }
         // add an overflow node and trigger split
         HybridPageGuard<LinearHashingNode> node(dt_id, true, this->hot_partition);
         auto overflow_node_guard = ExclusivePageGuard<LinearHashingNode>(std::move(node));
         overflow_node_guard.init(true, bucket);
         assert(overflow_node_guard->canInsert(key_length, value_length) == true);
         s32 slot_idx = overflow_node_guard->insert(key, key_length, value, value_length);
         assert(slot_idx != -1);
         assert(target_x_guard->overflow.raw() == 0);
         data_stored.fetch_add(key_length + value_length);
         target_x_guard->overflow = overflow_node_guard.swip();
         target_x_guard.incrementGSN();
         ret = OP_RESULT::OK;
         this->data_pages++;
         assert(this->data_pages >= 0);
         jumpmu_break;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }

   if (current_load_factor() > kSplitLoadFactor) {
      split();
   }
   return ret;
}

void LinearHashTable::split() {
   restart:
   split_mtx.lock();
   std::pair<u32, u32> pair1 = this->sp.load_power_and_buddy_bucket();

   auto local_i = pair1.first;
   auto old_buddy_bucket = pair1.second;
   if (this_round_started_splits + 1 > this_round_split_target) {
      assert(old_buddy_bucket == power2(local_i + 1) * N);
      split_mtx.unlock();
      goto restart;
   }
   auto buddy_bucket = old_buddy_bucket;
   bool res = this->sp.compare_swap(local_i, old_buddy_bucket, local_i, old_buddy_bucket + 1);
   assert(res);
   auto split_bucket = buddy_bucket - power2(local_i) * N;
   //assert(s_ <= split_bucket);
   assert(buddy_bucket >= power2(local_i) * N);
   ++this_round_started_splits;
   split_mtx.unlock();
   BufferFrame* splitDirNode = table->getDirNode(split_bucket);
   BufferFrame* buddy_bucket_node = table->getDirNode(buddy_bucket);

   // allocate enough nodes so we do not have to allocate while holding write locks.
   constexpr size_t kNTempNodes = 10;

   std::vector<LinearHashingNode> split_bucket_tmp(1, LinearHashingNode(false, split_bucket));
   std::vector<LinearHashingNode> buddy_bucket_tmp(1, LinearHashingNode(false, buddy_bucket));
   while(true) {
      bool succeed = false;
      // partition the data from the split bucket into two chains of buckets
      // handle overflow chains properly
      
      std::vector<HybridPageGuard<LinearHashingNode>> split_bucket_x_guards(kNTempNodes);
      u32 split_bucket_x_guard_used_idx = 0;
      assert(split_bucket_tmp.size() == 1);
      assert(buddy_bucket_tmp.size() == 1);
      bool must_be_no_jump = false;
      jumpmuTry()
      {
         HybridPageGuard<DirectoryNode> p_guard(splitDirNode);
         HybridPageGuard<LinearHashingNode> pp_x_guard;

         Swip<LinearHashingNode> & first_hash_node = p_guard->bucketPtrs[split_bucket % kDirNodeBucketPtrCount];
         HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, first_hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         p_guard.unlock();

         split_bucket_x_guards[split_bucket_x_guard_used_idx++] = std::move(target_x_guard);

         // obtain x locks on all the pages of this bucket
         while (true) {
            Swip<LinearHashingNode>& overflow_swip = split_bucket_x_guards[split_bucket_x_guard_used_idx - 1]->overflow;
            if (overflow_swip.raw() == 0) {
               break;
            }
            split_bucket_x_guards[split_bucket_x_guard_used_idx] = std::move(HybridPageGuard<LinearHashingNode>(split_bucket_x_guards[split_bucket_x_guard_used_idx - 1], overflow_swip, LATCH_FALLBACK_MODE::EXCLUSIVE));
            split_bucket_x_guards[split_bucket_x_guard_used_idx - 1].toExclusive();
            split_bucket_x_guards[split_bucket_x_guard_used_idx].toExclusive();
            split_bucket_x_guard_used_idx++;
         }

         HybridPageGuard<LinearHashingNode> temp_node_1(dt_id, false, this->hot_partition);
         HybridPageGuard<LinearHashingNode> temp_node_2(dt_id, false, this->hot_partition);

         //p_guard.toExclusive();
         // from now on, there should be no jumps
         must_be_no_jump = true;
         // partition the data in the split bucket into two buckets
         for (std::size_t j = 0; j < split_bucket_x_guard_used_idx; ++j) {
            HybridPageGuard<LinearHashingNode> & x_guard = split_bucket_x_guards[j];
            x_guard.guard.latch->assertExclusivelyLatched();
            for (int i = 0; i < x_guard->count; ++i) {
               auto original_bucket = hash(x_guard->getKey(i), x_guard->getKeyLen(i), local_i);
               auto target_bucket = hash(x_guard->getKey(i), x_guard->getKeyLen(i), local_i + 1);
               assert(original_bucket == split_bucket);
               if (target_bucket == split_bucket) {
                  if (split_bucket_tmp.back().canInsert(x_guard->getKeyLen(i), x_guard->getPayloadLength(i)) == false) {
                     split_bucket_tmp.push_back(LinearHashingNode(true, split_bucket));
                  }
                  assert(split_bucket_tmp.back().canInsert(x_guard->getKeyLen(i), x_guard->getPayloadLength(i)));
                  auto slot = split_bucket_tmp.back().insert(x_guard->getKey(i), x_guard->getKeyLen(i), x_guard->getPayload(i), x_guard->getPayloadLength(i));
                  assert(slot != -1);
               } else {
                  assert(target_bucket == buddy_bucket);
                  if (buddy_bucket_tmp.back().canInsert(x_guard->getKeyLen(i), x_guard->getPayloadLength(i)) == false) {
                     buddy_bucket_tmp.push_back(LinearHashingNode(true, buddy_bucket));
                  }
                  assert(buddy_bucket_tmp.back().canInsert(x_guard->getKeyLen(i), x_guard->getPayloadLength(i)));
                  auto slot = buddy_bucket_tmp.back().insert(x_guard->getKey(i), x_guard->getKeyLen(i), x_guard->getPayload(i), x_guard->getPayloadLength(i));
                  assert(slot != -1);
               }
            }
         }
         assert(split_bucket_tmp.size() <= split_bucket_x_guard_used_idx);

         for (std::size_t i = 0; i < split_bucket_tmp.size(); ++i) {
            HybridPageGuard<LinearHashingNode> & x_guard = split_bucket_x_guards[i];
            bool old_overflow = x_guard->is_overflow;
            SwipType old_overflow_swip = x_guard->overflow;
            auto bucket = x_guard->bucket;

            assert(sizeof(*x_guard.ptr()) == sizeof(split_bucket_tmp[i]));
            memcpy(x_guard.ptr(), &split_bucket_tmp[i], sizeof(split_bucket_tmp[i]));
            
            x_guard->is_overflow = old_overflow;
            x_guard->overflow = old_overflow_swip;
            x_guard->bucket = x_guard->bucket;
            x_guard.incrementGSN();
         }
         //assert(split_bucket_tmp.size() + buddy_bucket_tmp.size() >= split_bucket_x_guard_used_idx);
         u32 buddy_bucket_guard_idx = split_bucket_tmp.size();
         HybridPageGuard<LinearHashingNode> * buddy_bucket_head_page_x_guard = nullptr;

         assert(split_bucket_tmp.size() <= split_bucket_x_guard_used_idx);
         HybridPageGuard<LinearHashingNode> & x_guard = split_bucket_x_guards[split_bucket_tmp.size() - 1];
         x_guard->overflow = nullptr;
         x_guard.incrementGSN();

         //assert(temp_nodes_used_idx == 0);
         if (split_bucket_x_guard_used_idx - split_bucket_tmp.size() < buddy_bucket_tmp.size()) {
            temp_node_1.keep_alive = true;
            split_bucket_x_guards[split_bucket_x_guard_used_idx] = std::move(temp_node_1);
            split_bucket_x_guards[split_bucket_x_guard_used_idx].guard.latch->assertExclusivelyLatched();
            split_bucket_x_guard_used_idx++;
            this->data_pages++;
            assert(this->data_pages >= 0);
            if (split_bucket_x_guard_used_idx - split_bucket_tmp.size() < buddy_bucket_tmp.size()) {
               temp_node_2.keep_alive = true;
               split_bucket_x_guards[split_bucket_x_guard_used_idx] = std::move(temp_node_2);
               split_bucket_x_guards[split_bucket_x_guard_used_idx].guard.latch->assertExclusivelyLatched();
               split_bucket_x_guard_used_idx++;
               this->data_pages++;
               assert(this->data_pages >= 0);
            }
         }

         assert(split_bucket_x_guard_used_idx - split_bucket_tmp.size() >= buddy_bucket_tmp.size());
         assert(split_bucket_x_guard_used_idx <= kNTempNodes);

         buddy_bucket_head_page_x_guard = &split_bucket_x_guards[split_bucket_tmp.size()];

         for (std::size_t i = 0; i < buddy_bucket_tmp.size(); ++i) {
            HybridPageGuard<LinearHashingNode> & x_guard = split_bucket_x_guards[split_bucket_tmp.size() + i];
            x_guard.guard.latch->assertExclusivelyLatched();
            assert(sizeof(*x_guard.ptr()) == sizeof(buddy_bucket_tmp[i]));
            memcpy(x_guard.ptr(), &buddy_bucket_tmp[i], sizeof(buddy_bucket_tmp[i]));
            x_guard->overflow = nullptr;
            x_guard->is_overflow = true;
            x_guard->bucket = buddy_bucket;
            if (i > 0) {
               split_bucket_x_guards[split_bucket_tmp.size() + i - 1]->overflow = x_guard.swip();
            } else {
               x_guard->is_overflow = false;
            }
            x_guard.incrementGSN();
         }

         HybridPageGuard<DirectoryNode> buddy_p_guard;
         if (buddy_bucket_node != splitDirNode) { // Check if the bucket pointer is in the same directory node to avoid double locking 
            buddy_p_guard = std::move(HybridPageGuard<DirectoryNode>(buddy_bucket_node));
            //buddy_p_guard.toExclusive();
         }


         split_mtx.lock();
         //s_ = s_ + 1;
         ++this_round_finished_splits;
         assert(this_round_finished_splits <= this_round_started_splits);
         if(this_round_finished_splits == this_round_started_splits && this_round_started_splits == this_round_split_target) { 
            std::pair<u32, u32> pair2 = this->sp.load_power_and_buddy_bucket();
            u32 old_power = pair2.first;
            u32 old_buddy_bucket = pair2.second;
            u32 new_power = old_power + 1;
            u32 new_buddy_bucket = old_buddy_bucket;
            this_round_split_target = N * power2(new_power); // next round
            this_round_finished_splits = this_round_started_splits = 0; // reset all counters
            bool res = this->sp.compare_swap(old_power, old_buddy_bucket, new_power, new_buddy_bucket);
            assert(res);
         }
         split_mtx.unlock();
         // Link directory slot with new chain
         if (buddy_bucket_node == splitDirNode) {
            assert(p_guard->bucketPtrs[buddy_bucket % kDirNodeBucketPtrCount].bf == nullptr);
            p_guard->bucketPtrs[buddy_bucket % kDirNodeBucketPtrCount] = (*buddy_bucket_head_page_x_guard).swip();
            p_guard.incrementGSN();
         } else {
            assert(buddy_p_guard->bucketPtrs[buddy_bucket % kDirNodeBucketPtrCount].bf == nullptr);
            buddy_p_guard->bucketPtrs[buddy_bucket % kDirNodeBucketPtrCount] = (*buddy_bucket_head_page_x_guard).swip();
            buddy_p_guard.incrementGSN();
         }
         
         // Release whatever pages that are left
         for (std::size_t i = buddy_bucket_tmp.size() + split_bucket_tmp.size(); i < split_bucket_x_guard_used_idx; ++i) {
            HybridPageGuard<LinearHashingNode> & x_guard = split_bucket_x_guards[i];
            x_guard.reclaim();
            this->data_pages--;
            assert(this->data_pages >= 0);
         }
         succeed = true;
         //split_dir_node_guard = std::move(p_guard);
      }
      jumpmuCatch()
      {
         assert(must_be_no_jump == false);
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         assert(succeed == false);
      }

      if (succeed) {
         break;
      }
   }

}


void LinearHashTable::iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   // Pre: bf is read locked
   auto& node = *reinterpret_cast<LinearHashingNode*>(bf.page.dt);
   if (node.overflow.raw() == 0) {
      return;
   }
   if (!callback(node.overflow.cast<BufferFrame>())) {
      return;
   }
}
struct ParentSwipHandler LinearHashTable::findParent(void* ht_obj, BufferFrame& to_find)
{
   LinearHashTable& ht = *reinterpret_cast<LinearHashTable*>(ht_obj);
   if (ht.dt_id != to_find.page.dt_id) {
      jumpmu::jump();
   }
   auto& target_node = *reinterpret_cast<LinearHashingNode*>(to_find.page.dt);
   auto bucket = target_node.bucket;
   BufferFrame* dirNode = ht.table->getDirNode(bucket);
   
   // -------------------------------------------------------------------------------------
   HybridPageGuard<DirectoryNode> p_guard(dirNode, dirNode);
   Swip<LinearHashingNode>& c_swip = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
   if (c_swip.bfPtrAsHot() == &to_find) {
      p_guard.recheck();
      return {.swip = c_swip.cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = dirNode};
   }
   if (c_swip.isEVICTED()) {
      jumpmu::jump();
   }

   HybridPageGuard<LinearHashingNode> target_guard(p_guard, c_swip);
   // -------------------------------------------------------------------------------------
   //Swip<BTreeNode>* c_swip = &p_guard->upper;

   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   // check if bf is the root node
   
   if (target_guard->overflow.raw() == 0) {
      jumpmu::jump(); // did not find
   }
   // -------------------------------------------------------------------------------------
   HybridPageGuard<LinearHashingNode> pp_guard;
   while (true) {
      Swip<LinearHashingNode>& swip = target_guard->overflow;
      // We do not want to incur an I/O during findParent
      if (swip.isEVICTED()) {
         jumpmu::jump();
      }
      pp_guard = std::move(target_guard);
      if (swip.bfPtrAsHot() == &to_find) {
         pp_guard.recheck();
         return {.swip = swip.cast<BufferFrame>(), .parent_guard = std::move(pp_guard.guard), .parent_bf = pp_guard.bf};
      }
      // Load the swip and check the next node in the chain
      target_guard = HybridPageGuard<LinearHashingNode>(pp_guard, swip);  

      if (target_guard->overflow.raw() == 0) {
         break;
      }
   }

   jumpmu::jump(); // did not find
   return {.swip = c_swip.cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = dirNode};
}


bool LinearHashTable::isBTreeLeaf(void* ht_object, BufferFrame& to_find) {
   return false;
}

bool LinearHashTable::keepInMemory(void* ht_object) {
   return reinterpret_cast<LinearHashTable*>(ht_object)->hot_partition;
}

// -------------------------------------------------------------------------------------
void LinearHashTable::undo(void*, const u8*, const u64)
{
   // TODO: undo for storage
}
// -------------------------------------------------------------------------------------
void LinearHashTable::todo(void*, const u8*, const u64) {}

std::unordered_map<std::string, std::string> LinearHashTable::serialize(void * ht_object)
{
   return {};
}
// -------------------------------------------------------------------------------------
void LinearHashTable::deserialize(void *, std::unordered_map<std::string, std::string>)
{
   
   // assert(reinterpret_cast<BTreeNode*>(btree.meta_node_bf->page.dt)->count > 0);
}

bool LinearHashTable::checkSpaceUtilization(void* ht_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler)
{
   return false;
}

void LinearHashTable::checkpoint(void*, BufferFrame& bf, u8* dest)
{
   // std::memcpy(dest, bf.page.dt, EFFECTIVE_PAGE_SIZE);
   // auto& dest_node = *reinterpret_cast<BTreeNode*>(dest);
   // // root node is handled as inner
   // if (dest_node.isInner()) {
   //    for (u64 t_i = 0; t_i < dest_node.count; t_i++) {
   //       if (!dest_node.getChild(t_i).isEVICTED()) {
   //         auto& child_bf = *dest_node.getChild(t_i).bfPtrAsHot();
   //         dest_node.getChild(t_i).evict(child_bf.header.pid);
   //       }
   //    }
   //    if (!dest_node.upper.isEVICTED()) {
   //       auto& child_bf = *dest_node.upper.bfPtrAsHot();
   //       dest_node.upper.evict(child_bf.header.pid);
   //    }
   // }
}

DTRegistry::DTMeta LinearHashTable::getMeta() {
   DTRegistry::DTMeta ht_meta = {.iterate_children = LinearHashTable::iterateChildrenSwips,
                                    .find_parent = LinearHashTable::findParent,
                                    .is_btree_leaf = LinearHashTable::isBTreeLeaf,
                                    .check_space_utilization = LinearHashTable::checkSpaceUtilization,
                                    .checkpoint = LinearHashTable::checkpoint,
                                    .keep_in_memory = LinearHashTable::keepInMemory,
                                    .undo = LinearHashTable::undo,
                                    .todo = LinearHashTable::todo,
                                    .serialize = LinearHashTable::serialize,
                                    .deserialize = LinearHashTable::deserialize};
   return ht_meta;
}

}
}
}