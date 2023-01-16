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

bool LinearHashingNode::update(u8* key, u16 keyLength, u16 payload_length, u8* payload) {
    ensure(false);
    return false;
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
   // pre-allocate N buckets
   for(int b = 0; b < N; ++b) {
      BufferFrame * dirNode = table->getDirNode(b);
      HybridPageGuard<DirectoryNode> p_guard(dirNode);
      p_guard.toExclusive();
      HybridPageGuard<LinearHashingNode>  target_x_guard(dt_id);
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


OP_RESULT LinearHashTable::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) {
    u64 bucket = hash(key, key_length, i_);
    volatile u32 mask = 1;
    if (bucket < s_) {
      bucket = hash(key, key_length, i_ + 1);
    }

    BufferFrame* dirNode = table->getDirNode(bucket);
    
    while (true) {
      jumpmuTry()
      {
         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         HybridPageGuard<LinearHashingNode> target_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::SPIN);
         p_guard.recheck();
         s32 slot_in_node = target_guard->find(key, key_length);
         if (slot_in_node != -1) {
            payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node));
            target_guard.recheck();
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
    u64 bucket = hash(key, key_length, i_);
    volatile u32 mask = 1;
    if (bucket < s_) {
        bucket = hash(key, key_length, i_ + 1);
    }

    BufferFrame* dirNode = table->getDirNode(bucket);
    
    while (true) {
      jumpmuTry()
      {
         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         HybridPageGuard<LinearHashingNode> target_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         p_guard.recheck();
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
   for (u64 b = 0; b < s_buddy.load(); ++b) {
      BufferFrame* dirNode = table->getDirNode(b);

      while (true) {
         u64 sum = 0;
         jumpmuTry()
         {
            HybridPageGuard<DirectoryNode> p_guard(dirNode);
            Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[b % kDirNodeBucketPtrCount];
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
u64 LinearHashTable::countEntries() {
   u64 count = 0;
   for (u64 b = 0; b < s_buddy.load(); ++b) {
      BufferFrame* dirNode = table->getDirNode(b);

      while (true) {
         u64 sum = 0;
         jumpmuTry()
         {
            HybridPageGuard<DirectoryNode> p_guard(dirNode);
            Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[b % kDirNodeBucketPtrCount];
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
   auto local_i = i_.load();
   u64 bucket = hash(key, key_length, local_i);
   volatile u32 mask = 1;
   if (bucket < s_) {
      bucket = hash(key, key_length, local_i + 1);
   }

   BufferFrame* dirNode = table->getDirNode(bucket);
   bool try_merge = false;
   OP_RESULT ret;
   while (true) {
      jumpmuTry()
      {
         try_merge = false;
         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         p_guard.recheck();
         if (target_x_guard->freeSpaceAfterCompaction() > LinearHashingNode::underFullSize) {
            try_merge = true;
         }
         s32 slot_in_node = target_x_guard->find(key, key_length);
         if (slot_in_node != -1) {
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

   if (try_merge) {
      merge_chain(bucket, dirNode);
   }
   return ret;
}

void LinearHashTable::merge_chain(u64 bucket, BufferFrame* dirNode) {
   volatile u32 mask = 1;
   jumpmuTry()
   {
      HybridPageGuard<DirectoryNode> p_guard(dirNode);
      Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
      HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
      target_x_guard.toExclusive();
      p_guard.recheck();
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
               // release the page back to the storage manager
               target_x_guard.reclaim();
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

OP_RESULT LinearHashTable::insert(u8* key, u16 key_length, u8* value, u16 value_length) {
   auto local_i = i_.load();
   u64 bucket = hash(key, key_length, local_i);
   volatile u32 mask = 1;
   if (bucket < s_) {
      bucket = hash(key, key_length, local_i + 1);
   }

   BufferFrame* dirNode = table->getDirNode(bucket);

   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<DirectoryNode> p_guard(dirNode);
         Swip<LinearHashingNode> & hash_node = p_guard->bucketPtrs[bucket % kDirNodeBucketPtrCount];
         HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         p_guard.recheck();
         s32 slot_in_node = target_x_guard->find(key, key_length);
         if (slot_in_node != -1) {
            jumpmu_return OP_RESULT::DUPLICATE; 
         } else if (target_x_guard->overflow.raw() == 0) {
            if (target_x_guard->canInsert(key_length, value_length)) {
               target_x_guard->insert(key, key_length, value, value_length);   
               target_x_guard.incrementGSN();
               jumpmu_return OP_RESULT::OK; 
            }
         }

         if (target_x_guard->overflow.raw()) {
            HybridPageGuard<LinearHashingNode> pp_guard;
            while (true) {
               Swip<LinearHashingNode>& c_swip = target_x_guard->overflow;
               pp_guard = std::move(target_x_guard);
               target_x_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_guard, c_swip, LATCH_FALLBACK_MODE::EXCLUSIVE));
               target_x_guard.toExclusive();

               s32 slot_in_node = target_x_guard->find(key, key_length);
               if (slot_in_node != -1) {
                  jumpmu_return OP_RESULT::DUPLICATE;
               }
               if (target_x_guard->overflow.raw() == 0) {
                  if (target_x_guard->canInsert(key_length, value_length)) {
                     target_x_guard->insert(key, key_length, value, value_length);
                     target_x_guard.incrementGSN();
                     jumpmu_return OP_RESULT::OK; 
                  }
                  break;
               }
            }
         }
         // add an overflow node and trigger split
         HybridPageGuard<LinearHashingNode> overflow_node(dt_id);
         auto overflow_node_guard = ExclusivePageGuard<LinearHashingNode>(std::move(overflow_node));
         overflow_node_guard.init(true, bucket);
         assert(overflow_node_guard->canInsert(key_length, value_length) == true);
         s32 slot_idx = overflow_node_guard->insert(key, key_length, value, value_length);
         assert(slot_idx != -1);
         assert(target_x_guard->overflow.raw() == 0);
         target_x_guard->overflow = overflow_node_guard.bf();
         target_x_guard.incrementGSN();
         jumpmu_break;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }

split:
   split();
   return OP_RESULT::OK;
}

void LinearHashTable::split() {
   split_mtx.lock();
   auto buddy_bucket = s_buddy.fetch_add(1);
   auto local_i = i_.load();
   auto split_bucket = buddy_bucket - power2(i_) * N;
   assert(buddy_bucket >= power2(i_) * N);
   split_mtx.unlock();
   BufferFrame* splitDirNode = table->getDirNode(split_bucket);
   while(true) {
      bool succeed = false;
      u32 temp_nodes_used_idx = 0;
      // allocate enough nodes so we do not have to allocate while holding write locks.
      constexpr size_t kNTempNodes = 4;
      std::vector<HybridPageGuard<LinearHashingNode>> temp_nodes(kNTempNodes);
      
      for (size_t i = 0; i < 4; ++i) {
         temp_nodes[i] = HybridPageGuard<LinearHashingNode>(dt_id);
         temp_nodes[i].toExclusive();
      }

      HybridPageGuard<LinearHashingNode> original_split_bucket_chain_head_guard;
      jumpmuTry()
      {
         HybridPageGuard<DirectoryNode> p_guard(splitDirNode);
         HybridPageGuard<LinearHashingNode> pp_x_guard;

         Swip<LinearHashingNode> & first_hash_node = p_guard->bucketPtrs[split_bucket % kDirNodeBucketPtrCount];
         HybridPageGuard<LinearHashingNode> target_x_guard(p_guard, first_hash_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();

         // partition the data from the split bucket into two chains of buckets
         // handle overflow chains properly
         std::vector<LinearHashingNode> split_bucket_tmp(1, LinearHashingNode(false, split_bucket));
         std::vector<LinearHashingNode> buddy_bucket_tmp(1, LinearHashingNode(false, buddy_bucket));
         int idx = 0;
         while (true) {
            for (int i = 0; i < target_x_guard->count; ++i) {
               auto target_bucket = hash(target_x_guard->getKey(i), target_x_guard->getKeyLen(i), local_i + 1);
               if (target_bucket == split_bucket) {
                  if (split_bucket_tmp.back().canInsert(target_x_guard->getKeyLen(i), target_x_guard->getPayloadLength(i)) == false) {
                     split_bucket_tmp.push_back(LinearHashingNode(true, split_bucket));
                  }
                  assert(split_bucket_tmp.back().canInsert(target_x_guard->getKeyLen(i), target_x_guard->getPayloadLength(i)));
                  auto slot = split_bucket_tmp.back().insert(target_x_guard->getKey(i), target_x_guard->getKeyLen(i), target_x_guard->getPayload(i), target_x_guard->getPayloadLength(i));
                  assert(slot != -1);
               } else {
                  assert(target_bucket == buddy_bucket);
                  if (buddy_bucket_tmp.back().canInsert(target_x_guard->getKeyLen(i), target_x_guard->getPayloadLength(i)) == false) {
                     buddy_bucket_tmp.push_back(LinearHashingNode(true, buddy_bucket));
                  }
                  assert(buddy_bucket_tmp.back().canInsert(target_x_guard->getKeyLen(i), target_x_guard->getPayloadLength(i)));
                  auto slot = buddy_bucket_tmp.back().insert(target_x_guard->getKey(i), target_x_guard->getKeyLen(i), target_x_guard->getPayload(i), target_x_guard->getPayloadLength(i));
                  assert(slot != -1);
               }
            }
            if (pp_x_guard.swip().bfPtrAsHot() == first_hash_node.bfPtrAsHot()) {
               pp_x_guard.bf->header.latch.assertExclusivelyLatched();
               original_split_bucket_chain_head_guard = std::move(pp_x_guard);
               original_split_bucket_chain_head_guard.bf->header.latch.assertExclusivelyLatched();
            }
            ++idx;
            if (target_x_guard->overflow.raw() == 0) {
               if (original_split_bucket_chain_head_guard.bf == nullptr) {
                  target_x_guard.bf->header.latch.assertExclusivelyLatched();
                  original_split_bucket_chain_head_guard = std::move(target_x_guard);
                  original_split_bucket_chain_head_guard.bf->header.latch.assertExclusivelyLatched();
               }
               break;
            }
            
            Swip<LinearHashingNode>& overflow_swip = target_x_guard->overflow;
            target_x_guard.bf->header.latch.assertExclusivelyLatched();
            pp_x_guard = std::move(target_x_guard);
            pp_x_guard.bf->header.latch.assertExclusivelyLatched();
            target_x_guard = std::move(HybridPageGuard<LinearHashingNode>(pp_x_guard, overflow_swip, LATCH_FALLBACK_MODE::EXCLUSIVE));
            pp_x_guard.toExclusive();
            target_x_guard.toExclusive();
         }
         p_guard.recheck();
         
         // fill temp nodes for split bucket
         HybridPageGuard<LinearHashingNode> * split_bucket_first_node_guard = &temp_nodes[temp_nodes_used_idx];
         HybridPageGuard<LinearHashingNode> * prev_guard = nullptr;
         for (int i = 0; i < split_bucket_tmp.size(); ++i) {
            assert(temp_nodes_used_idx < kNTempNodes);
            HybridPageGuard<LinearHashingNode> * this_node_guard = &temp_nodes[temp_nodes_used_idx++];
            (*this_node_guard)->overflow = nullptr;
            if (prev_guard != nullptr) {
               (*prev_guard)->overflow = (*this_node_guard).swip();
               (*prev_guard).incrementGSN();
            }
            memcpy((*this_node_guard)->ptr(), &split_bucket_tmp[i], sizeof(LinearHashingNode));
            this_node_guard->incrementGSN();
            prev_guard = this_node_guard;
         }


         // Fill the buddy chain with contents from original chain
         prev_guard = nullptr;
         HybridPageGuard<LinearHashingNode> * buddy_bucket_first_node_guard = &temp_nodes[temp_nodes_used_idx];
         for (int i = 0; i < buddy_bucket_tmp.size(); ++i) {
            assert(temp_nodes_used_idx < kNTempNodes);
            HybridPageGuard<LinearHashingNode> * this_node_guard = &temp_nodes[temp_nodes_used_idx++];
            (*this_node_guard)->overflow = nullptr;
            if (prev_guard != nullptr) {
               (*prev_guard)->overflow = (*this_node_guard).swip();
               (*prev_guard).incrementGSN();
            }
            memcpy((*this_node_guard)->ptr(), &buddy_bucket_tmp[i], sizeof(LinearHashingNode));
            this_node_guard->incrementGSN();
            prev_guard = this_node_guard;
         }
         
         p_guard.recheck();
         p_guard.toExclusive();
         BufferFrame* buddy_bucket_node = table->getDirNode(buddy_bucket);
         HybridPageGuard<DirectoryNode> buddy_p_guard;
         if (buddy_bucket_node != splitDirNode) { // Check if the bucket pointer is in the same directory node to avoid double locking 
            buddy_p_guard = std::move(HybridPageGuard<DirectoryNode>(buddy_bucket_node));
            buddy_p_guard.toExclusive();
         }

         // Link directory slot with new chain
         if (buddy_bucket_node == splitDirNode) {
            p_guard->bucketPtrs[buddy_bucket % kDirNodeBucketPtrCount] = buddy_bucket_first_node_guard->swip();
            p_guard.incrementGSN();
         } else {
            buddy_p_guard->bucketPtrs[buddy_bucket % kDirNodeBucketPtrCount] = buddy_bucket_first_node_guard->swip();
            buddy_p_guard.incrementGSN();
         }
         
         // Link directory slot with new chain
         p_guard->bucketPtrs[split_bucket % kDirNodeBucketPtrCount] = split_bucket_first_node_guard->swip();

         succeed = true;
      }
      jumpmuCatch()
      {
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         assert(succeed == false);
      }
      if (succeed == false) {
         // reclaim all in temp_nodes
         for (size_t i = 0; i < temp_nodes.size(); ++i) {
            temp_nodes[i].reclaim();
         }
      } else {
         // reclaim unsed nodes left in temp_nodes
         for (size_t i = temp_nodes_used_idx; i < temp_nodes.size(); ++i) {
            temp_nodes[i].reclaim();
         }

         if (original_split_bucket_chain_head_guard.swip().raw()) {
            while (true) {
               jumpmuTry()
               {
                  // reclaim nodes in orignal split_bucket chain
                  Swip<LinearHashingNode>& c_swip = original_split_bucket_chain_head_guard->overflow;
                  if (c_swip.raw()) {
                     HybridPageGuard<LinearHashingNode> target_x_guard(original_split_bucket_chain_head_guard, c_swip, LATCH_FALLBACK_MODE::EXCLUSIVE);
                     target_x_guard.toExclusive();
                     original_split_bucket_chain_head_guard.reclaim();
                     original_split_bucket_chain_head_guard = std::move(target_x_guard);
                  } else {
                     original_split_bucket_chain_head_guard.reclaim();
                     original_split_bucket_chain_head_guard = HybridPageGuard<LinearHashingNode>();
                     jumpmu_break;
                  }
               }
               jumpmuCatch()
               {
                  WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
               }
            }
         }
         break;
      }
   }

   split_mtx.lock();
   s_ = s_ + 1;
   if (s_ >= N * power2(i_)) { // next round
      assert(s_buddy >= N * power2(i_));
      i_ = i_ + 1;
      s_ = 0;
   }
   split_mtx.unlock();
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
   HybridPageGuard<DirectoryNode> p_guard(dirNode);
   u16 level = 0;
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
}


bool LinearHashTable::isBTreeLeaf(void* ht_object, BufferFrame& to_find) {
   return false;
}

bool LinearHashTable::keepInMemory(void* ht_object) {
   return reinterpret_cast<LinearHashTable*>(ht_object)->keep_in_memory;
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
void LinearHashTable::deserialize(void * ht_object, std::unordered_map<std::string, std::string> map)
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