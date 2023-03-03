#include "HeapFile.hpp"

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
namespace heap
{

bool HeapPage::prepareInsert(u16 payload_len)
{
   const u16 space_needed = spaceNeeded(payload_len);
   if (!requestSpaceFor(space_needed))
      return false;  // no space, insert fails
   else
      return true;
}

// -------------------------------------------------------------------------------------
u16 HeapPage::spaceNeeded(u16 payload_len)
{
   return sizeof(Slot) + payload_len;
}

bool HeapPage::canInsert(u16 payload_len)
{
   const u16 space_needed = spaceNeeded(payload_len);
   if (!hasEnoughSpaceFor(space_needed))
      return false;  // no space, insert fails
   else
      return true;
}


// -------------------------------------------------------------------------------------
bool HeapPage::removeSlot(u16 slotId)
{
   space_used -= getPayloadLength(slotId);
   memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
   count--;
   assert(count >= 0);
   return true;
}

bool HeapPage::update(u16 slotId, u8* payload, u16 payload_length) {
    assert(slotId != -1);
    assert(payload_length == getPayloadLength(slotId));
    memcpy(getPayload(slotId), payload, payload_length);
    return true;
}

void HeapPage::compactify()
{
   u16 should = freeSpaceAfterCompaction();
   static_cast<void>(should);
   HeapPage tmp(0);
   copyKeyValueRange(&tmp, 0, 0, count);
   memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(HeapPage));
   assert(freeSpace() == should);  // TODO: why should ??
}

void HeapPage::storeKeyValue(u16 slotId, const u8* payload, const u16 payload_len)
{
   // -------------------------------------------------------------------------------------
   // Fingerprint
   slot[slotId].payload_len = payload_len;
   // Value
   const u16 space = payload_len;
   data_offset -= space;
   space_used += space;
   slot[slotId].offset = data_offset;
   // -------------------------------------------------------------------------------------
   memcpy(getPayload(slotId), payload, payload_len);
   assert(ptr() + data_offset >= reinterpret_cast<u8*>(slot + count));
}
// ATTENTION: dstSlot then srcSlot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void HeapPage::copyKeyValueRange(HeapPage* dst, u16 dstSlot, u16 srcSlot, u16 count)
{
    // Fast path
    memcpy(dst->slot + dstSlot, slot + srcSlot, sizeof(Slot) * count);
    DEBUG_BLOCK()
    {
        u32 total_space_used = 0;
        for (u16 i = 0; i < this->count; i++) {
        total_space_used += getPayloadLength(i);
        }
        assert(total_space_used == this->space_used);
    }
    for (u16 i = 0; i < count; i++) {
        u32 kv_size = getPayloadLength(srcSlot + i);
        dst->data_offset -= kv_size;
        dst->space_used += kv_size;
        dst->slot[dstSlot + i].offset = dst->data_offset;
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

s32 HeapPage::insert(const u8* payload, u16 payload_len) {
   DEBUG_BLOCK()
   {
      assert(canInsert(payload_len));
   }
   prepareInsert(payload_len);
   s32 slotId = count;
   //memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
   storeKeyValue(slotId, payload, payload_len);
   count++;
   return slotId;
}
void HeapFile::create(DTID dtid) {
   this->dt_id = dtid;
   this->table = new MappingTable(dtid);
   for(u32 p = 0; p < N; ++p) {
      BufferFrame * dirNode = table->getDirNode(p);
      HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
      p_guard.toExclusive();
      HybridPageGuard<HeapPage>  target_x_guard(dt_id, true, this->hot_partition);
      target_x_guard.toExclusive();
      target_x_guard.init(p);
      target_x_guard.incrementGSN();
      assert(p_guard->dataPagePointers[p % kDirNodeEntryCount].ptr.raw() == 0);
      p_guard->dataPagePointers[p % kDirNodeEntryCount].reserved = 0;
      p_guard->dataPagePointers[p % kDirNodeEntryCount].freeSpace = target_x_guard->freeSpaceAfterCompaction();
      p_guard->dataPagePointers[p % kDirNodeEntryCount].ptr = target_x_guard.swip();
      p_guard->node_count++;
      p_guard.incrementGSN();
      std::unique_lock<std::shared_mutex> g(this->mtx);
      pages_with_free_space.push_back(p);
   }
}

static inline u64 power2(int i) {
    constexpr std::array<unsigned long long, 16> LUT = {
     1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768 };
    return LUT[i];
}


OP_RESULT HeapFile::lookup(HeapTupleId id, std::function<void(u8*, u16)> payload_callback) {
   volatile u32 mask = 1;

   while (true) {
      jumpmuTry()
      { 
         if (id.heap_page_id() >= table->numSlots()) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }

         BufferFrame* dirNode = table->getDirNode(id.heap_page_id());
         
         HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
         Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[id.heap_page_id() % kDirNodeEntryCount].ptr;
         if (heap_page_node.bf == nullptr) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         HybridPageGuard<HeapPage> target_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::SPIN);
         p_guard.unlock();
         if (id.slot_id() >= target_guard->count) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         s32 slot_in_node = id.slot_id();
         payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node));
         target_guard.recheck();
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}


OP_RESULT HeapFile::lookupForUpdate(HeapTupleId id, std::function<bool(u8*, u16)> payload_callback) {
   volatile u32 mask = 1;

   while (true) {
      jumpmuTry()
      { 
         if (id.heap_page_id() >= table->numSlots()) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }

         BufferFrame* dirNode = table->getDirNode(id.heap_page_id());
         
         HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
         Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[id.heap_page_id() % kDirNodeEntryCount].ptr;
         if (heap_page_node.bf == nullptr) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         HybridPageGuard<HeapPage> target_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_guard.toExclusive();
         if (id.slot_id() >= target_guard->count) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         s32 slot_in_node = id.slot_id();
         if (payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node))) {
            target_guard.incrementGSN();
         }
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}


u64 HeapFile::countPages() {
   u64 count = 0;
   auto pages = table->numSlots();
   for (u32 pid = 0; pid < pages; ++pid) {
      while (true) {
         u64 sum = 0;
         jumpmuTry()
         {
            BufferFrame * dirNode = table->getDirNode(pid);
            HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
            Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[pid % kDirNodeEntryCount].ptr;
            if (heap_page_node.bf == nullptr) {
               jumpmu_break;
            }
            count += 1;
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

OP_RESULT HeapFile::scan(std::function<bool(const u8*, u16, HeapTupleId)> callback) {
   auto pages = table->numSlots();
   for (u32 pid = 0; pid < pages; ++pid) {
      BufferFrame* dirNode = table->getDirNode(pid);
      while (true) {
         jumpmuTry()
         {
            HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
            Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[pid % kDirNodeEntryCount].ptr;
            if (heap_page_node.bf == nullptr) {
               jumpmu_break;
            }
            HybridPageGuard<HeapPage> target_s_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::SHARED);
            target_s_guard.toShared();
            p_guard.recheck();
            u64 count = target_s_guard->count;
            for (std::size_t i = 0; i < count; ++i) {
               HeapTupleId tuple_id = HeapTupleId::MakeTupleId(pid, i);
               bool should_continue = callback(target_s_guard->getPayload(i), target_s_guard->getPayloadLength(i), tuple_id);
               if (should_continue == false) {
                  target_s_guard.recheck();
                  jumpmu_return OP_RESULT::OK;
               }
            }
            jumpmu_break;
         }
         jumpmuCatch() 
         {
            WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         }
      }
   }

   return OP_RESULT::OK;
}


OP_RESULT HeapFile::insert(HeapTupleId & ouput_tuple_id, u8* value, u16 value_length) {
   while (true) {
      u32 page_id = get_page_with_space(value_length);
      BufferFrame * dirNode = table->getDirNode(page_id);
      bool not_enough_space = false;
      u32 free_space = 0;
      jumpmuTry()
      {
         HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
         Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[page_id % kDirNodeEntryCount].ptr;
         assert(heap_page_node.bf != nullptr);
         HybridPageGuard<HeapPage> target_x_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         if (target_x_guard->canInsert(value_length)) {
            auto slot_id = target_x_guard->insert(value, value_length);
            target_x_guard.incrementGSN();
            ouput_tuple_id = HeapTupleId::MakeTupleId(page_id, slot_id);
            data_stored.fetch_add(value_length);
            not_enough_space = false;
            jumpmu_return OP_RESULT::OK;
         } else {
            free_space = target_x_guard->freeSpaceAfterCompaction();
            not_enough_space = true;
         }
      }
      jumpmuCatch() 
      {
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }

      if (not_enough_space) {
         update_free_space_in_dir_node(page_id, free_space, true);
      }
   }
}

u64 HeapFile::countEntries() {
   u64 count = 0;
   auto pages = table->numSlots();
   for (u64 b = 0; b < pages; ++b) {
      BufferFrame* dirNode = table->getDirNode(b);

      while (true) {
         u64 sum = 0;
         jumpmuTry()
         {
            HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
            Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[b % kDirNodeEntryCount].ptr;
            if (heap_page_node.bf == nullptr) {
               jumpmu_break;
            }
            HybridPageGuard<HeapPage> target_s_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::SPIN);
            p_guard.recheck();
            sum += target_s_guard->count;
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

OP_RESULT HeapFile::remove(HeapTupleId id) {
   while (true) {
      u32 page_id = id.heap_page_id();
      BufferFrame * dirNode = table->getDirNode(page_id);
      u32 free_space = 0;
      bool deleted = false;
      jumpmuTry()
      {
         HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
         Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[page_id % kDirNodeEntryCount].ptr;
         assert(heap_page_node.bf != nullptr);
         HybridPageGuard<HeapPage> target_x_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         if (target_x_guard->count > id.slot_id()) {
            data_stored.fetch_sub(target_x_guard->getPayloadLength(id.slot_id()));
            target_x_guard->removeSlot(id.slot_id());
            target_x_guard.incrementGSN();
            free_space = target_x_guard->freeSpaceAfterCompaction();
            deleted = true;
         }
      }
      jumpmuCatch() 
      {
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }

      if (deleted && (free_space / EFFECTIVE_PAGE_SIZE + 0.0) >= kFreeSpaceThreshold 
                  && (free_space / EFFECTIVE_PAGE_SIZE + 0.0) <= kFreeSpaceThreshold + 0.01) {
         update_free_space_in_dir_node(page_id, free_space, false);
      }
   }
}

double HeapFile::current_load_factor() {
   //return data_stored.load() / (sp.load_buddy_bucket() * sizeof(LinearHashingNode) + 0.0001);
   return data_stored.load() / (data_pages.load() * sizeof(HeapPage) + 0.0);
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


u32 HeapFile::get_page_with_space(u32 space_required) {
   again:
   mtx.lock_shared();
   if (pages_with_free_space.empty()) {
      mtx.unlock_shared();
   } else {
      u32 pid = pages_with_free_space[utils::RandomGenerator::getRand<u64>(0, pages_with_free_space.size())];
      mtx.unlock_shared();
      return pid;
   }

   mtx.lock();
   if (pages_with_free_space.empty()) {
      find_pages_with_free_space();
   }
   mtx.unlock();
   goto again;
}

void HeapFile::update_free_space_in_dir_node(u32 page_id, u32 free_space, bool remove_from_free_page_list) {
   BufferFrame* dirNode = table->getDirNode(page_id);
   while(true) {
      jumpmuTry() {
         HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
         p_guard.toExclusive();
         p_guard->dataPagePointers[page_id % kDirNodeEntryCount].freeSpace = free_space;
         jumpmu_break;
      } jumpmuCatch()
      {
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }

   if (remove_from_free_page_list) {
      mtx.lock();
      pages_with_free_space.erase(std::remove(pages_with_free_space.begin(), pages_with_free_space.end(), page_id), 
                                 pages_with_free_space.end());
      mtx.unlock();
   }
}

void HeapFile::find_pages_with_free_space() {
   assert(pages_with_free_space.empty());
   // collect data pages that have space by scanning the directory pages
   // If could not find enough (N) such pages, let's add a few new pages using `add_heap_pages`.
   std::vector<u64> pages_that_have_space;
   for (u64 d = next_dir_node_to_scan; d < table->numDirNodes(); ++d) {
      BufferFrame* dirNode = table->getDirNode(d);
      while(true) {
         jumpmuTry() {
            pages_that_have_space.clear();
            HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
            for (size_t i = 0; i < kDirNodeEntryCount; ++i) {
               if (p_guard->dataPagePointers[i].ptr.raw() &&
                  ((p_guard->dataPagePointers[i].freeSpace) / EFFECTIVE_PAGE_SIZE + 0.0) >= kFreeSpaceThreshold) {
                  pages_that_have_space.push_back(d * kDirNodeEntryCount + i);
               }
            }
            p_guard.recheck();
            pages_with_free_space.insert(pages_with_free_space.end(), pages_that_have_space.begin(), pages_that_have_space.end());
            next_dir_node_to_scan = (next_dir_node_to_scan + 1) % table->numDirNodes();
            jumpmu_break;
         } jumpmuCatch()
         {
            WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         }
      }
      
      if (pages_with_free_space.size() >= N) {
         break;
      }
   }

   if (pages_with_free_space.size() < N) {
      add_heap_pages(N - pages_with_free_space.size());
   }
}

void HeapFile::add_heap_pages(u32 n_pages) {
   size_t num_dir_nodes = table->numDirNodes();
   for (size_t i = 0; i < n_pages; ++i) {
      while (true) {
         jumpmuTry() {
            u32 page_id = (num_dir_nodes - 1) * kDirNodeEntryCount;
            BufferFrame * dirNode = table->getDirNode(page_id);
            HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
            p_guard.toExclusive();
            if (p_guard->node_count < kDirNodeEntryCount &&
               p_guard->dataPagePointers[p_guard->node_count].ptr.raw() == 0) {
               u32 pid = (num_dir_nodes - 1) * kDirNodeEntryCount + p_guard->node_count;
               HybridPageGuard<HeapPage>  target_x_guard(dt_id, true, this->hot_partition);
               target_x_guard.toExclusive();
               target_x_guard.init(pid);
               target_x_guard.incrementGSN();
               p_guard->dataPagePointers[p_guard->node_count].reserved = 0;
               p_guard->dataPagePointers[p_guard->node_count].freeSpace = target_x_guard->freeSpaceAfterCompaction();
               p_guard->dataPagePointers[p_guard->node_count].ptr = target_x_guard.swip();
               p_guard->node_count++;
               pages_with_free_space.push_back(pid);
               p_guard.incrementGSN();
               jumpmu_break;
            } else {
               num_dir_nodes++;
            }
         } jumpmuCatch()
         {
            WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         }
      }
   }
}

void HeapFile::iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   return;
}
struct ParentSwipHandler HeapFile::findParent(void* ht_obj, BufferFrame& to_find)
{
   HeapFile& hf = *reinterpret_cast<HeapFile*>(ht_obj);
   if (hf.dt_id != to_find.page.dt_id) {
      jumpmu::jump();
   }
   auto& target_node = *reinterpret_cast<HeapPage*>(to_find.page.dt);
   auto page_id = target_node.page_id;
   BufferFrame* dirNode = hf.table->getDirNode(page_id);
   
   // -------------------------------------------------------------------------------------
   HybridPageGuard<HeapDirectoryNode> p_guard(dirNode, dirNode);
   Swip<HeapPage>* c_swip = &p_guard->dataPagePointers[page_id % kDirNodeEntryCount].ptr;
   if (c_swip->isEVICTED()) {
      jumpmu::jump();
   }
   assert(c_swip->bfPtrAsHot() == &to_find);
   p_guard.recheck();
   return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = dirNode};
}


bool HeapFile::isBTreeLeaf(void* ht_object, BufferFrame& to_find) {
   return false;
}

bool HeapFile::keepInMemory(void* ht_object) {
   return reinterpret_cast<HeapFile*>(ht_object)->hot_partition;
}

// -------------------------------------------------------------------------------------
void HeapFile::undo(void*, const u8*, const u64)
{
   // TODO: undo for storage
}
// -------------------------------------------------------------------------------------
void HeapFile::todo(void*, const u8*, const u64) {}

std::unordered_map<std::string, std::string> HeapFile::serialize(void * ht_object)
{
   return {};
}
// -------------------------------------------------------------------------------------
void HeapFile::deserialize(void *, std::unordered_map<std::string, std::string>)
{
   
   // assert(reinterpret_cast<BTreeNode*>(btree.meta_node_bf->page.dt)->count > 0);
}

bool HeapFile::checkSpaceUtilization(void* ht_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler)
{
   return false;
}

void HeapFile::checkpoint(void*, BufferFrame& bf, u8* dest)
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

DTRegistry::DTMeta HeapFile::getMeta() {
   DTRegistry::DTMeta ht_meta = {.iterate_children = HeapFile::iterateChildrenSwips,
                                    .find_parent = HeapFile::findParent,
                                    .is_btree_leaf = HeapFile::isBTreeLeaf,
                                    .check_space_utilization = HeapFile::checkSpaceUtilization,
                                    .checkpoint = HeapFile::checkpoint,
                                    .keep_in_memory = HeapFile::keepInMemory,
                                    .undo = HeapFile::undo,
                                    .todo = HeapFile::todo,
                                    .serialize = HeapFile::serialize,
                                    .deserialize = HeapFile::deserialize};
   return ht_meta;
}

}
}
}