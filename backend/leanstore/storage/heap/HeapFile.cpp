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

static std::atomic<int> thread_id{0};
thread_local int HeapFile::this_thread_idx = thread_id.fetch_add(1);

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
   //space_used -= getPayloadLength(slotId);
   //memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
   //count--;
   assert(slotId < count);
   slot[slotId].status = SlotStatus::REMOVED;
   assert(count >= 0);
   return true;
}

bool HeapPage::update(u16 slotId, u8* payload, u16 payload_length) {
    assert(slotId != -1);
    assert(payload_length == getPayloadLength(slotId));
    memcpy(getPayload(slotId), payload, payload_length);
    return true;
}


void HeapPage::storeKeyValue(u16 slotId, const u8* payload, const u16 payload_len)
{
   // -------------------------------------------------------------------------------------
   // Fingerprint
   assert(slot[slotId].status == SlotStatus::FREE);
   slot[slotId].payload_len = payload_len;
   // Value
   const u16 space = payload_len;
   data_offset -= space;
   space_used += space;
   slot[slotId].offset = data_offset;
   slot[slotId].status = SlotStatus::OCCUPIED;
   // -------------------------------------------------------------------------------------
   memcpy(getPayload(slotId), payload, payload_len);
   assert(ptr() + data_offset >= reinterpret_cast<u8*>(slot + count));
}

s32 HeapPage::insert(const u8* payload, u16 payload_len) {
   assert(EFFECTIVE_PAGE_SIZE - data_offset == space_used);
   if (spaceNeeded(payload_len) <= freeSpaceWithoutRemovedTuples()) {
      s32 slotId = count;
      storeKeyValue(slotId, payload, payload_len);
      count++;
      assert(EFFECTIVE_PAGE_SIZE - data_offset == space_used);
      return slotId;
   } else {
      // find a removed slot
      for (s32 i = 0; i < count; ++i) {
         if (slot[i].status == SlotStatus::REMOVED) { 
            assert(payload_len == slot[i].payload_len); // Note, this is a hack. All our benchmarks use tuples of the same size
            memcpy(getPayload(i), payload, payload_len); // overwrite 
            slot[i].status = SlotStatus::OCCUPIED;
            return i;
         }
      }
      // Could not find any space
      return -1;
   }
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
      p_guard->dataPagePointers[p % kDirNodeEntryCount].freeSpace = target_x_guard->freeSpace();
      p_guard->dataPagePointers[p % kDirNodeEntryCount].ptr = target_x_guard.swip();
      p_guard->node_count++;
      p_guard.incrementGSN();
      this->data_pages++;
      std::unique_lock<std::shared_mutex> g(this->mtx);
      pages_with_free_space.push_back(p);
   }
   for (int i = 0; i < 1024; ++i) {
      pages_that_might_have_space[i].data = std::numeric_limits<u64>::max();
   }
}

static inline u64 power2(int i) {
    constexpr std::array<unsigned long long, 16> LUT = {
     1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768 };
    return LUT[i];
}


OP_RESULT HeapFile::lookup(HeapTupleId id, std::function<void(const u8*, u16)> payload_callback) {
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
         if (id.slot_id() >= target_guard->count || target_guard->slotStatus(id.slot_id()) != SlotStatus::OCCUPIED) {
            target_guard.recheck();
            p_guard.recheck();
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         s32 slot_in_node = id.slot_id();
         payload_callback(target_guard->getPayload(slot_in_node), target_guard->getPayloadLength(slot_in_node));
         target_guard.recheck();
         p_guard.recheck();
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
         if (id.slot_id() >= target_guard->count || target_guard->slotStatus(id.slot_id()) != SlotStatus::OCCUPIED) {
            p_guard.recheck();
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

OP_RESULT HeapFile::scanPage(u32 pid, std::function<bool(const u8*, u16, HeapTupleId)> callback) {
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
            if (target_s_guard->slotStatus(i) != SlotStatus::OCCUPIED) {
               continue;
            }
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

   return OP_RESULT::OK;
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
               if (target_s_guard->slotStatus(i) != SlotStatus::OCCUPIED) {
                  continue;
               }
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


OP_RESULT HeapFile::insert(HeapTupleId & ouput_tuple_id, const u8* value, u16 value_length) {
   while (true) {
      u32 page_id = get_page_with_space(value_length);
      BufferFrame * dirNode = table->getDirNode(page_id);
      bool not_enough_space = false;
      u32 free_space = 0;
      bool jumped = false;
      jumpmuTry()
      {
         HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
         Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[page_id % kDirNodeEntryCount].ptr;
         assert(heap_page_node.bf != nullptr);
         HybridPageGuard<HeapPage> target_x_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         s32 slot_id = target_x_guard->insert(value, value_length);
         if (slot_id != -1) {
            target_x_guard.incrementGSN();
            ouput_tuple_id = HeapTupleId::MakeTupleId(page_id, slot_id);
            data_stored.fetch_add(value_length);
            not_enough_space = false;
         } else {
            free_space = target_x_guard->freeSpace();
            not_enough_space = true;
         }
      }
      jumpmuCatch() 
      {
         jumped = true;
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
      if (!jumped) {
         if (not_enough_space) {
            update_free_space_in_dir_node(page_id, free_space, true);
         } else {
            return OP_RESULT::OK;
         }
      }
   }
}

u64 HeapFile::countEntries() {
   u64 count = 0;
   auto pages = table->numSlots();
   for (u32 pid = 0; pid < pages; ++pid) {
      BufferFrame* dirNode = table->getDirNode(pid);

      while (true) {
         u64 sum = 0;
         jumpmuTry()
         {
            HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
            Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[pid % kDirNodeEntryCount].ptr;
            if (heap_page_node.bf == nullptr) {
               jumpmu_break;
            }
            HybridPageGuard<HeapPage> target_s_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::SPIN);
            p_guard.recheck();
            for (std::size_t i = 0; i < target_s_guard->count; ++i) {
               if (target_s_guard->slotStatus(i) == SlotStatus::OCCUPIED) {
                  sum++;
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

OP_RESULT HeapFile::remove(HeapTupleId id) {
   while (true) {
      u32 page_id = id.heap_page_id();
      u32 slot_id = id.slot_id();
      BufferFrame * dirNode = table->getDirNode(page_id);
      u32 free_space = 0;
      bool deleted = false;
      bool jumpped = false;
      jumpmuTry()
      {
         HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
         Swip<HeapPage> & heap_page_node = p_guard->dataPagePointers[page_id % kDirNodeEntryCount].ptr;
         assert(heap_page_node.bf != nullptr);
         HybridPageGuard<HeapPage> target_x_guard(p_guard, heap_page_node, LATCH_FALLBACK_MODE::EXCLUSIVE);
         target_x_guard.toExclusive();
         if (target_x_guard->count > slot_id && target_x_guard->slotStatus(slot_id) == SlotStatus::OCCUPIED) {
            data_stored.fetch_sub(target_x_guard->getPayloadLength(slot_id));
            target_x_guard->removeSlot(slot_id);
            target_x_guard.incrementGSN();
            free_space = target_x_guard->freeSpace();
            deleted = true;
         }
      }
      jumpmuCatch() 
      {
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         jumpped = true;
      }

      if (!jumpped) {
         if (deleted && (free_space / (EFFECTIVE_PAGE_SIZE + 0.0)) >= getFreeSpaceThreshold() 
                  && (free_space / (EFFECTIVE_PAGE_SIZE + 0.0)) <= getFreeSpaceThreshold() + 0.2) {
            update_free_space_in_dir_node(page_id, free_space, false);
         }
         if (deleted) {
            return OP_RESULT::OK;
         } else {
            return OP_RESULT::NOT_FOUND;
         }
      }
   }
}

double HeapFile::current_load_factor() {
   return data_stored.load() / (getPages() * EFFECTIVE_PAGE_SIZE + 0.0);
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
   if (pages_that_might_have_space[this_thread_idx].data != std::numeric_limits<u64>::max()) {
      return pages_that_might_have_space[this_thread_idx].data;
   }
   mtx.lock_shared();
   if (pages_with_free_space.empty()) {
      mtx.unlock_shared();
   } else {
      u32 pid = pages_with_free_space[utils::RandomGenerator::getRand<u64>(0, pages_with_free_space.size())];
      pages_that_might_have_space[this_thread_idx].data = pid;
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
      pages_that_might_have_space[this_thread_idx].data = std::numeric_limits<u64>::max();
   }
}

void HeapFile::find_pages_with_free_space() {
   assert(pages_with_free_space.empty());
   // collect data pages that have space by scanning the directory pages
   // If could not find enough (N) such pages, let's add a few new pages using `add_heap_pages`.
   std::vector<u64> pages_that_have_space;
   static constexpr int kScanStep = N * 10;
   for (u64 pid = next_page_to_scan; pid < table->numSlots();) {
      BufferFrame* dirNode = table->getDirNode(pid);
      int dirNodeStartPageIdx = (pid / kDirNodeEntryCount) * kDirNodeEntryCount;
      int start_idx_within_node = pid % kDirNodeEntryCount;
      int end_idx_within_node = std::min(start_idx_within_node + kScanStep, kDirNodeEntryCount);
      while(true) {
         jumpmuTry() {
            pages_that_have_space.clear();
            HybridPageGuard<HeapDirectoryNode> p_guard(dirNode);
            for (int i = start_idx_within_node; i < end_idx_within_node; ++i) {
               if (p_guard->dataPagePointers[i].ptr.raw() &&
                  (p_guard->dataPagePointers[i].freeSpace / (EFFECTIVE_PAGE_SIZE + 0.0)) >= getFreeSpaceThreshold()) {
                  pages_that_have_space.push_back(dirNodeStartPageIdx + i);
               }
            }
            p_guard.recheck();
            pages_with_free_space.insert(pages_with_free_space.end(), pages_that_have_space.begin(), pages_that_have_space.end());
            jumpmu_break;
         } jumpmuCatch()
         {
            WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
         }
      }
      assert(end_idx_within_node - start_idx_within_node >= 1);
      pid += end_idx_within_node - start_idx_within_node;
      next_page_to_scan = pid % table->numSlots();  
      if (pages_with_free_space.size() >= N) {
         break;
      }
   }

   if (pages_with_free_space.size() < N && is_heap_growable() == true) {
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
               p_guard->dataPagePointers[p_guard->node_count].freeSpace = target_x_guard->freeSpace();
               p_guard->dataPagePointers[p_guard->node_count].ptr = target_x_guard.swip();
               p_guard->node_count++;
               pages_with_free_space.push_back(pid);
               p_guard.incrementGSN();
               this->data_pages++;
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
   DTRegistry::DTMeta hf_meta = {.iterate_children = HeapFile::iterateChildrenSwips,
                                    .find_parent = HeapFile::findParent,
                                    .is_btree_leaf = HeapFile::isBTreeLeaf,
                                    .check_space_utilization = HeapFile::checkSpaceUtilization,
                                    .checkpoint = HeapFile::checkpoint,
                                    .keep_in_memory = HeapFile::keepInMemory,
                                    .undo = HeapFile::undo,
                                    .todo = HeapFile::todo,
                                    .serialize = HeapFile::serialize,
                                    .deserialize = HeapFile::deserialize};
   return hf_meta;
}

OP_RESULT IndexedHeapFile::scanHeapPage(u16 key_length, u32 page_id, std::function<bool(const u8*, u16, const u8*, u16)> callback) {
   return heap_file.scanPage(page_id, [&](const u8* tuple, u16 tuple_length, HeapTupleId tid){
      assert(tuple_length >= key_length);
      const u8 * key = tuple;
      const u8 * value = tuple + key_length;
      u16 value_length = tuple_length - key_length;
      return callback(key, key_length, value, value_length);
   });
}

OP_RESULT IndexedHeapFile::scanAsc(u8* key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback) {
   HeapTupleId tuple_id;
   btree_index.scanAsc(key, key_length,
            [&](const u8 * key, u16 key_length, const u8 * value, u16 value_length) -> bool {
               assert(value_length == sizeof(HeapTupleId));
               tuple_id = *reinterpret_cast<const HeapTupleId*>(value);
               bool continue_scan = false;
               bool res = heap_file.lookup(tuple_id, [&](const u8 * tuple, u16 tuple_length){
                  const u8* key_ptr = tuple;
                  const u8* value_ptr = tuple + key_length;
                  auto value_length = tuple_length - key_length;
                  continue_scan = callback(key_ptr, key_length, value_ptr, value_length);
               }) == OP_RESULT::OK;
               assert(res);
               return continue_scan;
            },[](){});
   return OP_RESULT::OK;
}

OP_RESULT IndexedHeapFile::remove(u8* key, u16 key_length) {
   LockGuardProxy g(&lock_table, unfold(*(u64*)key));
   while(!g.write_lock());
   HeapTupleId tuple_id;
   bool found = false;
   auto btree_lookup_result = btree_index.lookup(key, key_length, [&](const u8 * payload, u16 payload_length){
      assert(payload_length == sizeof(HeapTupleId));
      found = true;
      tuple_id = *reinterpret_cast<const HeapTupleId*>(payload);
   });

   if (btree_lookup_result == leanstore::storage::btree::OP_RESULT::OK) {
      assert(found == true);
      btree_lookup_result = btree_index.remove(key, key_length);
      assert(btree_lookup_result == leanstore::storage::btree::OP_RESULT::OK);
      auto res = heap_file.remove(tuple_id);
      assert(res == OP_RESULT::OK);
      return OP_RESULT::OK;
   }

   return OP_RESULT::NOT_FOUND;
}

OP_RESULT IndexedHeapFile::upsert(u8* key, u16 key_length, u8* value, u16 value_length) {
   LockGuardProxy g(&lock_table, unfold(*(u64*)key));
   while(!g.write_lock());
   bool exists = false;
   auto res = doLookupForUpdate(key, key_length, [&](u8 * tuple, u16 tuple_length){
      assert(tuple_length == value_length);
      exists = true;
      memcpy(tuple, value, value_length);
      return true;
   });
   if (res == OP_RESULT::OK) {
      assert(exists == true);
      return OP_RESULT::OK;
   }
   assert(res == OP_RESULT::NOT_FOUND);
   return doInsert(key, key_length, value, value_length);
}


OP_RESULT IndexedHeapFile::doInsert(u8* key, u16 key_length, u8* value, u16 value_length) {
   HeapTupleId tuple_id;
   u8 buffer[value_length + key_length];
   memcpy(buffer, key, key_length);
   memcpy(buffer + key_length, value, value_length);
   
   OP_RESULT res = heap_file.insert(tuple_id, buffer, key_length + value_length);
   assert(res == OP_RESULT::OK);
   leanstore::storage::btree::OP_RESULT btree_res = btree_index.insert(key, key_length, (u8*)&tuple_id, sizeof(tuple_id));
   assert(btree_res == leanstore::storage::btree::OP_RESULT::OK);
   return OP_RESULT::OK;
}

OP_RESULT IndexedHeapFile::insert(u8* key, u16 key_length, u8* value, u16 value_length) {
   LockGuardProxy g(&lock_table, unfold(*(u64*)key));
   while(!g.write_lock());
   return doInsert(key, key_length, value, value_length);
}

OP_RESULT IndexedHeapFile::doLookupForUpdate(u8* key, u16 key_length, function<bool(u8*, u16)> payload_callback) {
   bool found = false;
   bool found_in_heap = false;
   HeapTupleId tuple_id;
   auto btree_lookup_result = btree_index.lookup(key, key_length, [&](const u8 * payload, u16 payload_length){
      assert(payload_length == sizeof(HeapTupleId));
      found = true;
      tuple_id = *reinterpret_cast<const HeapTupleId*>(payload);
   });
   
   if (btree_lookup_result == leanstore::storage::btree::OP_RESULT::OK) {
      auto res = heap_file.lookupForUpdate(tuple_id, [&](u8 * tuple, u16 tuple_length){
         found_in_heap = true;
         auto value_length = tuple_length - key_length;
         return payload_callback(tuple + key_length, value_length);
      });
      if (found_in_heap) {
         assert(res == OP_RESULT::OK);
         return OP_RESULT::OK;
      } else {
         return OP_RESULT::NOT_FOUND;
      }
      
   } else {
      assert(btree_lookup_result == leanstore::storage::btree::OP_RESULT::NOT_FOUND);
      return OP_RESULT::NOT_FOUND;
   }
}

OP_RESULT IndexedHeapFile::lookupForUpdate(u8* key, u16 key_length, function<bool(u8*, u16)> payload_callback) {
   LockGuardProxy g(&lock_table, unfold(*(u64*)key));
   while(!g.write_lock());
   return doLookupForUpdate(key, key_length, payload_callback);
}


OP_RESULT IndexedHeapFile::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) {
   while (true) {
      LockGuardProxy g(&lock_table, unfold(*(u64*)key));
      if(!g.read_lock()){
         continue;
      }
      bool found_in_heap = false;
      auto btree_lookup_result = btree_index.lookup(key, key_length, [&](const u8 * payload, u16 payload_length){
         assert(payload_length == sizeof(HeapTupleId));
         HeapTupleId tuple_id = *reinterpret_cast<const HeapTupleId*>(payload);
         heap_file.lookup(tuple_id, [&](const u8 * tuple, u16 tuple_length){
            found_in_heap = true;
            auto value_length = tuple_length - key_length;
            payload_callback(tuple + key_length, value_length);
         });
      });

      if (!g.validate()) {
         continue;
      }
      if (btree_lookup_result == leanstore::storage::btree::OP_RESULT::NOT_FOUND) {
         return OP_RESULT::NOT_FOUND;
      }

      if (found_in_heap) {
         return OP_RESULT::OK;
      }
      return OP_RESULT::NOT_FOUND;
   }
}


static std::size_t btree_entries(leanstore::storage::btree::BTreeInterface& btree, std::size_t & pages) {
   constexpr std::size_t scan_buffer_cap = 64;
   size_t scan_buffer_len = 0;
   typedef u64 Key;

   Key keys[scan_buffer_cap];
   bool tree_end = false;
   pages = 0;
   const char * last_leaf_frame = nullptr;
   u8 key_bytes[sizeof(Key)];
   auto fill_scan_buffer = [&](Key startk) {
      if (scan_buffer_len < scan_buffer_cap && tree_end == false) {
         btree.scanAsc(key_bytes, fold(key_bytes, startk),
         [&](const u8 * key, u16 key_length, [[maybe_unused]] const u8 * value, [[maybe_unused]] u16 value_length, const char * leaf_frame) -> bool {
            auto real_key = unfold(*(Key*)(key));
            assert(key_length == sizeof(Key));
            keys[scan_buffer_len] = real_key;
            scan_buffer_len++;
            if (last_leaf_frame != leaf_frame) {
               last_leaf_frame = leaf_frame;
               ++pages;
            }
            if (scan_buffer_len >= scan_buffer_cap) {
               return false;
            }
            return true;
         }, [](){});
         if (scan_buffer_len < scan_buffer_cap) {
            tree_end = true;
         }
      }
   };
   Key start_key = std::numeric_limits<Key>::min();
   
   std::size_t entries = 0;
   
   while (true) {
      fill_scan_buffer(start_key);
      size_t idx = 0;
      while (idx < scan_buffer_len) {
         idx++;
         entries++;
      }

      if (idx >= scan_buffer_len && tree_end) {
         break;
      }
      assert(idx > 0);
      start_key = keys[idx - 1] + 1;
      scan_buffer_len = 0;
   }
   return entries;
}

u64 IndexedHeapFile::countHeapPages() {
   return heap_file.countPages();
}

u64 IndexedHeapFile::countHeapEntries() {
   return heap_file.countEntries();
}

u64 IndexedHeapFile::countIndexPages() {
   size_t pages = 0;
   btree_entries(btree_index, pages);
   return pages;
}

u64 IndexedHeapFile::countIndexEntries(){
   size_t pages = 0;
   return btree_entries(btree_index, pages);
}

}
}
}