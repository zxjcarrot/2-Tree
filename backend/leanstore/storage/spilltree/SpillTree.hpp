#pragma once
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include "shared-headers/Units.hpp"
// -------------------------------------------------------------------------------------
#include <signal.h>
#include <atomic>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------

namespace leanstore
{
namespace storage
{
namespace spilltree
{

enum class LeafType {InMemory, Spilled, Hybrid};

enum class SpillTreeOpResult {Good, NotExist, Duplicate};
struct NodeBase {
    std::atomic<uint64_t> version;
    LeafType type;
    u16 count;
};
template<typename Key, typename Value>
class SpillTree {
private:
constexpr static int kMaxEntries = 50;
struct Leaf : NodeBase {
    // TODO : Block identifier that points to on-disk location for spilled entries in the leave
    // or a in-memory pointer to the buffered spilled data.
    u64 block_id;
    u32 array_capacity;
    u32 array_size;
    Key* key_array;
    Value* value_array;
    
    const Key & get_key(unsigned idx) const {
        return key_array[idx];
    }

    const Value & get_value(unsigned idx) const {
        return value_array[idx];
    }

    Key & get_key(unsigned idx) {
        return key_array[idx];
    }

    Value & get_value(unsigned idx) {
        return value_array[idx];
    }

    static unsigned 
    lower_bound(const Key & k) {
        unsigned lower = 0;
        unsigned upper = this->count;
        do {
            unsigned mid = ((upper - lower) / 2) + lower;
            // This is the key at the pivot position
            const Key &middle_key = get_key(mid);
            if (k < middle_key) {
                upper = mid;
            } else if (middle_key < k) {
                lower = mid + 1;
            } else {
                lower = mid;
                break;
            }
        } while (lower < upper);
        return lower;
    }

    static SpillTreeOpResult
    update(Key k, Value p, std::function<bool(const void *)> &predicate) {
        assert(this->count < kMaxEntries);
        unsigned pos = lowerBound(k);
        if ((pos < this->count)) {
            const Key & ck = get_key(pos);
            Value & v = get_value(pos);
            if (ck == k) {
                bool predicated_true = predicate((const void *) &v) == false;
                if (predicated_true == false) {
                    v = p;
                    return SpillTreeOpResult::Good;
                }
            }
        }
        return SpillTreeOpResult::NotExist;
    }

    static SpillTreeOpResult
    insert(Key k, Value p, PageAccessor &accessor) {
        assert(this->count < kMaxEntries);
        if (this->count) {
            unsigned pos = lower_bound(k);
            if ((pos < this->count)) {
                const Key & ck = get_key(pos);
                if (ck->first == k) {
                    return SpillTreeOpResult::Duplicate;
                }
            }
            int len = this_node_base.count - pos + 1;
            Key *key_pos = &key_array[pos];
            Key *value_pos = &value_array[pos];
            memmove(key_pos + 1, key_pos, sizeof(Key) * (this->count - pos));
            memmove(value_pos + 1, value_pos, sizeof(Value) * (this->count - pos));
            key_pos[0] = k;
            value_pos[0] = p;
        } else {
            key_array[0] = k;
            value_array[0] = p;
        }
        this->count++;
        return SpillTreeOpResult::Good;
    }

    static BTreeOPResult
    upsert(Key k, Value p, PageAccessor &accessor, std::function<bool(const void *)> &predicate) {
        auto this_node_base = NodeBase::GetNodeBase(accessor);
        assert(this_node_base.count < kMaxEntries);
        if (this_node_base.count) {
            unsigned pos = lowerBound(k, accessor, this_node_base);
            if ((pos < this_node_base.count)) {
                auto kv = RefKeyValue(pos, accessor);
                if (kv->first == k) {
                    bool predicated_true = predicate((const void *) &kv->second) == false;
                    if (predicated_true == false) {
                        kv->second = p;
                        accessor.FinishAccess();
                        return BTreeOPResult::UPDATED;
                    } else {
                        accessor.FinishAccess();
                        return BTreeOPResult::FAILED;
                    }
                }
            }
            int len = this_node_base.count - pos + 1;
            KeyValueType *data_pos = RefKeyValues(pos, len, accessor);
            memmove(data_pos + 1, data_pos, sizeof(KeyValueType) * (this_node_base.count - pos));
            data_pos[0] = std::make_pair(k, p);
        } else {
            *RefKeyValue(0, accessor) = std::make_pair(k, p);
        }
        NodeBase::RefNodeBase(accessor)->count++;
        accessor.FinishAccess();
        return BTreeOPResult::SUCCESS;
    }

};

};


}
}
}