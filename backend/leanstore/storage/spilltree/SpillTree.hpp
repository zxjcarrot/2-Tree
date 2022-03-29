#pragma once
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include "shared-headers/Units.hpp"
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
namespace spilltree
{

enum class LeafType {InMemory, Spilled, Hybrid};

struct NodeBase {
    LeafType type;
    u64 BlockId;
};

struct HotLeaf {
    
};

struct WarmLeaf {

};

}
}
}