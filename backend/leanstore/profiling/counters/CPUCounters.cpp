#include "CPUCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
std::mutex CPUCounters::mutex;
u64 CPUCounters::id = 0;
std::unordered_map<u64, CPUCounters> CPUCounters::threads;
// -------------------------------------------------------------------------------------
u64 CPUCounters::registerThread(string name, bool perf_inherit, bool enable_perf_profiling)
{
   std::unique_lock guard(mutex);
   threads[id] = {.e = std::make_unique<PerfEvent>(perf_inherit, enable_perf_profiling), .name = name};
   return id++;
}
void CPUCounters::removeThread(u64 id)
{
   std::unique_lock guard(mutex);
   threads.erase(id);
}
}  // namespace leanstore
