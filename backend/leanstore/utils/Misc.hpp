#pragma once
#include "Units.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <chrono>
#include <cmath>
#include <functional>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
u32 getBitsNeeded(u64 input);
// -------------------------------------------------------------------------------------
double calculateMTPS(std::chrono::high_resolution_clock::time_point begin, std::chrono::high_resolution_clock::time_point end, u64 factor);
// -------------------------------------------------------------------------------------
void pinThisThreadRome();
void pinThisThreadRome(const u64 t_i);
void pinThisThread(const u64 t_i);
// -------------------------------------------------------------------------------------
void printBackTrace();
// -------------------------------------------------------------------------------------
inline u64 upAlign(u64 x)
{
   return (x + 511) & ~511ul;
}

inline u64 upAlignY(u64 x, u64 y)
{
   return (x + (y-1)) & ~(y-1);
}

inline u64 upAlign4K(u64 x)
{
   return upAlignY(x, 4096);
}



// -------------------------------------------------------------------------------------
inline u64 downAlign(u64 x)
{
   return x - (x & 511);
}

inline u64 downAlignY(u64 x, u64 y)
{
   return x - (x & (y-1));
}
inline u64 downAlign4K(u64 x)
{
   return downAlignY(x, 4096);
}


// -------------------------------------------------------------------------------------
u32 CRC(const u8* src, u64 size);
// -------------------------------------------------------------------------------------

class IOScopedCounter {
public:
   IOScopedCounter(std::function<void(u64)> f):  f(f) {
      io_reads_snapshot = WorkerCounters::myCounters().io_reads.load();
   }
   ~IOScopedCounter() {
      f(io_reads_snapshot = WorkerCounters::myCounters().io_reads.load() - io_reads_snapshot);
   }
private:
   u64 io_reads_snapshot = 0;
   std::function<void(u64)> f;
};
}  // namespace utils
}  // namespace leanstore
