#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
class FNV
{
  private:
   static constexpr u64 FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
   static constexpr u64 FNV_PRIME_64 = 1099511628211L;

  public:
   static u64 hash(u64 val);
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
