#include "FNVHash.hpp"
#include "Units.hpp"
#include "ZipfGenerator.hpp"
#include "RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
class ScrambledZipfGenerator: public Generator
{
  public:
   u64 min, max, n;
   double theta;
   ZipfGenerator zipf_generator;
   // 10000000000ul
   // [min, max)
   ScrambledZipfGenerator(u64 min, u64 max, double theta) : min(min), max(max), n(max - min), zipf_generator((max - min), theta) {}
   u64 rand() override;
   u64 rand(u64);
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
