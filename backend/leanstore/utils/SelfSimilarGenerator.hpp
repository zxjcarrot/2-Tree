#include "FNVHash.hpp"
#include "Units.hpp"
#include "selfsimilar_int_distribution.h"
#include "RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
class SelfSimilarGenerator: public Generator
{
  public:
    std::default_random_engine generator;
    selfsimilar_int_distribution<u64> distribution;
   /* Create a generator for Hotspot distributions.
   *
   * @param min lower bound of the distribution.
   * @param max upper bound of the distribution.
   * @param hotset_fraction percentage of data item
   * @param hot_op_fraction percentage of operations accessing the hot set.
   */
   SelfSimilarGenerator(u64 min, u64 max, double skew = 0.2) : distribution(min, max, skew) {
    
   }

   u64 rand() override {
      return distribution(generator);
   }

   u64 rand(u64) override {
      return distribution(generator);
   }
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
