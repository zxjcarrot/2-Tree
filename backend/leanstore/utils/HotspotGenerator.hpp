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
class HotspotGenerator: public Generator
{
  public:
    u64 min, max;
    double hotset_fraction;
    double hot_op_fraction;
    u64 hot_interval;
    u64 cold_interval;
   /* Create a generator for Hotspot distributions.
   *
   * @param min lower bound of the distribution.
   * @param max upper bound of the distribution.
   * @param hotset_fraction percentage of data item
   * @param hot_op_fraction percentage of operations accessing the hot set.
   */
   HotspotGenerator(u64 min, u64 max, double hotset_fraction, double hot_op_fraction = 1.0) : min(min), max(max), hotset_fraction(hotset_fraction), hot_op_fraction(hot_op_fraction) {
    if (this->hotset_fraction < 0.0 || this->hotset_fraction > 1.0) {
      std::cerr << "Hotset fraction out of range. Setting to 1.0";
      this->hotset_fraction = 1.0;
    }
    if (this->hot_op_fraction < 0.0 || this->hot_op_fraction > 1.0) {
      std::cerr << "Hot operation fraction out of range. Setting to 1.0";
      this->hot_op_fraction = 1.0;
    }

    if (min > max) {
      std::cerr << "Upper bound of Hotspot generator smaller than the lower bound. Swapping the values.";
      std::swap(this->min, this->max);
    }
    u64 interval = max - min + 1;
    this->hot_interval = (int) (interval * this->hotset_fraction);
    this->cold_interval = interval - hot_interval;
   }

   u64 rand() override {
      u64 value = 0;
      double constant = 1000000000000000000.0;
      u64 i = RandomGenerator::getRandU64(0, 1000000000000000001);
      double u = static_cast<double>(i) / constant;
      if (u < hot_op_fraction) {
        // Choose a value from the hot set.
        value = min + RandomGenerator::getRandU64(0, 1000000000000000000) % hot_interval;
      } else {
        // Choose a value from the cold set.
        value = min + hot_interval + RandomGenerator::getRandU64(0, 1000000000000000000) % cold_interval;
      }
      return value;
   }
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
