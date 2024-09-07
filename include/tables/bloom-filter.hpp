#include <stdint.h>

#include <bitset>
#include <cmath>
#include <utils/hashing.hpp>

namespace parser {

template <uint64_t m>
struct BloomFilter {
 public:
  BloomFilter(uint64_t n_) : n(n_) { k = m / n_ * log(2); }

  void insert(uint64_t key) {
    for (uint64_t i = 0; i < k; i++) {
      uint64_t h = hash_key(i);
      bit_array.set(h);
    }
  }

  bool filter(uint64_t key) {
    for (uint64_t i = 0; i < k; i++) {
      uint64_t h = hash_key(i);
      if (!bit_array.test(h)) {
        return false;
      }
      return true;
    }
  }

 private:
  std::bitset<m> bit_array;
  uint64_t k;
  uint64_t n;

  uint64_t hash_key(uint64_t i) {
    uint64_t h1 = MurmurHash64A_1(key);
    uint64_t h2 = MurmurHash64A_2(key);

    return (h1 + i * h2) % m;
  }
};

}  // namespace parser