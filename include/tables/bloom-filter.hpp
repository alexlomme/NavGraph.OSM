#include <stdint.h>

#include <bitset>
#include <cmath>
#include <iostream>
#include <utils/hashing.hpp>
#include <vector>

namespace parser {

struct BloomFilter {
 public:
  BloomFilter(uint64_t n_, float p) : n(n_) {
    m = std::ceil(-(log(p) * n) / pow(log(2), 2));
    k = m / n_ * log(2);
    // std::cerr << n << std::endl;
    // std::cerr << p << std::endl;
    // std::cerr << pow(log(2), 2) << std::endl;
    bit_array.resize((uint64_t)std::floor(m / 64) + 1);
    // std::cerr << "After allocation" << std::endl;
  }

  void insert(uint64_t key) {
    for (uint64_t i = 0; i < k; i++) {
      uint64_t h = hash_key(i, key);
      uint64_t idx = std::floor(h / 64);
      uint64_t pos = h % 64;
      bit_array[idx] |= (1ull << (63 - pos));
    }
  }

  bool filter(uint64_t key) {
    for (uint64_t i = 0; i < k; i++) {
      uint64_t h = hash_key(i, key);
      uint64_t idx = std::floor(h / 64);
      uint64_t pos = h % 64;
      if (bit_array[idx] >> (63 - pos) & 1ull != 1) {
        return false;
      }
      //   if (!bit_array.test(h)) {
      //     return false;
      //   }

      return true;
    }
  }

 private:
  std::vector<uint64_t> bit_array;
  uint64_t k;
  uint64_t n;
  uint64_t m;

  uint64_t hash_key(uint64_t i, uint64_t key) {
    uint64_t h1 = MurmurHash64A_1(key);
    // uint64_t h2 = MurmurHash64A_2(key);
    uint64_t h2 = std::hash<uint64_t>()(key);
    return (h1 + i * h2) % m;
  }
};

}  // namespace parser