#include <bitset>
#include <functional>
#include <iostream>
#include <vector>

class BloomFilter {
 public:
  BloomFilter(size_t size, int numHashes)
      : bitArray(size), numHashes(numHashes), size(size) {}

  // Insert an item into the Bloom filter
  void insert(const uint64_t item) {
    for (int i = 0; i < numHashes; ++i) {
      size_t hashValue = hash(item, i);
      bitArray[hashValue % size] = true;
    }
  }

  // Check if an item is in the Bloom filter
  bool contains(const uint64_t item) const {
    for (int i = 0; i < numHashes; ++i) {
      size_t hashValue = hash(item, i);
      if (!bitArray[hashValue % size]) {
        return false;
      }
    }
    return true;
  }

 private:
  std::vector<bool> bitArray;
  int numHashes;
  size_t size;

  // Combine hash functions using std::hash and different seeds
  size_t hash(const uint64_t item, int i) const {
    return std::hash<uint64_t>{}(item) ^ (std::hash<int>{}(i) << 1);
  }
};