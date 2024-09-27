#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stdexcept>
#include <string>
#include <tables/ska/flat_hash_map.hpp>
#include <unordered_map>
#include <vector>

template <typename K, typename V>
struct KeyFileClusterWrite {
 public:
  KeyFileClusterWrite(std::vector<std::pair<K, int>>& file_fds,
                      uint64_t maxBufSizesSum)
      : maxBufSizesSum(maxBufSizesSum) {
    for (const auto& [key, fd] : file_fds) {
      if (fd == -1) {
        throw std::runtime_error("Failed opening file");
      }
      filesMap.emplace(key, std::make_pair(fd, 0));
      buffers.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                      std::forward_as_tuple());
    }
    maxBufSize = 0;
    bufSizesSum = 0;
  }

  void add(K key, V val) {
    const auto bufIt = buffers.find(key);
    if (bufIt == buffers.end()) {
      throw std::runtime_error("Key is not in key-set");
    }
    bufIt->second.push_back(val);
    bufSizesSum++;

    if (bufIt->second.size() > maxBufSize) {
      maxBufSize = bufIt->second.size();
      maxBufKey = key;
    }

    if (bufSizesSum >= maxBufSize) {
      auto fileIt = filesMap.find(maxBufKey);
      const auto maxBufIt = buffers.find(maxBufKey);

      if (fileIt == filesMap.end() || maxBufIt == buffers.end()) {
        throw std::runtime_error("Buffer or file not found for max key");
      }

      fileIt->second.second += sizeof(V) * maxBufIt->second.size();

      if (ftruncate(fileIt->second.first, fileIt->second.second) == -1) {
        throw std::runtime_error("Buffer or file not found for max key");
      }
      void* map = mmap(nullptr, fileIt->second.second, PROT_READ | PROT_WRITE,
                       MAP_SHARED, fileIt->second.first, 0);

      if (map == MAP_FAILED) {
        throw std::runtime_error("failed mapping in new chunk");
      }
      std::memcpy(static_cast<char*>(map) + fileIt->second.second -
                      sizeof(V) * maxBufIt->second.size(),
                  maxBufIt->second.data(), sizeof(V) * maxBufIt->second.size());
      if (munmap(map, fileIt->second.second) == -1) {
        throw std::runtime_error("Failed to unmap ways file");
      }
      bufSizesSum -= maxBufIt->second.size();
      maxBufIt->second.clear();

      maxBufSize = -1;

      for (const auto& [key, buf] : buffers) {
        if (buf.size() >= maxBufSize) {
          maxBufSize = buf.size();
          maxBufKey = key;
        }
      }
    }
  }

  void flush() {
    for (auto& [key, buf] : buffers) {
      if (buf.size() == 0) {
        continue;
      }

      auto fileIt = filesMap.find(key);

      if (fileIt == filesMap.end()) {
        throw std::runtime_error("No file found for buffer");
      }

      fileIt->second.second += sizeof(V) * buf.size();

      if (ftruncate(fileIt->second.first, fileIt->second.second) == -1) {
        throw std::runtime_error("failed to alloc memory for nodes (2)");
      }
      void* map = mmap(nullptr, fileIt->second.second, PROT_READ | PROT_WRITE,
                       MAP_SHARED, fileIt->second.first, 0);

      if (map == MAP_FAILED) {
        throw std::runtime_error("failed mapping flush");
      }
      std::memcpy(static_cast<char*>(map) + fileIt->second.second -
                      sizeof(V) * buf.size(),
                  buf.data(), sizeof(V) * buf.size());
      if (munmap(map, fileIt->second.second) == -1) {
        throw std::runtime_error("Failed to unmap ways file");
      }
      buf.clear();
    }
  }

  void close_fds() {
    for (const auto& [_, pair] : filesMap) {
      close(pair.first);
    }
  }

 private:
  ska::flat_hash_map<K, std::pair<int, size_t>> filesMap;
  std::unordered_map<K, std::vector<V>> buffers;

  K maxBufKey;
  int64_t maxBufSize;
  uint64_t bufSizesSum;
  uint64_t maxBufSizesSum;
};