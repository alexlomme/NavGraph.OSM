#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stdexcept>
#include <vector>

template <typename T>
struct FileWrite {
 public:
  FileWrite(int fd, uint64_t maxChunkSize)
      : fd(fd), maxChunkSize(maxChunkSize), size(0) {
    if (fd == -1) {
      throw std::runtime_error("Failed opening file");
    }
    buffer.reserve(maxChunkSize);
  }

  void add(T obj) {
    buffer.push_back(obj);
    if (buffer.size() * sizeof(T) >= maxChunkSize) {
      flush();
    }
  }

  void flush() {
    size += sizeof(T) * buffer.size();

    if (ftruncate(fd, size) == -1) {
      throw std::runtime_error("Failed to truncate memory");
    }
    void* map = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
      throw std::runtime_error("Failed mapping a file");
    }
    std::memcpy(static_cast<char*>(map) + size - sizeof(T) * buffer.size(),
                buffer.data(), sizeof(T) * buffer.size());
    if (munmap(map, size) == -1) {
      throw std::runtime_error("Failed to unmap file");
    }
    buffer.clear();
  }

  uint64_t sizeAfterFlush() { return size + sizeof(T) * buffer.size(); }

  uint64_t fsize() { return size; }

  void close_fd() { close(fd); }

 private:
  int fd;
  uint64_t maxChunkSize;
  std::vector<T> buffer;
  uint64_t size;
};