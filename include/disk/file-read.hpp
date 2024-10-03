#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stdexcept>

struct FileRead {
 public:
  FileRead(int fd) : fd(fd) {
    if (fd == -1) {
      throw std::runtime_error("Failed opening file for reading");
    }
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
      throw std::runtime_error("Failed obtaning file size");
    }
    size = sb.st_size;
  }

  void* mmap_file() {
    void* map_temp = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map_temp == MAP_FAILED) {
      std::cerr << size << std::endl;
      throw std::runtime_error("Failed mapping");
    };

    map = map_temp;
    return map;
  }

  void unmap_file() {
    if (munmap(map, size) == -1) {
      throw std::runtime_error("Failed unmapping file for reading");
    }
    map = (void*)-1;
  }

  size_t fsize() { return size; }

  void close_fd() { close(fd); }

  const char* eof() { return static_cast<const char*>(map) + size; }

 private:
  int fd;
  void* map;
  size_t size;
};