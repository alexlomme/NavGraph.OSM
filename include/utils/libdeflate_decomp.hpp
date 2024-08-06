#pragma once

#include <libdeflate.h>

#include <iostream>
#include <string>

bool ldeflate_decompress(std::string data, unsigned char* buf, size_t rawSize) {
  libdeflate_decompressor* decompressor = libdeflate_alloc_decompressor();

  size_t actual_decompressed_size;
  libdeflate_result result =
      libdeflate_zlib_decompress(decompressor, data.data(), data.capacity(),
                                 buf, rawSize, &actual_decompressed_size);

  if (result != LIBDEFLATE_SUCCESS) {
    throw std::runtime_error("Decompression failed\n");
  }

  libdeflate_free_decompressor(decompressor);

  return true;
}