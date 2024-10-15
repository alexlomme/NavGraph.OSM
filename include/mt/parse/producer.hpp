#pragma once

#include <arpa/inet.h>
#include <osmpbf/fileformat.pb.h>
#include <unistd.h>

#include <cstring>
#include <mt/mutexes.hpp>

namespace ngosm {
namespace mt {
namespace parse {
void producer(const char*& pbf_data, const char* eof_pbf,
              ngosm::mt::Mutexes& mutexes) {
  while (pbf_data < eof_pbf) {
    uint32_t headerSize;
    std::memcpy(&headerSize, pbf_data, sizeof(headerSize));
    headerSize = ntohl(headerSize);

    pbf_data += sizeof(headerSize);

    OSMPBF::BlobHeader header;
    if (!header.ParseFromArray(pbf_data, headerSize)) {
      throw std::runtime_error("");
    }

    pbf_data += headerSize;

    if (header.type() != "OSMData") {
      pbf_data += header.datasize();
      continue;
    }

    {
      std::unique_lock<std::mutex> lock(mutexes.queue_mutex);
      mutexes.tuple_queue.push(std::make_tuple(pbf_data, header.datasize()));
    }

    pbf_data += header.datasize();
  }

  mutexes.done = true;
  mutexes.cv.notify_all();
}
}  // namespace parse
}  // namespace mt
}  // namespace ngosm