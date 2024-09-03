#pragma once

#include <osmpbf/osmformat.pb.h>
#include <stdint.h>

#include <vector>

namespace parser {
struct BorderWay {
  google::protobuf::int64 id;
  bool oneway;

  std::vector<std::pair<uint32_t, std::vector<google::protobuf::int64>>> nodes;

  BorderWay(google::protobuf::int64 id, bool oneway) : id(id), oneway(oneway) {}
};
}  // namespace parser