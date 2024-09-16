#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/node.hpp>
#include <types/way.hpp>

namespace parser {
struct Edge {
  google::protobuf::int64 id;
  google::protobuf::int64 wayId;
  uint64_t wayOffset;
  google::protobuf::int64 sourceNodeId;
  uint64_t sourceNodeOffset;
  google::protobuf::int64 targetNodeId;
  uint64_t targetNodeOffset;
  double cost;
  long partition;
};
}  // namespace parser