#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/node.hpp>
#include <types/way.hpp>

namespace parser {
struct Edge {
  google::protobuf::int64 id;
  google::protobuf::int64 wayId;
  bool wasOneWay;
  long part;
  uint64_t geomOffset;
  int64_t geomSize;
  google::protobuf::int64 sourceNodeId;
  uint64_t sourceNodeOffset;
  google::protobuf::int64 targetNodeId;
  uint64_t targetNodeOffset;
  double cost;
};
}  // namespace parser