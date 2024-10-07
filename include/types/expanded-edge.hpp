#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/node.hpp>

namespace parser {

struct ExpandedEdge {
  google::protobuf::int64 id;
  google::protobuf::int64 sourceEdgeId;
  uint64_t sourceEdgeOffset;
  google::protobuf::int64 targetEdgeId;
  uint64_t targetEdgeOffset;
  double cost;
  long partition;
};

}  // namespace parser