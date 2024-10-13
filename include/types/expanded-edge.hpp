#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/node.hpp>

namespace parser {

struct ExpandedEdge {
  google::protobuf::int64 id;
  long sourcePart;
  uint64_t sourceEdgeOffset;
  long targetPart;
  uint64_t targetEdgeOffset;
  double cost;
};

}  // namespace parser