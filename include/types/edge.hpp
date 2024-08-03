#pragma once

#include <osmpbf/osmformat.pb.h>

namespace parser {
struct Edge {
  google::protobuf::int64 id;
  google::protobuf::int64 wayId;
  google::protobuf::int64 sourceNode;
  google::protobuf::int64 targetNode;
  double cost;
};
}  // namespace parser