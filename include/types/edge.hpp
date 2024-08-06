#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/node.hpp>
#include <types/way.hpp>

namespace parser {
struct Edge {
  google::protobuf::int64 id;
  parser::Way* wayPtr;
  parser::Node* sourceNodePtr;
  parser::Node* targetNodePtr;
  double cost;
};
}  // namespace parser