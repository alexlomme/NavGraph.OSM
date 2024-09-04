#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/border-way.hpp>
#include <types/node.hpp>

namespace parser {
struct BorderEdge {
  google::protobuf::int64 id;
  parser::BorderWay* wayPtr;
  parser::Node* sourceNodePtr;
  parser::Node* targetNodePtr;
  double cost;

  BorderEdge(google::protobuf::int64 id, parser::BorderWay* wayPtr,
             parser::Node* sourceNodePtr, parser::Node* targetNodePtr,
             double cost)
      : id(id),
        wayPtr(wayPtr),
        sourceNodePtr(sourceNodePtr),
        targetNodePtr(targetNodePtr),
        cost(cost) {}
};
}  // namespace parser