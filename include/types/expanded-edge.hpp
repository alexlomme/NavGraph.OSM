#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/node.hpp>

namespace parser {

struct ExpandedEdge {
  google::protobuf::int64 id;
  // google::protobuf::int64 source;
  // google::protobuf::int64 target;
  parser::Node* sourceEdgeSourceNode;
  parser::Node* sourceEdgeTargetNode;
  parser::Node* targetEdgeSourceNode;
  parser::Node* targetEdgeTargetNode;
  double cost;
};

}  // namespace parser