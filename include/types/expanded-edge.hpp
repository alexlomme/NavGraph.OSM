#pragma once

#include <osmpbf/osmformat.pb.h>

namespace parser {

struct ExpandedEdge {
  google::protobuf::int64 id;
  google::protobuf::int64 source;
  google::protobuf::int64 target;
  double cost;
};

}