#pragma once

#include <osmpbf/osmformat.pb.h>

namespace parser {

struct Way {
  Way(google::protobuf::int64 id, std::vector<google::protobuf::int64> nodes,
      bool oneway)
      : id(id), nodes(nodes), oneway(oneway) {}

  google::protobuf::int64 id;
  std::vector<google::protobuf::int64> nodes;
  bool oneway;
};

}