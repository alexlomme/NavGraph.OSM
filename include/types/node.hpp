#pragma once

#include "osmpbf/osmformat.pb.h"

namespace parser {
struct Node {
  google::protobuf::int64 id;
  double lat;
  double lon;
  uint64_t used;
};
}  // namespace parser