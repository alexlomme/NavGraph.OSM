#pragma once

#include "osmpbf/osmformat.pb.h"

namespace parser {
struct Node {
  google::protobuf::int64 id;
  double lat;
  double lon;
  uint64_t used;

  Node(google::protobuf::int64 id, double lat, double lon, uint64_t used)
      : id(id), lat(lat), lon(lon), used(used) {}
};
}  // namespace parser