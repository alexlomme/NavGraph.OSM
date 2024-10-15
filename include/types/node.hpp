#pragma once

#include "osmpbf/osmformat.pb.h"

namespace ngosm {
namespace types {
struct Node {
  google::protobuf::int64 id;
  double lat;
  double lon;

  Node(google::protobuf::int64 id, double lat, double lon)
      : id(id), lat(lat), lon(lon) {}
};
}  // namespace types
}  // namespace ngosm