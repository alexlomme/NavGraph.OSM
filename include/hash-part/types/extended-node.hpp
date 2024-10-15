#pragma once

#include <osmpbf/osmformat.pb.h>
#include <unistd.h>

namespace ngosm {
namespace hashpart {
namespace types {

struct ExtendedNode {
  google::protobuf::int64 id;
  double lat;
  double lon;
  uint64_t used;
  uint64_t offset;
};

}  // namespace types
}  // namespace hashpart
}  // namespace ngosm