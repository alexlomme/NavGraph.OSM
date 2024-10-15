#pragma once

#include <unistd.h>

namespace ngosm {
namespace hashpart {
namespace types {

struct NodeData {
  uint64_t offset;
  double lat;
  double lon;
  uint64_t used;
};

}  // namespace types
}  // namespace hashpart
}  // namespace ngosm