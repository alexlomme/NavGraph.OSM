#pragma once
#include <sstream>
#include <string>

std::string numFilename(const std::string& filename, uint64_t num,
                        std::string ext) {
  std::ostringstream stream;
  stream << filename << std::to_string(num) << "." << ext;
  return stream.str();
}

namespace ngosm {
namespace buf {
namespace parse {
inline const uint64_t MAX_NODE_CLUSTER_SIZE = 64'000'000;
inline const uint64_t MAX_WAY_CLUSTER_SIZE = 4'800'000;
inline const uint64_t MAX_WAY_NODES_CLUSTER_SIZE = 57'600'000;
inline const uint64_t MAX_USED_NODES_CLUSTER_SIZE = 57'600'000;
inline const uint64_t MAX_RESTRICTIONS_CLUSTER_SIZE = 235'930;
// inline const uint64_t MAX_EDGES_CLUSTER_SIZE = 1'638'400;

}  // namespace parse
}  // namespace buf
}  // namespace ngosm