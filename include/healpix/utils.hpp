#pragma once

#include <unistd.h>

namespace ngosm {
namespace healpix {

inline const uint64_t MAX_NODE_CLUSTER_SIZE = 48'000'000;
inline const uint64_t MAX_EDGES_CLUSTER_SIZE = 1'638'400;
inline const uint64_t MAX_BN_CLUSTER_SIZE = 2'000'000;
inline const uint64_t MAX_BE_CLUSTER_SIZE = 330'000;

inline const uint16_t N_SIDE = 50;

}  // namespace healpix
}  // namespace ngosm