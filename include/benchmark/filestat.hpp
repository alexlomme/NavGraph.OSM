#pragma once

#include <unistd.h>

namespace ngosm {
namespace bm {
struct Filestat {
  uint64_t total_decomp_size;
  uint64_t total_nodes;
  uint64_t used_nodes;
  uint64_t total_ways;
  uint64_t used_ways;
  uint64_t total_nodes_in_ways;
  uint64_t used_nodes_in_ways;
  uint64_t total_relations;
  uint64_t used_relations;

  int64_t total_parse_nodes_time;
  int64_t total_parse_ways_time;
};
}  // namespace bm
}  // namespace ngosm