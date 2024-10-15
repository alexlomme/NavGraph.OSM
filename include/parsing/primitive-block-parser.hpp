#pragma once

#include <osmpbf/osmformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <tables/ska/flat_hash_map.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>

namespace parser {

namespace primitive_block {

double convertCoord(int64_t offset, int32_t granularity, int64_t coord);

}  // namespace primitive_block

}  // namespace parser