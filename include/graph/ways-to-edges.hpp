#pragma once

#include <osmpbf/osmformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <tables/ska/flat_hash_map.hpp>
#include <types/edge.hpp>
#include <types/node.hpp>
#include <types/way.hpp>
#include <utils/geomath.hpp>

namespace parser {

void waysToEdges(
    std::vector<parser::Way>& ways,
    std::unordered_map<google::protobuf::int64, parser::Node*>& nodes,
    std::vector<parser::Edge>& edgesBuf);
}  // namespace parser