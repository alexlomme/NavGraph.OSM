#pragma once

#include <osmpbf/osmformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <tables/ska/flat_hash_map.hpp>
#include <types/disk-way.hpp>
#include <types/edge.hpp>
#include <types/node.hpp>
#include <types/way.hpp>
#include <utils/geomath.hpp>

namespace parser {

void waysToEdges(
    // std::vector<parser::Way>& ways,
    // std::unordered_map<google::protobuf::int64, parser::Node*>& nodes,
    DiskWay* waysPtr, size_t waysSize, google::protobuf::int64* wayNodes,
    parser::Node* nodes,
    ska::flat_hash_map<google::protobuf::int64, uint64_t>& nodeHashTable,
    std::vector<parser::Edge>& edgesBuf, google::protobuf::int64& edgeId);
}  // namespace parser