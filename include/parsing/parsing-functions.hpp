#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/border-way.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/used-node.hpp>
#include <types/way.hpp>

namespace parser {
namespace parsing {

void parseUsedNodes(
    const char* data, const char* endOfFile,
    std::unordered_map<google::protobuf::int64, parser::UsedNode>& usedNodes);

void parseNodes(
    const char* data, const char* endOfFile,
    std::unordered_map<uint32_t, std::vector<parser::Node>>& nodePartitions,
    std::unordered_map<google::protobuf::int64, parser::UsedNode>& usedNodes,
    std::unordered_set<uint32_t>& pixels);

void thirdPhaseParse(
    const char* data, const char* endOfFile,
    std::unordered_map<uint32_t, std::vector<parser::Way>>& wayPartitions,
    std::vector<parser::BorderWay>& borderWays,
    std::vector<parser::Restriction>& restrictions,
    const std::unordered_map<google::protobuf::int64, parser::UsedNode>&
        usedNode);

}  // namespace parsing
}  // namespace parser