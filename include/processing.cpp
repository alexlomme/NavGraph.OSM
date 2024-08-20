#include "processing.hpp"

namespace parser {
namespace processing {

void hash_restrictions(restriction_buffer_t& buffer,
                       only_restrictions_by_to& onlyRestrictionsByTo,
                       no_restriction_map_t& noRestrictionsMap) {
  for (auto& restriction : buffer) {
    auto& type = restriction.type;
    if (type != "only_right_turn" && type != "only_left_turn" &&
        type != "only_straight_on") {
      if (type == "no_right_turn" || type == "no_left_turn" ||
          type == "no_straight_on") {
        noRestrictionsMap.insert(std::make_pair(
            std::make_tuple(restriction.from, restriction.via, restriction.to),
            &restriction));
      }
      continue;
    }
    onlyRestrictionsByTo.insert(std::make_pair(restriction.to, &restriction));
  }
}

void process_ways(way_buffer_t& buffer, node_count_t& usedNodes,
                  only_restrictions_by_to& onlyRestrictionsByTo,
                  only_restriction_mm& onlyRestrictionsMap) {
  for (auto& way : buffer) {
    auto restRange = onlyRestrictionsByTo.equal_range(way.id);
    if (restRange.first != restRange.second) {
      std::for_each(
          restRange.first, restRange.second, [&](auto restrictionPair) {
            onlyRestrictionsMap.insert(
                std::make_pair(std::make_tuple(restrictionPair.second->from,
                                               restrictionPair.second->via),
                               restrictionPair.second));
          });
    }

    for (uint64_t i = 0; i < way.nodes.size(); i++) {
      auto pairIt = usedNodes.find(way.nodes[i]);

      if (pairIt == usedNodes.end()) {
        if (i == 0 || i == way.nodes.size() - 1) {
          usedNodes.insert(std::make_pair(way.nodes[i], 2));
        } else {
          usedNodes.insert(std::make_pair(way.nodes[i], 1));
        }
      } else {
        if (i == 0 || i == way.nodes.size() - 1) {
          pairIt->second += 2;
        } else {
          pairIt->second++;
        }
      }
    }
  }
}

void hash_nodes(node_buffer_t& buffer, node_count_t& usedNodes,
                node_map& nodesHashMap) {
  for (auto& node : buffer) {
    auto pairIt = usedNodes.find(node.id);
    if (pairIt == usedNodes.end()) {
      continue;
    }
    node.used = pairIt->second;
    nodesHashMap.insert(std::make_pair(node.id, &node));
  }
}

}  // namespace processing
}  // namespace parser