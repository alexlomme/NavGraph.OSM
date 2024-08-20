#include "ways-to-edges.hpp"

#include <osmpbf/osmformat.pb.h>

void parser::waysToEdges(
    std::vector<parser::Way>& ways,
    std::unordered_map<google::protobuf::int64, parser::Node*>& nodes,
    std::vector<parser::Edge>& edgesBuf) {
  google::protobuf::int64 edgeId = 0;

  for (auto& way : ways) {
    auto& wayNodes = way.nodes;

    auto sourcePairIt = nodes.find(way.nodes[0]);
    if (sourcePairIt == nodes.end()) {
      throw std::runtime_error("Node is missing");
    }
    auto sourceNodePtr = sourcePairIt->second;
    auto prevNodePtr = sourceNodePtr;

    double cost = 0;

    for (uint64_t i = 1; i < wayNodes.size(); i++) {
      auto nodeId = wayNodes[i];
      auto nodePairIt = nodes.find(nodeId);

      if (nodePairIt == nodes.end()) {
        throw std::runtime_error("Node is missing");
      }
      auto& nodePtr = nodePairIt->second;

      cost +=
          geopointsDistance(std::make_pair(prevNodePtr->lat, prevNodePtr->lon),
                            std::make_pair(nodePtr->lat, nodePtr->lon));

      prevNodePtr = nodePtr;

      if (nodePtr->used <= 1) {
        continue;
      }

      edgesBuf.push_back(
          parser::Edge{edgeId, &way, sourceNodePtr, nodePtr, cost});
      edgeId++;

      if (!way.oneway) {
        edgesBuf.push_back(
            parser::Edge{edgeId, &way, nodePtr, sourceNodePtr, cost});
      }

      sourceNodePtr = nodePtr;
      cost = 0;
      edgeId++;
    }
  }
}