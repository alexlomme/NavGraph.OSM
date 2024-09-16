#include "ways-to-edges.hpp"

#include <osmpbf/osmformat.pb.h>

void parser::waysToEdges(
    DiskWay* waysPtr, size_t waysSize, google::protobuf::int64* wayNodes,
    parser::Node* nodes,
    ska::flat_hash_map<google::protobuf::int64, uint64_t>& nodeHashTable,
    std::vector<parser::Edge>& edgesBuf, google::protobuf::int64& edgeId) {
  for (uint64_t i = 0; i < waysSize; i++) {
    DiskWay* wayPtr = waysPtr + i;

    // auto& wayNodes = way.nodes;

    const auto sourcePairIt = nodeHashTable.find(*(wayNodes + wayPtr->offset));
    if (sourcePairIt == nodeHashTable.end()) {
      throw std::runtime_error("Node is missing in ways2edges");
    }
    auto sourceNodeOffset = sourcePairIt->second;
    auto prevNodeOffset = sourceNodeOffset;

    double cost = 0;

    for (uint64_t j = 1; j < wayPtr->size; j++) {
      auto nodeId = *(wayNodes + wayPtr->offset + j);
      auto nodePairIt = nodeHashTable.find(nodeId);

      if (nodePairIt == nodeHashTable.end()) {
        throw std::runtime_error("Node is missing (1) in ways2edges");
      }
      auto nodeOffset = nodePairIt->second;

      cost += geopointsDistance(
          std::make_pair((nodes + prevNodeOffset)->lat,
                         (nodes + prevNodeOffset)->lon),
          std::make_pair((nodes + nodeOffset)->lat, (nodes + nodeOffset)->lon));

      prevNodeOffset = nodeOffset;

      if ((nodes + nodeOffset)->used <= 1) {
        continue;
      }

      edgesBuf.push_back(parser::Edge{
          edgeId, wayPtr->id, i, (nodes + sourceNodeOffset)->id,
          sourceNodeOffset, (nodes + nodeOffset)->id, nodeOffset, cost, 0});

      if (!wayPtr->oneway) {
        edgeId++;
        edgesBuf.push_back(parser::Edge{
            edgeId, wayPtr->id, i, (nodes + nodeOffset)->id, nodeOffset,
            (nodes + sourceNodeOffset)->id, sourceNodeOffset, cost, 0});
      }

      sourceNodeOffset = nodeOffset;
      cost = 0;
      edgeId++;
    }
  }
}