#pragma once

#include <osmpbf/osmformat.pb.h>

#include <disk/file-write.hpp>
#include <types/expanded-edge.hpp>
#include <types/relation.hpp>
#include <unordered_map>
#include <utils/hashing.hpp>

namespace parser {
namespace graph {
namespace invert {

template <typename E, typename EE>
void applyRestrictionsSourcePartition(
    E* sourceEdge,
    std::unordered_map<google::protobuf::int64, std::vector<EE*>>& graph,
    parser::Node* nodes, parser::Node* borderNodes,
    std::unordered_multimap<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& onlyRestrictionsHash,
    std::unordered_map<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& noRestrictionsHash,
    google::protobuf::int64& expandedEdgeId,
    FileWrite<parser::ExpandedEdge>& expEdgesFileWrite) {
  auto viaNodeId = sourceEdge->targetNodeId;
  auto graphNodePairIt = graph.find(viaNodeId);

  if (graphNodePairIt == graph.end()) {
    return;
  }

  auto mandRestRange = onlyRestrictionsHash.equal_range(
      std::make_pair(sourceEdge->wayId, viaNodeId));

  auto numMandRests = std::distance(mandRestRange.first, mandRestRange.second);

  if (numMandRests > 1) {
    auto to = mandRestRange.first->second->to;
    bool allEq = true;

    for (auto it = mandRestRange.first; it != mandRestRange.second; ++it) {
      if (it == mandRestRange.first) {
        continue;
      }
      if (to != it->second->to) {
        allEq = false;
      }
    }
    if (!allEq) {
      return;
    }
    numMandRests = 1;
  }

  if (numMandRests == 1) {
    auto targetEdgePtrIt =
        std::find_if(graphNodePairIt->second.begin(),
                     graphNodePairIt->second.end(), [&](auto edgePtr) {
                       return mandRestRange.first->second->to == edgePtr->wayId;
                     });

    if (targetEdgePtrIt != graphNodePairIt->second.end()) {
      auto sourceEdgeSourceNodePtr = nodes + sourceEdge->sourceNodeOffset;
      auto sourceEdgeTargetNodePtr = nodes + sourceEdge->targetNodeOffset;
      auto targetEdgeSourceNodePtr =
          borderNodes + (*targetEdgePtrIt)->sourceNodeOffset;
      auto targetEdgeTargetNodePtr =
          borderNodes + (*targetEdgePtrIt)->targetNodeOffset;

      if (sourceEdgeSourceNodePtr->lat == targetEdgeTargetNodePtr->lat &&
          sourceEdgeSourceNodePtr->lon == targetEdgeTargetNodePtr->lon &&
          sourceEdgeTargetNodePtr->lat == targetEdgeSourceNodePtr->lat &&
          sourceEdgeTargetNodePtr->lon == targetEdgeSourceNodePtr->lon) {
        return;
      }
      auto forbidRestPairIt = noRestrictionsHash.find(
          std::make_tuple(sourceEdge->wayId, (*targetEdgePtrIt)->wayId));
      if (forbidRestPairIt != noRestrictionsHash.end()) {
        return;
      }
      expEdgesFileWrite.add(parser::ExpandedEdge{
          expandedEdgeId, sourceEdge->id, 0, (*targetEdgePtrIt)->id, 0,
          (sourceEdge->cost + (*targetEdgePtrIt)->cost) / 2, 0});
      expandedEdgeId++;
      return;
    } else {
      return;
    }
  }

  for (auto targetEdgePtr : graphNodePairIt->second) {
    auto targetEdge = *targetEdgePtr;

    auto sourceEdgeSourceNodePtr = nodes + sourceEdge->sourceNodeOffset;
    auto sourceEdgeTargetNodePtr = nodes + sourceEdge->targetNodeOffset;
    auto targetEdgeSourceNodePtr = borderNodes + targetEdge.sourceNodeOffset;
    auto targetEdgeTargetNodePtr = borderNodes + targetEdge.targetNodeOffset;

    if (sourceEdgeSourceNodePtr->lat == targetEdgeTargetNodePtr->lat &&
        sourceEdgeSourceNodePtr->lon == targetEdgeTargetNodePtr->lon &&
        sourceEdgeTargetNodePtr->lat == targetEdgeSourceNodePtr->lat &&
        sourceEdgeTargetNodePtr->lon == targetEdgeSourceNodePtr->lon) {
      continue;
    }

    auto restrictionPairIt = noRestrictionsHash.find(
        std::make_tuple(sourceEdge->wayId, targetEdge.wayId));

    if (restrictionPairIt != noRestrictionsHash.end()) {
      continue;
    }

    expEdgesFileWrite.add(
        parser::ExpandedEdge{expandedEdgeId, sourceEdge->id, 0, targetEdge.id,
                             0, (sourceEdge->cost + targetEdge.cost) / 2, 0});

    expandedEdgeId++;
  }
}
}  // namespace invert
}  // namespace graph
}  // namespace parser