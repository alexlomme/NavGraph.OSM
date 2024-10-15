#include <osmpbf/osmformat.pb.h>

#include <disk/file-write.hpp>
#include <types/expanded-edge.hpp>
#include <types/relation.hpp>
#include <unordered_map>
#include <utils/hashing.hpp>

namespace parser {
namespace graph {
namespace invert {

void applyRestrictions(
    parser::Edge* sourceEdge, long sourceIpix, uint64_t sourceOffset,
    std::vector<std::tuple<parser::Edge*, long, uint64_t>>& outgoingEdges,
    parser::Node* nodes,
    std::unordered_multimap<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& onlyRestrictionsHash,
    std::unordered_map<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& noRestrictionsHash,
    google::protobuf::int64& expandedEdgeId,
    FileWrite<parser::ExpandedEdge>& expEdgesFileWrite) {
  auto viaNodeId = sourceEdge->targetNodeId;

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
    auto targetEdgePtrIt = std::find_if(
        outgoingEdges.begin(), outgoingEdges.end(), [&](auto tuple) {
          return mandRestRange.first->second->to == get<0>(tuple)->wayId;
        });

    if (targetEdgePtrIt != outgoingEdges.end()) {
      auto sourceEdgeSourceNodePtr = nodes + sourceEdge->sourceNodeOffset;
      auto sourceEdgeTargetNodePtr = nodes + sourceEdge->targetNodeOffset;
      auto targetEdgeSourceNodePtr =
          nodes + (get<0>(*targetEdgePtrIt))->sourceNodeOffset;
      auto targetEdgeTargetNodePtr =
          nodes + (get<0>(*targetEdgePtrIt))->targetNodeOffset;

      if (sourceEdgeSourceNodePtr->lat == targetEdgeTargetNodePtr->lat &&
          sourceEdgeSourceNodePtr->lon == targetEdgeTargetNodePtr->lon &&
          sourceEdgeTargetNodePtr->lat == targetEdgeSourceNodePtr->lat &&
          sourceEdgeTargetNodePtr->lon == targetEdgeSourceNodePtr->lon) {
        return;
      }
      auto forbidRestPairIt = noRestrictionsHash.find(std::make_tuple(
          sourceEdge->wayId, (get<0>(*targetEdgePtrIt))->wayId));
      if (forbidRestPairIt != noRestrictionsHash.end()) {
        return;
      }
      expEdgesFileWrite.add(parser::ExpandedEdge{
          expandedEdgeId, sourceIpix, sourceOffset, get<1>(*targetEdgePtrIt),
          get<2>(*targetEdgePtrIt),
          (sourceEdge->cost + (get<0>(*targetEdgePtrIt))->cost) / 2});
      expandedEdgeId++;
      return;
    } else {
      return;
    }
  }

  for (auto tuple : outgoingEdges) {
    auto targetEdge = *get<0>(tuple);

    auto sourceEdgeSourceNodePtr = nodes + sourceEdge->sourceNodeOffset;
    auto sourceEdgeTargetNodePtr = nodes + sourceEdge->targetNodeOffset;
    auto targetEdgeSourceNodePtr = nodes + targetEdge.sourceNodeOffset;
    auto targetEdgeTargetNodePtr = nodes + targetEdge.targetNodeOffset;

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

    expEdgesFileWrite.add(parser::ExpandedEdge{
        expandedEdgeId, sourceIpix, sourceOffset, get<1>(tuple), get<2>(tuple),
        (sourceEdge->cost + targetEdge.cost) / 2});

    expandedEdgeId++;
  }
}
}  // namespace invert
}  // namespace graph
}  // namespace parser