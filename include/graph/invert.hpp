#include <osmpbf/osmformat.pb.h>

#include <types/expanded-edge.hpp>
#include <types/relation.hpp>
#include <unordered_map>
#include <utils/hashing.hpp>

namespace parser {
namespace graph {
namespace invert {

template <typename E, typename EE>
void applyRestrictions(
    E& sourceEdge,
    std::unordered_map<google::protobuf::int64, std::vector<EE*>>& graph,
    std::unordered_multimap<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& onlyRestrictionsHash,
    std::unordered_map<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& noRestrictionsHash,
    google::protobuf::int64& expandedEdgeId,
    std::vector<parser::ExpandedEdge>& expEdgesBuffer) {
  auto viaNodeId = sourceEdge.targetNodePtr->id;
  auto graphNodePairIt = graph.find(viaNodeId);

  if (graphNodePairIt == graph.end()) {
    return;
  }

  auto mandRestRange = onlyRestrictionsHash.equal_range(
      std::make_pair(sourceEdge.wayPtr->id, viaNodeId));

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
        graphNodePairIt->second.begin(), graphNodePairIt->second.end(),
        [&](auto edgePtr) {
          return mandRestRange.first->second->to == edgePtr->wayPtr->id;
        });

    if (targetEdgePtrIt != graphNodePairIt->second.end()) {
      auto sourceEdgeSourceNodePtr = sourceEdge.sourceNodePtr;
      auto sourceEdgeTargetNodePtr = sourceEdge.targetNodePtr;
      auto targetEdgeSourceNodePtr = (*targetEdgePtrIt)->sourceNodePtr;
      auto targetEdgeTargetNodePtr = (*targetEdgePtrIt)->targetNodePtr;

      if (sourceEdgeSourceNodePtr->lat == targetEdgeTargetNodePtr->lat &&
          sourceEdgeSourceNodePtr->lon == targetEdgeTargetNodePtr->lon &&
          sourceEdgeTargetNodePtr->lat == targetEdgeSourceNodePtr->lat &&
          sourceEdgeTargetNodePtr->lon == targetEdgeSourceNodePtr->lon) {
        return;
      }
      auto forbidRestPairIt = noRestrictionsHash.find(std::make_tuple(
          sourceEdge.wayPtr->id, (*targetEdgePtrIt)->wayPtr->id));
      if (forbidRestPairIt != noRestrictionsHash.end()) {
        return;
      }
      expEdgesBuffer.push_back(parser::ExpandedEdge{
          expandedEdgeId, sourceEdge.sourceNodePtr, sourceEdge.targetNodePtr,
          (*targetEdgePtrIt)->sourceNodePtr, (*targetEdgePtrIt)->targetNodePtr,
          (sourceEdge.cost + (*targetEdgePtrIt)->cost) / 2});
      std::vector<google::protobuf::int64> vec;
      vec.push_back(expandedEdgeId);
      expandedEdgeId++;
      return;
    } else {
      return;
    }
  }

  for (auto targetEdgePtr : graphNodePairIt->second) {
    auto targetEdge = *targetEdgePtr;

    auto sourceEdgeSourceNodePtr = sourceEdge.sourceNodePtr;
    auto sourceEdgeTargetNodePtr = sourceEdge.targetNodePtr;
    auto targetEdgeSourceNodePtr = targetEdge.sourceNodePtr;
    auto targetEdgeTargetNodePtr = targetEdge.targetNodePtr;

    if (sourceEdgeSourceNodePtr->lat == targetEdgeTargetNodePtr->lat &&
        sourceEdgeSourceNodePtr->lon == targetEdgeTargetNodePtr->lon &&
        sourceEdgeTargetNodePtr->lat == targetEdgeSourceNodePtr->lat &&
        sourceEdgeTargetNodePtr->lon == targetEdgeSourceNodePtr->lon) {
      continue;
    }

    auto restrictionPairIt = noRestrictionsHash.find(
        std::make_tuple(sourceEdge.wayPtr->id, targetEdge.wayPtr->id));

    if (restrictionPairIt != noRestrictionsHash.end()) {
      continue;
    }

    expEdgesBuffer.push_back(parser::ExpandedEdge{
        expandedEdgeId, sourceEdge.sourceNodePtr, sourceEdge.targetNodePtr,
        targetEdge.sourceNodePtr, targetEdge.targetNodePtr,
        (sourceEdge.cost + targetEdge.cost) / 2});

    expandedEdgeId++;
  }
}
}  // namespace invert
}  // namespace graph
}  // namespace parser