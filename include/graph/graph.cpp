#include "graph.hpp"

#include <types/expanded-edge.hpp>
#include <unordered_set>

parser::graph::Graph::Graph(std::vector<parser::Edge>& edges) {
  for (auto& edge : edges) {
    auto entryIt = vertEdgeMap.find(edge.sourceNodePtr->id);
    if (entryIt == vertEdgeMap.end()) {
      std::vector<parser::Edge*> verts;
      verts.push_back(&edge);
      vertEdgeMap.insert(std::make_pair(edge.sourceNodePtr->id, verts));
    } else {
      entryIt->second.push_back(&edge);
    }
  }
}

void parser::graph::Graph::invert(
    std::vector<parser::Edge>& edges,
    std::unordered_multimap<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& mandatoryRestrictions,
    std::unordered_map<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& forbidRestrictions,
    std::vector<parser::ExpandedEdge>& expEdgesBuffer) {
  google::protobuf::int64 expandedEdgeId = 0;

  for (auto sourceEdge : edges) {
    auto viaNodeId = sourceEdge.targetNodePtr->id;
    auto graphNodePairIt = vertEdgeMap.find(viaNodeId);

    if (graphNodePairIt == vertEdgeMap.end()) {
      continue;
    }

    auto mandRestRange = mandatoryRestrictions.equal_range(
        std::make_pair(sourceEdge.wayPtr->id, viaNodeId));

    auto numMandRests =
        std::distance(mandRestRange.first, mandRestRange.second);

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
        continue;
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
          continue;
        }
        auto forbidRestPairIt = forbidRestrictions.find(std::make_tuple(
            sourceEdge.wayPtr->id, (*targetEdgePtrIt)->wayPtr->id));
        if (forbidRestPairIt != forbidRestrictions.end()) {
          continue;
        }
        expEdgesBuffer.push_back(parser::ExpandedEdge{
            expandedEdgeId, sourceEdge.sourceNodePtr, sourceEdge.targetNodePtr,
            (*targetEdgePtrIt)->sourceNodePtr,
            (*targetEdgePtrIt)->targetNodePtr,
            (sourceEdge.cost + (*targetEdgePtrIt)->cost) / 2});
        std::vector<google::protobuf::int64> vec;
        vec.push_back(expandedEdgeId);
        invVertEdgeMap.insert(std::make_pair(sourceEdge.id, vec));
        expandedEdgeId++;

        continue;
      } else {
        continue;
      }
    }

    for (auto targetEdgePtr : graphNodePairIt->second) {
      if (targetEdgePtr->id == sourceEdge.id) {
        continue;
      }

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

      auto restrictionPairIt = forbidRestrictions.find(
          std::make_tuple(sourceEdge.wayPtr->id, targetEdge.wayPtr->id));

      if (restrictionPairIt != forbidRestrictions.end()) {
        continue;
      }

      expEdgesBuffer.push_back(parser::ExpandedEdge{
          expandedEdgeId, sourceEdge.sourceNodePtr, sourceEdge.targetNodePtr,
          targetEdge.sourceNodePtr, targetEdge.targetNodePtr,
          (sourceEdge.cost + targetEdge.cost) / 2});
      auto invertedGraphSourceIt = invVertEdgeMap.find(sourceEdge.id);

      if (invertedGraphSourceIt == invVertEdgeMap.end()) {
        std::vector<google::protobuf::int64> vec;
        vec.push_back(expandedEdgeId);
        invVertEdgeMap.insert(std::make_pair(sourceEdge.id, vec));
      } else {
        invertedGraphSourceIt->second.push_back(expandedEdgeId);
      }
      expandedEdgeId++;
    }
  }
}

std::unordered_map<google::protobuf::int64,
                   std::vector<parser::Edge*>>::iterator
parser::graph::Graph::find(google::protobuf::int64 nodeId) {
  return vertEdgeMap.find(nodeId);
}

std::unordered_map<google::protobuf::int64,
                   std::vector<parser::Edge*>>::iterator
parser::graph::Graph::end() {
  return vertEdgeMap.end();
}

std::unordered_map<google::protobuf::int64, std::vector<parser::Edge*>>&
parser::graph::Graph::graph() {
  return vertEdgeMap;
}
