#include "graph.hpp"

#include <types/expanded-edge.hpp>
#include <unordered_set>

parser::graph::Graph::Graph(
    std::unordered_map<google::protobuf::int64, parser::Edge>& edges) {
  for (auto& edgePair : edges) {
    auto entryIt = vertEdgeMap.find(edgePair.second.sourceNodePtr->id);
    if (entryIt == vertEdgeMap.end()) {
      std::vector<parser::Edge*> verts;
      verts.push_back(&edgePair.second);
      vertEdgeMap.insert(
          std::make_pair(edgePair.second.sourceNodePtr->id, verts));
    } else {
      entryIt->second.push_back(&edgePair.second);
    }
  }
}

void parser::graph::Graph::invert(
    std::unordered_map<google::protobuf::int64, parser::Edge>& edges,
    std::unordered_multimap<
        std::tuple<google::protobuf::int64, google::protobuf::int64>,
        parser::Restriction*>& mandatoryRestrictions,
    std::unordered_map<
        std::tuple<google::protobuf::int64, google::protobuf::int64,
                   google::protobuf::int64>,
        parser::Restriction*>& forbidRestrictions,
    std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>&
        expEdgesBuffer) {
  google::protobuf::int64 expandedEdgeId = 0;

  uint64_t mandRests = 0;

  for (auto sourceEdgePair : edges) {
    auto viaNodeId = sourceEdgePair.second.targetNodePtr->id;
    auto graphNodePairIt = vertEdgeMap.find(viaNodeId);

    if (graphNodePairIt == vertEdgeMap.end()) {
      continue;
    }

    auto mandRestRange = mandatoryRestrictions.equal_range(
        std::make_pair(sourceEdgePair.second.wayPtr->id, viaNodeId));

    auto numMandRests =
        std::distance(mandRestRange.first, mandRestRange.second);

    mandRests += numMandRests;

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
        auto sourceEdgeSourceNodePtr = sourceEdgePair.second.sourceNodePtr;
        auto sourceEdgeTargetNodePtr = sourceEdgePair.second.targetNodePtr;
        auto targetEdgeSourceNodePtr = (*targetEdgePtrIt)->sourceNodePtr;
        auto targetEdgeTargetNodePtr = (*targetEdgePtrIt)->targetNodePtr;

        if (sourceEdgeSourceNodePtr->lat == targetEdgeTargetNodePtr->lat &&
            sourceEdgeSourceNodePtr->lon == targetEdgeTargetNodePtr->lon &&
            sourceEdgeTargetNodePtr->lat == targetEdgeSourceNodePtr->lat &&
            sourceEdgeTargetNodePtr->lon == targetEdgeSourceNodePtr->lon) {
          continue;
        }
        auto forbidRestPairIt = forbidRestrictions.find(
            std::make_tuple(sourceEdgePair.second.wayPtr->id, viaNodeId,
                            (*targetEdgePtrIt)->wayPtr->id));
        if (forbidRestPairIt != forbidRestrictions.end()) {
          continue;
        }
        expEdgesBuffer.insert(std::make_pair(
            expandedEdgeId,
            parser::ExpandedEdge{
                expandedEdgeId, sourceEdgePair.first, (*targetEdgePtrIt)->id,
                (sourceEdgePair.second.cost + (*targetEdgePtrIt)->cost) / 2}));
        std::vector<google::protobuf::int64> vec;
        vec.push_back(expandedEdgeId);
        invVertEdgeMap.insert(std::make_pair(sourceEdgePair.first, vec));
        expandedEdgeId++;

        continue;
      } else {
        std::cerr << "AAAA" << std::endl;
        continue;
      }
    }

    for (auto targetEdgePtr : graphNodePairIt->second) {
      if (targetEdgePtr->id == sourceEdgePair.first) {
        continue;
      }

      auto targetEdge = *targetEdgePtr;

      auto sourceEdgeSourceNodePtr = sourceEdgePair.second.sourceNodePtr;
      auto sourceEdgeTargetNodePtr = sourceEdgePair.second.targetNodePtr;
      auto targetEdgeSourceNodePtr = targetEdge.sourceNodePtr;
      auto targetEdgeTargetNodePtr = targetEdge.targetNodePtr;

      if (sourceEdgeSourceNodePtr->lat == targetEdgeTargetNodePtr->lat &&
          sourceEdgeSourceNodePtr->lon == targetEdgeTargetNodePtr->lon &&
          sourceEdgeTargetNodePtr->lat == targetEdgeSourceNodePtr->lat &&
          sourceEdgeTargetNodePtr->lon == targetEdgeSourceNodePtr->lon) {
        continue;
      }

      auto restrictionPairIt = forbidRestrictions.find(std::make_tuple(
          sourceEdgePair.second.wayPtr->id, viaNodeId, targetEdge.wayPtr->id));

      if (restrictionPairIt != forbidRestrictions.end()) {
        continue;
      }

      expEdgesBuffer.insert(std::make_pair(
          expandedEdgeId,
          parser::ExpandedEdge{
              expandedEdgeId, sourceEdgePair.first, targetEdge.id,
              (sourceEdgePair.second.cost + targetEdge.cost) / 2}));
      auto invertedGraphSourceIt = invVertEdgeMap.find(sourceEdgePair.first);

      if (invertedGraphSourceIt == invVertEdgeMap.end()) {
        std::vector<google::protobuf::int64> vec;
        vec.push_back(expandedEdgeId);
        invVertEdgeMap.insert(std::make_pair(sourceEdgePair.first, vec));
      } else {
        invertedGraphSourceIt->second.push_back(expandedEdgeId);
      }
      expandedEdgeId++;
    }
  }

  std::cerr << mandRests << std::endl;
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