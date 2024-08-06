#include "graph.hpp"

#include <types/expanded-edge.hpp>

parser::graph::Graph::Graph(
    std::unordered_map<google::protobuf::int64, parser::Edge>& edges) {
  for (auto& edgePair : edges) {
    auto entryIt = vertEdgeMap.find(edgePair.second.sourceNodePtr->id);
    if (entryIt == vertEdgeMap.end()) {
      std::vector<google::protobuf::int64> verts;
      verts.push_back(edgePair.second.id);
      vertEdgeMap.insert(
          std::make_pair(edgePair.second.sourceNodePtr->id, verts));
    } else {
      entryIt->second.push_back(edgePair.second.id);
    }
  }
}

void parser::graph::Graph::invert(
    std::unordered_map<google::protobuf::int64, parser::Edge>& edges,
    std::unordered_multimap<google::protobuf::int64, parser::Restriction>&
        restrictions,
    std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>&
        expEdgesBuf) {
  google::protobuf::int64 expandedEdgeId = 0;

  std::unordered_map<google::protobuf::int64,
                     std::vector<google::protobuf::int64>>
      invVertEdgeMap;

  for (auto sourceEdgePair : edges) {
    auto viaNodeId = sourceEdgePair.second.targetNodePtr->id;
    auto graphNodePairIt = vertEdgeMap.find(viaNodeId);

    if (graphNodePairIt == vertEdgeMap.end()) {
      // std::cerr << "No outgoing edges from node" << std::endl;
      continue;
    }

    auto restrictionsForNodeRange = restrictions.equal_range(viaNodeId);

    auto mandatoryRestrictionPairIt = std::find_if(
        restrictionsForNodeRange.first, restrictionsForNodeRange.second,
        [&](auto& restrictionPair) {
          auto& restriction = restrictionPair.second;
          return restriction.from == sourceEdgePair.second.wayPtr->id &&
                 restriction.via == viaNodeId &&
                 (restriction.type == "only_right_turn" ||
                  restriction.type == "only_left_turn" ||
                  restriction.type == "only_straight_on");
        });

    if (mandatoryRestrictionPairIt != restrictionsForNodeRange.second) {
      auto targetEdgeIt =
          std::find_if(edges.begin(), edges.end(), [&](auto& edgePair) {
            return edgePair.second.wayPtr->id ==
                   mandatoryRestrictionPairIt->second.to;
          });

      if (targetEdgeIt != edges.end()) {
        expEdgesBuf.insert(std::make_pair(
            expandedEdgeId,
            parser::ExpandedEdge{
                expandedEdgeId, sourceEdgePair.first, targetEdgeIt->first,
                (sourceEdgePair.second.cost + targetEdgeIt->second.cost) / 2}));
        std::vector<google::protobuf::int64> vec;
        vec.push_back(expandedEdgeId);
        invVertEdgeMap.insert(std::make_pair(sourceEdgePair.first, vec));
        expandedEdgeId++;

        continue;
      }
    }

    for (auto& targetEdgeId : graphNodePairIt->second) {
      if (targetEdgeId == sourceEdgePair.first) {
        continue;
      }

      auto targetPairIt = edges.find(targetEdgeId);
      auto targetEdge = targetPairIt->second;

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

      auto restrictionPairIt = std::find_if(
          restrictionsForNodeRange.first, restrictionsForNodeRange.second,
          [&](auto& restrictionPair) {
            auto& restriction = restrictionPair.second;
            return restriction.from == sourceEdgePair.second.wayPtr->id &&
                   restriction.via == viaNodeId &&
                   restriction.to == targetEdge.wayPtr->id;
          });

      if (restrictionPairIt != restrictionsForNodeRange.second &&
          (restrictionPairIt->second.type == "no_right_turn" ||
           restrictionPairIt->second.type == "no_left_turn" ||
           restrictionPairIt->second.type == "no_straight_on")) {
        continue;
      }

      expEdgesBuf.insert(std::make_pair(
          expandedEdgeId,
          parser::ExpandedEdge{
              expandedEdgeId, sourceEdgePair.first, targetEdgeId,
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
  inverted = true;

  vertEdgeMap = invVertEdgeMap;
}