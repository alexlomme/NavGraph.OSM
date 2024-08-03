#include "graph.hpp"

void parser::Graph::add_way(parser::Way way) {
  ways.insert(std::make_pair(way.id, way));
  for (uint64_t i = 0; i < way.nodes.size(); i++) {
    auto nodeId = way.nodes[i];
    auto nodeEntryIt = nodesInWays.find(nodeId);

    if (nodeEntryIt == nodesInWays.end()) {
      if (i == 0 || i == way.nodes.size() - 1) {
        nodesInWays.insert(std::make_pair(nodeId, 2));
      } else {
        nodesInWays.insert(std::make_pair(nodeId, 1));
      }
    } else {
      if (i == 0 || i == way.nodes.size() - 1) {
        nodeEntryIt->second += 2;
      } else {
        nodeEntryIt->second++;
      }
    }
  }
}

void parser::Graph::add_node(parser::Node node) {
  nodes.insert(std::make_pair(node.id, node));
}

void parser::Graph::add_restriction(parser::Restriction restriction) {
  restrictions.insert(std::make_pair(restriction.via, restriction));
}

void parser::Graph::convert() {
  std::erase_if(nodes, [&](auto nodePair) {
    return nodesInWays.find(nodePair.first) == nodesInWays.end();
  });

  std::cerr << "Ways: " << ways.size() << std::endl;
  std::cerr << "Nodes: " << nodes.size() << std::endl;
  std::cerr << "Restrictions: " << restrictions.size() << std::endl;

  google::protobuf::int64 edgeId = 0;
  for (auto& wayPair : ways) {
    auto& way = wayPair.second;

    auto& wayNodes = way.nodes;
    std::vector<std::pair<double, double>> locations;

    google::protobuf::int64 sourceId = wayNodes[0];

    // TODO:  improve through addLocation

    for (uint64_t i = 0; i < wayNodes.size(); i++) {
      auto nodeId = wayNodes[i];
      auto nodePairIt = nodes.find(nodeId);

      if (nodePairIt == nodes.end()) {
        throw std::runtime_error("Node is missing");
      }
      auto& node = nodePairIt->second;

      locations.push_back(std::make_pair(node.lat, node.lon));

      auto nodeUsedPairIt = nodesInWays.find(nodeId);

      if (nodeUsedPairIt == nodesInWays.end()) {
        throw std::runtime_error("");
      }

      if (i == 0 || nodeUsedPairIt->second <= 1) {
        continue;
      }

      double cost = wayCost(locations);

      auto entryIt = graph.find(sourceId);
      if (entryIt == graph.end()) {
        std::vector<google::protobuf::int64> vec;
        vec.push_back(edgeId);
        graph.insert(std::make_pair(sourceId, vec));
      } else {
        entryIt->second.push_back(edgeId);
      }

      edgesMap.insert(
          {edgeId, parser::Edge{edgeId, way.id, sourceId, nodeId, cost}});
      edgeId++;

      if (!way.oneway) {
        auto targetEntryIt = graph.find(nodeId);
        if (targetEntryIt == graph.end()) {
          std::vector<google::protobuf::int64> vec;
          vec.push_back(edgeId);
          graph.insert(std::make_pair(nodeId, vec));
        } else {
          targetEntryIt->second.push_back(edgeId);
        }

        edgesMap.insert(
            {edgeId, parser::Edge{edgeId, way.id, nodeId, sourceId, cost}});
      }

      locations.clear();
      locations.push_back(std::make_pair(node.lat, node.lon));
      sourceId = nodeId;
      edgeId++;
    }
  }

  std::cerr << "Edges: " << edgesMap.size() << std::endl;

  ways.clear();

  google::protobuf::int64 expandedEdgeId = 0;

  for (auto sourceEdgePair : edgesMap) {
    auto viaNodeId = sourceEdgePair.second.targetNode;
    auto graphNodePairIt = graph.find(viaNodeId);

    if (graphNodePairIt == graph.end()) {
      // std::cerr << "No outgoing edges from node" << std::endl;
      continue;
    }

    auto restrictionsForNodeRange = restrictions.equal_range(viaNodeId);

    auto mandatoryRestrictionPairIt = std::find_if(
        restrictionsForNodeRange.first, restrictionsForNodeRange.second,
        [&](auto& restrictionPair) {
          auto& restriction = restrictionPair.second;
          return restriction.from == sourceEdgePair.second.wayId &&
                 restriction.via == viaNodeId &&
                 (restriction.type == "only_right_turn" ||
                  restriction.type == "only_left_turn" ||
                  restriction.type == "only_straight_on");
        });

    if (mandatoryRestrictionPairIt != restrictionsForNodeRange.second) {
      auto targetEdgeIt =
          std::find_if(edgesMap.begin(), edgesMap.end(), [&](auto& edgePair) {
            return edgePair.second.wayId ==
                   mandatoryRestrictionPairIt->second.to;
          });

      if (targetEdgeIt != edgesMap.end()) {
        expandedEdgesMap.insert(std::make_pair(
            expandedEdgeId,
            parser::ExpandedEdge{
                expandedEdgeId, sourceEdgePair.first, targetEdgeIt->first,
                (sourceEdgePair.second.cost + targetEdgeIt->second.cost) / 2}));
        std::vector<google::protobuf::int64> vec;
        vec.push_back(expandedEdgeId);
        invertedGraph.insert(std::make_pair(sourceEdgePair.first, vec));
        expandedEdgeId++;

        continue;
      }
    }

    for (auto& targetEdgeId : graphNodePairIt->second) {
      if (targetEdgeId == sourceEdgePair.first) {
        continue;
      }

      auto targetPairIt = edgesMap.find(targetEdgeId);
      auto targetEdge = targetPairIt->second;

      auto sourceEdgeSourceNodePairIt =
          nodes.find(sourceEdgePair.second.sourceNode);
      auto sourceEdgeTargetNodePairIt =
          nodes.find(sourceEdgePair.second.targetNode);
      auto targetEdgeSourceNodePairIt = nodes.find(targetEdge.sourceNode);
      auto targetEdgeTargetNodePairIt = nodes.find(targetEdge.targetNode);

      if (sourceEdgeSourceNodePairIt->second.lat ==
              targetEdgeTargetNodePairIt->second.lat &&
          sourceEdgeSourceNodePairIt->second.lon ==
              targetEdgeTargetNodePairIt->second.lon &&
          sourceEdgeTargetNodePairIt->second.lat ==
              targetEdgeSourceNodePairIt->second.lat &&
          sourceEdgeTargetNodePairIt->second.lon ==
              targetEdgeSourceNodePairIt->second.lon) {
        continue;
      }
      auto restrictionPairIt = std::find_if(
          restrictionsForNodeRange.first, restrictionsForNodeRange.second,
          [&](auto& restrictionPair) {
            auto& restriction = restrictionPair.second;
            return restriction.from == sourceEdgePair.second.wayId &&
                   restriction.via == viaNodeId &&
                   restriction.to == targetEdge.wayId;
          });

      if (restrictionPairIt != restrictionsForNodeRange.second &&
          (restrictionPairIt->second.type == "no_right_turn" ||
           restrictionPairIt->second.type == "no_left_turn" ||
           restrictionPairIt->second.type == "no_straight_on")) {
        continue;
      }

      expandedEdgesMap.insert(std::make_pair(
          expandedEdgeId,
          parser::ExpandedEdge{
              expandedEdgeId, sourceEdgePair.first, targetEdgeId,
              (sourceEdgePair.second.cost + targetEdge.cost) / 2}));
      auto invertedGraphSourceIt = invertedGraph.find(sourceEdgePair.first);

      if (invertedGraphSourceIt == invertedGraph.end()) {
        std::vector<google::protobuf::int64> vec;
        vec.push_back(expandedEdgeId);
        invertedGraph.insert(std::make_pair(sourceEdgePair.first, vec));
      } else {
        invertedGraphSourceIt->second.push_back(expandedEdgeId);
      }
      expandedEdgeId++;
    }
  }
  std::cerr << "Expanded Edges: " << expandedEdgesMap.size() << std::endl;
}

std::unordered_map<google::protobuf::int64, parser::Edge>&
parser::Graph::edges() {
  return edgesMap;
}

std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>&
parser::Graph::expaned_edges() {
  return expandedEdgesMap;
}
