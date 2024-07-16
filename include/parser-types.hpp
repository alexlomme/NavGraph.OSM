#include <ranges>

#include "geomath.hpp"
#include "osmpbf/osmformat.pb.h"

namespace parser {

struct Node {
  google::protobuf::int64 id;
  double lat;
  double lon;
  uint64_t used;
};

struct Edge {
  google::protobuf::int64 id;
  google::protobuf::int64 wayId;
  google::protobuf::int64 sourceNode;
  google::protobuf::int64 targetNode;
  double cost;
};

struct ExpandedEdge {
  google::protobuf::int64 id;
  google::protobuf::int64 source;
  google::protobuf::int64 target;
  double cost;
};

struct Way {
  Way(google::protobuf::int64 id, std::vector<google::protobuf::int64> nodes,
      bool oneway)
      : id(id), nodes(nodes), oneway(oneway) {}

  google::protobuf::int64 id;
  std::vector<google::protobuf::int64> nodes;
  bool oneway;

  google::protobuf::int64 first_node() {
    return nodes.size() != 0 ? nodes[0] : -1;
  }

  google::protobuf::int64 last_node() {
    return nodes.size() != 0 ? nodes[nodes.size() - 1] : -1;
  }
};

struct Relation {
 public:
  Relation(google::protobuf::int64 from, google::protobuf::int64 via,
           google::protobuf::int64 to)
      : from(from), via(via), to(to) {}

  google::protobuf::int64 from;
  google::protobuf::int64 to;
  google::protobuf::int64 via;
};

struct Restriction : parser::Relation {
  Restriction(google::protobuf::int64 from, google::protobuf::int64 via,
              google::protobuf::int64 to, std::string type)
      : parser::Relation(from, via, to), type(type) {}

  std::string type;
};

// struct Highway : parser::Way {
//   enum Type {
//     motorway = 'motorway',
//   };

//  public:
//   Highway(google::protobuf::int64 id,
//           std::vector<google::protobuf::int64> nodes)
//       : parser::Way(id, nodes) {}

//  private:
//   Highway::Type type;
//   std::string name;
//   std::string ref;
//   bool oneway;
// };

struct Graph {
 public:
  void add_way(parser::Way&& way) {
    ways.insert(std::make_pair(way.id, way));
    for (auto node : way.nodes) {
      nodesInWays.insert(node);
    }
  }

  void add_node(parser::Node&& node) {
    nodes.insert(std::make_pair(node.id, node));
  }

  void add_restriction(parser::Restriction&& restriction) {
    restrictions.insert(std::make_pair(restriction.via, restriction));
  }

  void convert() {
    std::erase_if(nodes, [&](auto nodePair) {
      return nodesInWays.find(nodePair.first) == nodesInWays.end();
    });

    for (auto& wayPair : ways) {
      auto& way = wayPair.second;
      for (auto nodeId : way.nodes) {
        auto nodeIt = nodes.find(nodeId);
        if (nodeId == way.nodes[0] ||
            nodeId == way.nodes[way.nodes.size() - 1]) {
          nodeIt->second.used += 2;
        } else {
          nodeIt->second.used++;
        }
      }
    }

    nodesInWays.clear();

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

        if (i == 0 || node.used <= 1) {
          continue;
        }

        auto sourcePairIt = nodes.find(sourceId);
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
                  (sourceEdgePair.second.cost + targetEdgeIt->second.cost) /
                      2}));
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

  std::unordered_map<google::protobuf::int64, parser::Edge>& edges() {
    return edgesMap;
  }

  std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>&
  expaned_edges() {
    return expandedEdgesMap;
  }

 private:
  // initially parsed data
  std::unordered_map<google::protobuf::int64, parser::Way> ways;
  std::unordered_multimap<google::protobuf::int64, parser::Restriction>
      restrictions;
  std::unordered_map<google::protobuf::int64, parser::Node> nodes;
  std::unordered_set<google::protobuf::int64> nodesInWays;

  // graph from initially parsed data
  std::unordered_map<google::protobuf::int64,
                     std::vector<google::protobuf::int64>>
      graph;
  std::unordered_map<google::protobuf::int64, parser::Edge> edgesMap;

  // inverted graph, considering turn restrictions
  std::unordered_map<google::protobuf::int64,
                     std::vector<google::protobuf::int64>>
      invertedGraph;
  std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>
      expandedEdgesMap;
};

struct PrimitiveBlockParser {
 public:
  PrimitiveBlockParser(OSMPBF::PrimitiveBlock& block) : block(block) {}

  double convertLat(google::protobuf::int64 coord) {
    return convertCoord(block.lat_offset(), coord);
  }

  double convertLon(google::protobuf::int64 coord) {
    return convertCoord(block.lon_offset(), coord);
  }

  void parse(parser::Graph& graph) {
    auto stringTable = block.stringtable();

    for (auto& group : block.primitivegroup()) {
      for (auto& parsedWay : group.ways()) {
        auto& keys = parsedWay.keys();
        auto& values = parsedWay.vals();

        auto highwayIt = std::find_if(
            keys.begin(), keys.end(),
            [&](uint32_t key) { return stringTable.s(key) == "highway"; });
        if (highwayIt == keys.end()) {
          continue;
        }

        std::string highwayType =
            stringTable.s(values[std::distance(keys.begin(), highwayIt)]);

        if (std::find(highwayTypes.begin(), highwayTypes.end(), highwayType) ==
            highwayTypes.end()) {
          continue;
        }

        auto owIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
          return stringTable.s(key) == "oneway";
        });

        bool oneway = false;
        if (owIt != keys.end()) {
          uint64_t index = std::distance(keys.begin(), owIt);
          auto onewayVal = stringTable.s(values[index]);

          if (onewayVal == "yes" || onewayVal == "1") {
            oneway = true;
          }
        } else {
          // std::cerr << "oneway not found" << std::endl;
        }

        std::vector<google::protobuf::int64> nodeRefs;

        google::protobuf::int64 prevValue = 0;

        for (auto& nodeRef : parsedWay.refs()) {
          nodeRefs.push_back(nodeRef + prevValue);
          prevValue += nodeRef;
        }

        graph.add_way(std::move(parser::Way{parsedWay.id(), nodeRefs, oneway}));
      }

      for (auto& parsedRelation : group.relations()) {
        auto& keys = parsedRelation.keys();
        auto& values = parsedRelation.vals();

        std::vector<uint64_t> range(keys.size());
        std::iota(range.begin(), range.end(), 0);

        auto it = std::find_if(range.begin(), range.end(), [&](uint64_t index) {
          return stringTable.s(keys[index]) == "type";
        });
        if (it == range.end()) {
          // std::cerr << "Relation doesn't have a \"type\" key" << std::endl;
        }

        // if (stringTable.s(values[*it]) != "restriction") {
        //   continue;
        // }

        auto restIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
          return stringTable.s(key) == "restriction";
        });

        if (restIt == keys.end()) {
          // std::cerr << "Restriction type no specified" << std::endl;
          continue;
        }

        auto restrictionType =
            stringTable.s(values[std::distance(keys.begin(), restIt)]);

        auto& ids = parsedRelation.memids();
        auto& roles = parsedRelation.roles_sid();
        auto& types = parsedRelation.types();

        if (parsedRelation.memids_size() != 3) {
          // std::cerr << "Too much members" << std::endl;
          continue;
        }

        google::protobuf::int64 from = -1;
        google::protobuf::int64 to = -1;
        google::protobuf::int64 via = -1;

        google::protobuf::int64 prevId = 0;

        for (uint64_t i = 0; i < parsedRelation.memids_size(); i++) {
          auto id = ids[i] + prevId;
          auto role = stringTable.s(roles[i]);
          auto type = types[i];

          if (role == "from" &&
              type == OSMPBF::Relation::MemberType::Relation_MemberType_WAY) {
            from = id;
          } else if (role == "to" && type == OSMPBF::Relation::MemberType::
                                                 Relation_MemberType_WAY) {
            to = id;
          } else if (role == "via" && type == OSMPBF::Relation::MemberType::
                                                  Relation_MemberType_NODE) {
            via = id;
          } else {
            // std::cerr << role << " " << type << restrictionType << std::endl;
          }

          prevId = id;
        }

        if (from == -1 || to == -1 || via == -1) {
          // std::cerr << "Invalid restriction" << std::endl;
          continue;
        }

        graph.add_restriction(
            parser::Restriction{from, via, to, restrictionType});
      }

      for (auto& parsedNode : group.nodes()) {
        auto lat = convertLat(parsedNode.lat());
        auto lon = convertLon(parsedNode.lon());

        graph.add_node(std::move(parser::Node{parsedNode.id(), lat, lon, 0}));
      }

      auto& denseNodes = group.dense();

      if (denseNodes.id_size() != denseNodes.lon_size() ||
          denseNodes.id_size() != denseNodes.lat_size()) {
        throw std::runtime_error("Lacking information for dense nodes\n");
      }

      int64_t prevId = 0;
      int64_t prevLon = 0;
      int64_t prevLat = 0;

      for (int64_t i = 0; i < denseNodes.id_size(); i++) {
        auto deltaId = denseNodes.id(i);
        auto deltaLat = denseNodes.lat(i);
        auto deltaLon = denseNodes.lon(i);

        int64_t id = deltaId + prevId;

        auto lat = convertLat(deltaLat + prevLat);
        auto lon = convertLon(deltaLon + prevLon);

        graph.add_node(std::move(parser::Node{id, lat, lon, 0}));

        prevId += deltaId;
        prevLat += deltaLat;
        prevLon += deltaLon;
      }
    }
  }

 private:
  OSMPBF::PrimitiveBlock& block;

  std::vector<std::string> highwayTypes{
      "motorway",      "primary",        "primary_link", "road",
      "secondary",     "secondary_link", "residential",  "tertiary",
      "tertiary_link", "unclassified",   "trunk",        "trunk_link",
      "motorway_link"};

  double convertCoord(google::protobuf::int64 offset,
                      google::protobuf::int64 coord) {
    return (offset + block.granularity() * coord) / pow(10, 9);
  }
};

}  // namespace parser