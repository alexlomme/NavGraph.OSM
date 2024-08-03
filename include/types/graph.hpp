#pragma once

#include <utils/geomath.hpp>

#include "edge.hpp"
#include "expanded-edge.hpp"
#include "node.hpp"
#include "relation.hpp"
#include "way.hpp"

namespace parser {

struct Graph {
 public:
  void add_way(parser::Way way);

  void add_node(parser::Node node);

  void add_restriction(parser::Restriction restriction);

  void convert();

  std::unordered_map<google::protobuf::int64, parser::Edge>& edges();

  std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>&
  expaned_edges();

 private:
  // initially parsed data
  std::unordered_map<google::protobuf::int64, parser::Way> ways;
  std::unordered_multimap<google::protobuf::int64, parser::Restriction>
      restrictions;
  std::unordered_map<google::protobuf::int64, parser::Node> nodes;
  std::unordered_map<google::protobuf::int64, uint64_t> nodesInWays;

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

}  // namespace parser