#pragma once

#include <osmpbf/osmformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <tables/ska/flat_hash_map.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>

namespace parser {
namespace graph {
struct Graph {
 public:
  Graph(std::unordered_map<google::protobuf::int64, parser::Edge>& edges);

  void invert(std::unordered_map<google::protobuf::int64, parser::Edge>& edges,
              std::unordered_multimap<google::protobuf::int64,
                                      parser::Restriction>& restrictions,
              std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>&
                  expEdgeBuf);

 private:
  std::unordered_map<google::protobuf::int64,
                     std::vector<google::protobuf::int64>>
      vertEdgeMap;
  bool inverted;
};
}  // namespace graph
}  // namespace parser