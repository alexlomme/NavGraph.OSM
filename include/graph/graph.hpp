#pragma once

#include <osmpbf/osmformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <tables/ska/flat_hash_map.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <utils/hashing.hpp>

namespace parser {
namespace graph {
struct Graph {
 public:
  Graph(std::unordered_map<google::protobuf::int64, parser::Edge>& edges);

  void invert(std::unordered_map<google::protobuf::int64, parser::Edge>& edges,
              std::unordered_multimap<
                  std::tuple<google::protobuf::int64, google::protobuf::int64>,
                  parser::Restriction*>& mandatoryRestrictions,
              std::unordered_map<
                  std::tuple<google::protobuf::int64, google::protobuf::int64,
                             google::protobuf::int64>,
                  parser::Restriction*>& forbidRestrictions,
              std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>&
                  expEdgesBuffer);

  std::unordered_map<google::protobuf::int64,
                     std::vector<parser::Edge*>>::iterator
  find(google::protobuf::int64 nodeId);

  std::unordered_map<google::protobuf::int64,
                     std::vector<parser::Edge*>>::iterator
  end();

 private:
  std::unordered_map<google::protobuf::int64, std::vector<parser::Edge*>>
      vertEdgeMap;
  std::unordered_map<google::protobuf::int64,
                     std::vector<google::protobuf::int64>>
      invVertEdgeMap;
};
}  // namespace graph
}  // namespace parser