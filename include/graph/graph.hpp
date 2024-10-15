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
  Graph(parser::Edge* edges, size_t edgesCount);

  std::unordered_map<google::protobuf::int64,
                     std::vector<parser::Edge*>>::iterator
  find(google::protobuf::int64 nodeId);

  std::unordered_map<google::protobuf::int64,
                     std::vector<parser::Edge*>>::iterator
  end();

  std::unordered_map<google::protobuf::int64, std::vector<parser::Edge*>>&
  graph();

 private:
  std::unordered_map<google::protobuf::int64, std::vector<parser::Edge*>>
      vertEdgeMap;
  std::unordered_map<google::protobuf::int64,
                     std::vector<google::protobuf::int64>>
      invVertEdgeMap;
};
}  // namespace graph
}  // namespace parser