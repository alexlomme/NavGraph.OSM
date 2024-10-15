#include "graph.hpp"

#include <types/expanded-edge.hpp>
#include <unordered_set>

parser::graph::Graph::Graph(parser::Edge* edges, size_t edgeCount) {
  for (uint64_t i = 0; i < edgeCount; i++) {
    auto edgePtr = edges + i;
    auto entryIt = vertEdgeMap.find(edgePtr->sourceNodeId);
    if (entryIt == vertEdgeMap.end()) {
      std::vector<parser::Edge*> verts;
      verts.push_back(edgePtr);
      vertEdgeMap.insert(std::make_pair(edgePtr->sourceNodeId, verts));
    } else {
      entryIt->second.push_back(edgePtr);
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
