#include "graph.hpp"

#include <types/expanded-edge.hpp>
#include <unordered_set>

ngosm::graph::Graph::Graph(ngosm::types::Edge* edges, size_t edgeCount) {
  for (uint64_t i = 0; i < edgeCount; i++) {
    auto edgePtr = edges + i;
    auto entryIt = vertEdgeMap.find(edgePtr->sourceNodeId);
    if (entryIt == vertEdgeMap.end()) {
      std::vector<ngosm::types::Edge*> verts;
      verts.push_back(edgePtr);
      vertEdgeMap.insert(std::make_pair(edgePtr->sourceNodeId, verts));
    } else {
      entryIt->second.push_back(edgePtr);
    }
  }
}

std::unordered_map<google::protobuf::int64,
                   std::vector<ngosm::types::Edge*>>::iterator
ngosm::graph::Graph::find(google::protobuf::int64 nodeId) {
  return vertEdgeMap.find(nodeId);
}

std::unordered_map<google::protobuf::int64,
                   std::vector<ngosm::types::Edge*>>::iterator
ngosm::graph::Graph::end() {
  return vertEdgeMap.end();
}

std::unordered_map<google::protobuf::int64, std::vector<ngosm::types::Edge*>>&
ngosm::graph::Graph::graph() {
  return vertEdgeMap;
}
