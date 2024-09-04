#include <arpa/inet.h>
#include <chealpix.h>
#include <fcntl.h>
#include <osmpbf/fileformat.pb.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmath>
#include <fstream>
#include <graph/graph.hpp>
#include <graph/invert.hpp>
#include <graph/ways-to-edges.hpp>
#include <iostream>
#include <parsing/parsing-functions.hpp>
#include <processing.hpp>
#include <sstream>
#include <stdexcept>
#include <tables/ska/flat_hash_map.hpp>
#include <types/border-edge.hpp>
#include <types/border-way.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/used-node.hpp>
#include <types/way.hpp>
#include <utils/hashing.hpp>

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  auto start = std::chrono::high_resolution_clock::now();
  if (argc < 2) {
    std::cerr << "No input file passed" << std::endl;
    return 1;
  }
  std::string filename = argv[1];
  int fd = open(filename.data(), O_RDONLY);

  if (fd == -1) {
    throw std::runtime_error("Failed to open file");
  }

  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    close(fd);
    throw std::runtime_error("Failed to obtain file size");
  }
  size_t file_size = sb.st_size;

  void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);

  if (mapped == MAP_FAILED) {
    close(fd);
    throw std::runtime_error("Failed mapping");
  }
  close(fd);

  const char* data = static_cast<const char*>(mapped);
  const char* enof = data + file_size;

  std::unordered_map<google::protobuf::int64, parser::UsedNode> usedNodes;

  // Filter out used nodes and count them
  parser::parsing::parseUsedNodes(data, enof, usedNodes);

  std::unordered_set<uint32_t> pixels;
  data = static_cast<const char*>(mapped);
  std::unordered_map<uint32_t, std::vector<parser::Node>> partNodeBuffers;

  // Partition and parse nodes
  parser::parsing::parseNodes(data, enof, partNodeBuffers, usedNodes, pixels);

  data = static_cast<const char*>(mapped);
  std::unordered_map<uint32_t, std::vector<parser::Way>> partWays;
  std::vector<parser::BorderWay> borderWays;

  std::vector<parser::Restriction> restrictions;

  // Partition and parse ways
  parser::parsing::thirdPhaseParse(data, enof, partWays, borderWays,
                                   restrictions, usedNodes);

  if (munmap(mapped, file_size) == -1) {
    throw std::runtime_error("Failed to unmap");
  }

  std::unordered_multimap<google::protobuf::int64, parser::Restriction*>
      onlyRestrictionsByTo;
  std::unordered_map<
      std::tuple<google::protobuf::int64, google::protobuf::int64>,
      parser::Restriction*>
      noRestrictionsHash;

  parser::processing::hash_restrictions(restrictions, onlyRestrictionsByTo,
                                        noRestrictionsHash);

  std::unordered_multimap<
      std::tuple<google::protobuf::int64, google::protobuf::int64>,
      parser::Restriction*>
      onlyRestrictionsMap;

  for (const auto& [_, buf] : partWays) {
    for (const auto& way : buf) {
      const auto& [first, second] = onlyRestrictionsByTo.equal_range(way.id);
      if (first == second) {
        continue;
      }

      std::for_each(first, second, [&](auto& pair) {
        auto ptr = pair.second;
        onlyRestrictionsMap.emplace(std::piecewise_construct,
                                    std::forward_as_tuple(ptr->from, ptr->via),
                                    std::forward_as_tuple(ptr));
      });
    }
  }

  for (const auto& way : borderWays) {
    const auto& [first, second] = onlyRestrictionsByTo.equal_range(way.id);
    if (first == second) {
      continue;
    }

    std::for_each(first, second, [&](auto& pair) {
      auto ptr = pair.second;
      onlyRestrictionsMap.emplace(std::piecewise_construct,
                                  std::forward_as_tuple(ptr->from, ptr->via),
                                  std::forward_as_tuple(ptr));
    });
  }

  std::unordered_map<uint32_t, parser::graph::Graph> graphs;
  std::unordered_map<uint32_t, std::vector<parser::Edge>> edgePartitions;
  std::unordered_map<uint32_t, std::vector<parser::ExpandedEdge>>
      expEdgePartitions;
  std::unordered_map<uint32_t,
                     std::unordered_map<google::protobuf::int64, parser::Node*>>
      nodeHTs;

  // process partitions
  for (auto ipix : pixels) {
    const auto nodesIt = partNodeBuffers.find(ipix);
    const auto waysIt = partWays.find(ipix);

    if (nodesIt == partNodeBuffers.end()) {
      continue;
    }

    auto& nodes = nodesIt->second;

    const auto htIt =
        nodeHTs
            .emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                     std::forward_as_tuple())
            .first;

    auto& nodesHashTable = htIt->second;

    nodesHashTable.reserve(nodes.size());

    for (auto& node : nodes) {
      nodesHashTable.emplace(node.id, &node);
    }

    if (waysIt == partWays.end()) {
      continue;
    }

    const auto edgesIt =
        edgePartitions
            .emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                     std::forward_as_tuple())
            .first;

    auto& edgesBuf = edgesIt->second;

    parser::waysToEdges(waysIt->second, nodesHashTable, edgesBuf);
  }

  for (auto& [ipix, buf] : edgePartitions) {
    graphs.emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                   std::forward_as_tuple(buf));
  }

  for (const auto ipix : pixels) {
    const auto edgesIt = edgePartitions.find(ipix);

    const auto graphIt = graphs.find(ipix);

    if (graphIt == graphs.end() || edgesIt == edgePartitions.end()) {
      continue;
    }

    const auto expEdgesIt =
        expEdgePartitions
            .emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                     std::forward_as_tuple())
            .first;
    auto& expEdgesBuf = expEdgesIt->second;

    graphIt->second.invert(edgesIt->second, onlyRestrictionsMap,
                           noRestrictionsHash, expEdgesBuf);
  }

  uint64_t sum = 0;
  for (const auto& [_, buf] : expEdgePartitions) {
    sum += buf.size();
  }

  std::vector<parser::BorderEdge> borderEdges;
  borderEdges.reserve(borderWays.size());
  google::protobuf::int64 edgeId = 0;

  // process border ways / restrictions
  for (auto& way : borderWays) {
    if (way.nodes.size() == 0) {
      continue;
    }

    auto sourceIpix = way.nodes[0].first;
    const auto sourceNodeHtIt = nodeHTs.find(sourceIpix);

    if (sourceNodeHtIt == nodeHTs.end()) {
      std::cerr << sourceIpix << std::endl;
      throw std::runtime_error("No ht for source node");
    }

    auto sourcePairIt = sourceNodeHtIt->second.find(way.nodes[0].second[0]);
    if (sourcePairIt == sourceNodeHtIt->second.end()) {
      throw std::runtime_error("Node is missing 1");
    }
    auto sourceNodePtr = sourcePairIt->second;
    auto prevNodePtr = sourceNodePtr;

    double cost = 0;

    for (uint64_t i = 0; i < way.nodes.size(); i++) {
      const auto nodeHtIt = nodeHTs.find(way.nodes[i].first);
      if (nodeHtIt == nodeHTs.end()) {
        throw std::runtime_error("No ht for this partition");
      }

      for (uint64_t j = (i == 0 ? 1 : 0); j < way.nodes[i].second.size(); j++) {
        const auto nodeIt = nodeHtIt->second.find(way.nodes[i].second[j]);

        if (nodeIt == nodeHtIt->second.end()) {
          throw std::runtime_error("Node is missing 2");
        }

        auto& nodePtr = nodeIt->second;

        cost += geopointsDistance(
            std::make_pair(prevNodePtr->lat, prevNodePtr->lon),
            std::make_pair(nodePtr->lat, nodePtr->lon));

        prevNodePtr = nodePtr;

        if (nodePtr->used <= 1) {
          continue;
        }

        borderEdges.emplace_back(edgeId, &way, sourceNodePtr, nodePtr, cost);

        if (!way.oneway) {
          edgeId++;
          borderEdges.emplace_back(edgeId, &way, nodePtr, sourceNodePtr, cost);
        }

        sourceNodePtr = nodePtr;
        cost = 0;
        edgeId++;
      }
    }
  }

  std::unordered_map<google::protobuf::int64, std::vector<parser::BorderEdge*>>
      borderGraph;

  for (auto& edge : borderEdges) {
    auto entryIt = borderGraph.find(edge.sourceNodePtr->id);
    if (entryIt == borderGraph.end()) {
      auto vecIt = borderGraph
                       .emplace(std::piecewise_construct,
                                std::forward_as_tuple(edge.sourceNodePtr->id),
                                std::forward_as_tuple())
                       .first;
      vecIt->second.push_back(&edge);
    } else {
      entryIt->second.push_back(&edge);
    }
  }

  std::vector<parser::ExpandedEdge> borderExpEdges;

  for (const auto ipix : pixels) {
    const auto edgesPaitIt = edgePartitions.find(ipix);

    if (edgesPaitIt == edgePartitions.end()) {
      continue;
    }
    auto& buf = edgesPaitIt->second;

    google::protobuf::int64 expandedEdgeId = 0;

    for (auto& sourceEdge : buf) {
      parser::graph::invert::applyRestrictions(
          sourceEdge, borderGraph, onlyRestrictionsMap, noRestrictionsHash,
          expandedEdgeId, borderExpEdges);
    }
  }

  google::protobuf::int64 expandedEdgeId = 0;

  for (auto& sourceEdge : borderEdges) {
    parser::graph::invert::applyRestrictions(
        sourceEdge, borderGraph, onlyRestrictionsMap, noRestrictionsHash,
        expandedEdgeId, borderExpEdges);
  }

  for (auto& sourceEdge : borderEdges) {
    auto theta = (90 - sourceEdge.targetNodePtr->lat) * M_PI / 180;
    auto phi = sourceEdge.targetNodePtr->lon * M_PI / 180;
    long ipix;
    ang2pix_ring(50, theta, phi, &ipix);

    const auto graphIt = graphs.find(ipix);

    if (graphIt == graphs.end()) {
      continue;
    }

    auto& graph = graphIt->second;

    parser::graph::invert::applyRestrictions(
        sourceEdge, graph.graph(), onlyRestrictionsMap, noRestrictionsHash,
        expandedEdgeId, borderExpEdges);
  }

  std::cerr << "Total expanded edges: " << sum + borderExpEdges.size()
            << std::endl;

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << "Execution time: " << duration.count() << "ms" << std::endl;

  return 0;
}