#include <arpa/inet.h>
#include <osmpbf/fileformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <fstream>
#include <graph/graph.hpp>
#include <graph/ways-to-edges.hpp>
#include <parsing/primitive-block-parser.hpp>
#include <processing.hpp>
#include <sstream>
#include <tables/ska/flat_hash_map.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>
#include <utils/hashing.hpp>
#include <utils/inflate.hpp>
#include <utils/libdeflate_decomp.hpp>

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (argc < 2) {
    std::cerr << "No input file passed" << std::endl;
    return 1;
  }

  std::string filename = argv[1];

  std::ifstream file(filename, std::ios::in | std::ios::binary);

  auto start = std::chrono::high_resolution_clock::now();

  if (!file.is_open()) {
    std::cerr << "Error opening input file";
    return 1;
  }

  // initialize buffers
  std::vector<parser::Way> ways;
  std::vector<parser::Restriction> restrictions;
  std::vector<parser::Node> nodes;
  while (file) {
    uint32_t headerSize;
    file.read(reinterpret_cast<char*>(&headerSize), sizeof(headerSize));
    if (!file) {
      break;
    }
    headerSize = ntohl(headerSize);
    std::vector<char> headerData(headerSize);
    file.read(headerData.data(), headerSize);

    OSMPBF::BlobHeader blobHeader;

    if (!blobHeader.ParseFromArray(headerData.data(), headerSize)) {
      std::cerr << "Error while reading BlobHeader" << std::endl;
      return -1;
    }

    uint32_t blobSize = blobHeader.datasize();
    std::vector<char> data(blobSize);
    file.read(data.data(), blobSize);

    OSMPBF::Blob blob;

    if (!blob.ParseFromArray(data.data(), blobSize)) {
      std::cerr << "Error while reading blob" << std::endl;
      return -1;
    }

    if (blobHeader.type() != "OSMData") {
      continue;
    }

    unsigned char* uncompressedData = new unsigned char[blob.raw_size()];

    auto decomp = std::chrono::high_resolution_clock::now();
    if (blob.has_raw()) {
      const std::string rawData = blob.raw();
      std::copy(rawData.begin(), rawData.end(), uncompressedData);
    } else if (blob.has_zlib_data()) {
      ldeflate_decompress(blob.zlib_data(), uncompressedData, blob.raw_size());
    } else {
      std::cerr << "Unsupported compression format" << std::endl;
      return -1;
    }

    OSMPBF::PrimitiveBlock primitiveBlock;

    auto primBl = std::chrono::high_resolution_clock::now();
    if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
      std::cerr << "Unable to parse primitive block" << std::endl;
      return -1;
    }

    parser::primitive_block::parse(primitiveBlock, nodes, ways, restrictions);

    delete[] uncompressedData;
  }

  file.close();

  std::cerr << "Nodes: " << nodes.size() << std::endl;
  std::cerr << "Ways: " << ways.size() << std::endl;
  std::cerr << "Restrictions: " << restrictions.size() << std::endl;

  // hash restrictions by to
  std::unordered_multimap<google::protobuf::int64, parser::Restriction*>
      toOnlyRestrictionsMap;

  std::unordered_map<
      std::tuple<google::protobuf::int64, google::protobuf::int64,
                 google::protobuf::int64>,
      parser::Restriction*>
      forbidRestrictionsMap;
  // for (auto& restriction : restrictions) {
  //   auto& type = restriction.type;
  //   if (type != "only_right_turn" && type != "only_left_turn" &&
  //       type != "only_straight_on") {
  //     if (type == "no_right_turn" || type == "no_left_turn" ||
  //         type == "no_straight_on") {
  //       forbidRestrictionsMap.insert(std::make_pair(
  //           std::make_tuple(restriction.from, restriction.via,
  //           restriction.to), &restriction));
  //     }
  //     continue;
  //   }
  //   toOnlyRestrictionsMap.insert(std::make_pair(restriction.to,
  //   &restriction));
  // }

  parser::processing::hash_restrictions(restrictions, toOnlyRestrictionsMap,
                                        forbidRestrictionsMap);

  std::unordered_multimap<
      std::tuple<google::protobuf::int64, google::protobuf::int64>,
      parser::Restriction*>
      mandatoryRestrictionsMap;
  ska::flat_hash_map<google::protobuf::int64, uint64_t> usedNodes;

  // for (auto& way : ways) {
  //   auto restRange = toOnlyRestrictionsMap.equal_range(way.id);
  //   if (restRange.first != restRange.second) {
  //     std::for_each(
  //         restRange.first, restRange.second, [&](auto restrictionPair) {
  //           mandatoryRestrictionsMap.insert(
  //               std::make_pair(std::make_tuple(restrictionPair.second->from,
  //                                              restrictionPair.second->via),
  //                              restrictionPair.second));
  //         });
  //   }

  //   for (uint64_t i = 0; i < way.nodes.size(); i++) {
  //     auto pairIt = usedNodes.find(way.nodes[i]);

  //     if (pairIt == usedNodes.end()) {
  //       if (i == 0 || i == way.nodes.size() - 1) {
  //         usedNodes.insert(std::make_pair(way.nodes[i], 2));
  //       } else {
  //         usedNodes.insert(std::make_pair(way.nodes[i], 1));
  //       }
  //     } else {
  //       if (i == 0 || i == way.nodes.size() - 1) {
  //         pairIt->second += 2;
  //       } else {
  //         pairIt->second++;
  //       }
  //     }
  //   }
  // }

  parser::processing::process_ways(ways, usedNodes, toOnlyRestrictionsMap,
                                   mandatoryRestrictionsMap);

  std::unordered_map<google::protobuf::int64, parser::Node*> nodesHashMap;

  // for (auto& node : nodes) {
  //   auto pairIt = usedNodes.find(node.id);
  //   if (pairIt == usedNodes.end()) {
  //     continue;
  //   }
  //   node.used = pairIt->second;
  //   nodesHashMap.insert(std::make_pair(node.id, &node));
  // }

  parser::processing::hash_nodes(nodes, usedNodes, nodesHashMap);

  std::vector<parser::Edge> edgesBuffer;

  std::vector<parser::ExpandedEdge> expEdgesBuffer;

  parser::waysToEdges(ways, nodesHashMap, edgesBuffer);

  std::cerr << "Edges: " << edgesBuffer.size() << std::endl;

  parser::graph::Graph graph(edgesBuffer);

  graph.invert(edgesBuffer, mandatoryRestrictionsMap, forbidRestrictionsMap,
               expEdgesBuffer);

  std::cerr << "Expanded edges: " << expEdgesBuffer.size() << std::endl;

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << "Execution time: " << duration.count() << "ms" << std::endl;

  // std::string outputFile = argc > 2 ? argv[2] : "output.csv";

  // std::ofstream ofile(outputFile);

  // if (!ofile.is_open()) {
  //   std::cerr << "Error opening file" << std::endl;
  //   return 1;
  // }

  // from_vertex_id;
  // to_vertex_id;
  // weight;
  // geom;
  // was_one_way;
  // edge_id;
  // osm_way_from;
  // osm_way_to;
  // osm_way_from_source_node;
  // osm_way_from_target_node;
  // osm_way_to_source_node;
  // osm_way_to_target_node

  // ofile << "from_vertex_id,"
  //       << "to_vertex_id,"
  //       << "weight,"
  //       << "edge_id,"
  //       << "osm_way_from,"
  //       << "osm_way_to,"
  //       << "osm_way_from_source_node,"
  //       << "osm_way_from_target_node,"
  //       << "osm_way_to_source_node,"
  //       << "osm_way_to_target_node" << std::endl;

  // for (auto& pair : expEdgesBuffer) {
  //   auto expEdge = pair.second;

  //   auto sourceEdgePairIt = std::find_if(
  //       edgesBuffer.begin(), edgesBuffer.end(),
  //       [&](auto& edgePair) { return edgePair.first == expEdge.source; });
  //   auto targetEdgePairIt = std::find_if(
  //       edgesBuffer.begin(), edgesBuffer.end(),
  //       [&](auto& edgePair) { return edgePair.first == expEdge.target; });

  //   auto sourceEdge = sourceEdgePairIt->second;
  //   auto targetEdge = targetEdgePairIt->second;

  //   ofile << expEdge.source << "," << expEdge.target << "," << expEdge.cost
  //         << "," << expEdge.id << "," << sourceEdge.wayPtr->id << ","
  //         << targetEdge.wayPtr->id << "," << sourceEdge.sourceNodePtr->id <<
  //         ","
  //         << sourceEdge.targetNodePtr->id << "," <<
  //         targetEdge.sourceNodePtr->id
  //         << "," << targetEdge.targetNodePtr->id << std::endl;
  // }

  return 0;
}