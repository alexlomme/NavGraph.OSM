#include <arpa/inet.h>
#include <osmpbf/fileformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <fstream>
#include <graph/graph.hpp>
#include <graph/ways-to-edges.hpp>
#include <parsing/primitive-block-parser.hpp>
#include <sstream>
#include <tables/ska/flat_hash_map.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>
#include <utils/inflate.hpp>

constexpr uint64_t NODES_IN_L1 = 2000000;

const uint64_t NODES_TABLE_SIZE = std::floor(NODES_IN_L1 / 2);

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
  std::unordered_multimap<google::protobuf::int64, parser::Restriction>
      restrictions;
  std::vector<parser::Node> nodes;

  while (!file.eof()) {
    uint32_t headerSize;
    file.read(reinterpret_cast<char*>(&headerSize), sizeof(headerSize));
    if (file.eof()) {
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

    if (blob.has_raw()) {
      const std::string rawData = blob.raw();
      std::copy(rawData.begin(), rawData.end(), uncompressedData);
    } else if (blob.has_zlib_data()) {
      decompress(blob.zlib_data(), uncompressedData, blob.raw_size());
    } else {
      std::cerr << "Unsupported compression format" << std::endl;
      return -1;
    }

    OSMPBF::PrimitiveBlock primitiveBlock;

    if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
      std::cerr << "Unable to parse primitive block" << std::endl;
      return -1;
    }

    parser::primitive_block::parse(primitiveBlock, nodes, ways, restrictions);

    delete[] uncompressedData;
  }

  auto parseEnd = std::chrono::high_resolution_clock::now();

  auto parseDuration =
      std::chrono::duration_cast<std::chrono::milliseconds>(parseEnd - start);

  std::cerr << "Parse duration: " << parseDuration.count() << "ms" << std::endl;
  std::cerr << "Nodes: " << nodes.size() << std::endl;
  std::cerr << "Ways: " << ways.size() << std::endl;
  std::cerr << "Restrictions: " << restrictions.size() << std::endl;

  ska::flat_hash_map<google::protobuf::int64, uint64_t> usedNodes;

  auto hashingStart = std::chrono::high_resolution_clock::now();

  for (auto& way : ways) {
    for (uint64_t i = 0; i < way.nodes.size(); i++) {
      auto pairIt = usedNodes.find(way.nodes[i]);

      if (pairIt == usedNodes.end()) {
        if (i == 0 || i == way.nodes.size() - 1) {
          usedNodes.insert(std::make_pair(way.nodes[i], 2));
        } else {
          usedNodes.insert(std::make_pair(way.nodes[i], 1));
        }
      } else {
        if (i == 0 || i == way.nodes.size() - 1) {
          pairIt->second += 2;
        } else {
          pairIt->second++;
        }
      }
    }
  }

  std::unordered_map<google::protobuf::int64, parser::Node*> nodesHashMap;

  for (auto& node : nodes) {
    auto pairIt = usedNodes.find(node.id);
    if (pairIt == usedNodes.end()) {
      continue;
    }
    node.used = pairIt->second;
    nodesHashMap.insert(std::make_pair(node.id, &node));
  }

  std::cerr << "Hashing nodes: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - hashingStart)
                   .count()
            << "ms" << std::endl;

  std::unordered_map<google::protobuf::int64, parser::Edge> edgesBuffer;

  std::unordered_map<google::protobuf::int64, parser::ExpandedEdge>
      expEdgesBuffer;

  auto edgStart = std::chrono::high_resolution_clock::now();

  parser::waysToEdges(ways, nodesHashMap, edgesBuffer);

  std::cerr << "Generation of edges: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - edgStart)
                   .count()
            << "ms" << std::endl;
  std::cerr << "Edges: " << edgesBuffer.size() << std::endl;

  parser::graph::Graph graph(edgesBuffer);
  graph.invert(edgesBuffer, restrictions, expEdgesBuffer);

  std::cerr << "Expanded edges: " << expEdgesBuffer.size() << std::endl;

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << "Execution time: " << duration.count() << "ms" << std::endl;

  std::string outputFile = argc > 2 ? argv[2] : "output.csv";

  std::ofstream ofile(outputFile);

  if (!ofile.is_open()) {
    std::cerr << "Error opening file" << std::endl;
    return 1;
  }

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

  ofile << "from_vertex_id,"
        << "to_vertex_id,"
        << "weight,"
        << "edge_id,"
        << "osm_way_from,"
        << "osm_way_to,"
        << "osm_way_from_source_node,"
        << "osm_way_from_target_node,"
        << "osm_way_to_source_node,"
        << "osm_way_to_target_node" << std::endl;

  for (auto& pair : expEdgesBuffer) {
    auto expEdge = pair.second;

    auto sourceEdgePairIt = std::find_if(
        edgesBuffer.begin(), edgesBuffer.end(),
        [&](auto& edgePair) { return edgePair.first == expEdge.source; });
    auto targetEdgePairIt = std::find_if(
        edgesBuffer.begin(), edgesBuffer.end(),
        [&](auto& edgePair) { return edgePair.first == expEdge.target; });

    auto sourceEdge = sourceEdgePairIt->second;
    auto targetEdge = targetEdgePairIt->second;

    ofile << expEdge.source << "," << expEdge.target << "," << expEdge.cost
          << "," << expEdge.id << "," << sourceEdge.wayPtr->id << ","
          << targetEdge.wayPtr->id << "," << sourceEdge.sourceNodePtr->id << ","
          << sourceEdge.targetNodePtr->id << "," << targetEdge.sourceNodePtr->id
          << "," << targetEdge.targetNodePtr->id << std::endl;
  }

  file.close();

  std::ofstream ofile1("vertices.csv");

  if (!ofile1.is_open()) {
    std::cerr << "Error opening file" << std::endl;
    return 1;
  }

  for (auto nodePair : nodesHashMap) {
    ofile1 << nodePair.second->id << "," << nodePair.second->lat << ","
           << nodePair.second->lon << std::endl;
  }

  return 0;
}