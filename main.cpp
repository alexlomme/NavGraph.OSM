#include <arpa/inet.h>
#include <fcntl.h>
#include <osmpbf/fileformat.pb.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <graph/graph.hpp>
#include <graph/ways-to-edges.hpp>
#include <iostream>
#include <parsing/primitive-block-parser.hpp>
#include <processing.hpp>
#include <sstream>
#include <stdexcept>
#include <tables/ska/flat_hash_map.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>
#include <utils/hashing.hpp>
#include <utils/libdeflate_decomp.hpp>

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

  std::vector<parser::Way> ways;
  std::vector<parser::Restriction> restrictions;
  std::vector<parser::Node> nodes;
  while (data < enof) {
    uint32_t headerSize;
    std::memcpy(&headerSize, data, sizeof(headerSize));
    headerSize = ntohl(headerSize);

    data += sizeof(headerSize);

    OSMPBF::BlobHeader header;
    if (!header.ParseFromArray(data, headerSize)) {
      std::cerr << "Failed to parse BlobHeader." << std::endl;
      break;
    }

    data += headerSize;

    if (header.type() != "OSMData") {
      data += header.datasize();
      continue;
    }

    OSMPBF::Blob blob;
    if (!blob.ParseFromArray(data, header.datasize())) {
      std::cerr << "Failed to parse Blob." << std::endl;
      break;
    }
    data += header.datasize();

    unsigned char* uncompressedData = new unsigned char[blob.raw_size()];

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
    if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
      std::cerr << "Unable to parse primitive block" << std::endl;
      return -1;
    }

    parser::primitive_block::parse(primitiveBlock, nodes, ways, restrictions);

    delete[] uncompressedData;
  }

  if (munmap(mapped, file_size) == -1) {
    throw std::runtime_error("Failed to unmap");
  }

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

  parser::processing::hash_restrictions(restrictions, toOnlyRestrictionsMap,
                                        forbidRestrictionsMap);

  std::unordered_multimap<
      std::tuple<google::protobuf::int64, google::protobuf::int64>,
      parser::Restriction*>
      mandatoryRestrictionsMap;
  ska::flat_hash_map<google::protobuf::int64, uint64_t> usedNodes;

  parser::processing::process_ways(ways, usedNodes, toOnlyRestrictionsMap,
                                   mandatoryRestrictionsMap);

  toOnlyRestrictionsMap.clear();

  std::unordered_map<google::protobuf::int64, parser::Node*> nodesHashMap;
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

  return 0;
}