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
#include <parsing/primitive-block-parser.hpp>
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
#include <utils/libdeflate_decomp.hpp>

struct DiskNode {
  google::protobuf::int64 id;
  double lat;
  double lon;
};

struct DiskWay {
  google::protobuf::int64 id;
  uint64_t offset;
  int size;
  bool oneway;
};

struct DiskRestriction {
  google::protobuf::int64 id;
  google::protobuf::int64 from;
  google::protobuf::int64 via;
  google::protobuf::int64 to;
  char t;
  char w;
};

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
    std::cerr << "Failed to open .osm.pbf file" << std::endl;
    return 1;
  }

  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    close(fd);
    std::cerr << "Failed fstat" << std::endl;
    return 1;
  }
  size_t pbf_size = sb.st_size;

  void* mapped_pbf = mmap(nullptr, pbf_size, PROT_READ, MAP_PRIVATE, fd, 0);

  if (mapped_pbf == MAP_FAILED) {
    close(fd);
    std::cerr << ".osm.pbf mmap failed" << std::endl;
    return 1;
  }
  close(fd);

  const char* pbf_data = static_cast<const char*>(mapped_pbf);
  const char* eof_pbf = pbf_data + pbf_size;

  // ---------------------------------------------------------

  int fd_nodes = open("./disk-buffers/nodes.bin", O_RDWR | O_CREAT | O_TRUNC,
                      S_IRUSR | S_IWUSR);
  if (fd_nodes == -1) {
    std::cerr << "Failed to open file.\n";
    return -1;
  }
  void* map_nodes;

  size_t nodes_size = 0;

  // -----------------------------------------------------------

  int fd_ways = open("./disk-buffers/ways.bin", O_RDWR | O_CREAT | O_TRUNC,
                     S_IRUSR | S_IWUSR);
  if (fd_ways == -1) {
    std::cerr << "Failed to open file.\n";
    return -1;
  }
  size_t ways_size = 0;
  void* map_ways;

  // --------------------------------------------------------------

  int fd_way_nodes = open("./disk-buffers/way_nodes.bin",
                          O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_way_nodes == -1) {
    std::cerr << "Failed to open file.\n";
    return -1;
  }
  size_t size_way_nodes = 0;
  void* map_way_nodes;

  // --------------------------------------------------------------

  int fd_rests = open("./disk-buffers/restrictions.bin",
                      O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

  if (fd_rests == -1) {
    std::cerr << "failed to open file with restrictions for writing"
              << std::endl;
    return -1;
  }
  size_t rests_size = 0;
  void* map_rests;

  uint64_t offset = 0;

  std::unordered_map<google::protobuf::int64, parser::UsedNode> usedNodes;
  while (pbf_data < eof_pbf) {
    uint32_t headerSize;
    std::memcpy(&headerSize, pbf_data, sizeof(headerSize));
    headerSize = ntohl(headerSize);

    pbf_data += sizeof(headerSize);

    OSMPBF::BlobHeader header;
    if (!header.ParseFromArray(pbf_data, headerSize)) {
      std::cerr << "Failed to parse BlobHeader" << std::endl;
      return 1;
    }

    pbf_data += headerSize;

    if (header.type() != "OSMData") {
      pbf_data += header.datasize();
      continue;
    }

    OSMPBF::Blob blob;
    if (!blob.ParseFromArray(pbf_data, header.datasize())) {
      std::cerr << "Failed to parse Blob" << std::endl;
      return 1;
    }
    pbf_data += header.datasize();

    unsigned char* uncompressedData = new unsigned char[blob.raw_size()];

    if (blob.has_raw()) {
      const std::string rawData = blob.raw();
      std::copy(rawData.begin(), rawData.end(), uncompressedData);
    } else if (blob.has_zlib_data()) {
      ldeflate_decompress(blob.zlib_data(), uncompressedData, blob.raw_size());
    } else {
      std::cerr << "Unsupported compression format" << std::endl;
      return 1;
    }

    OSMPBF::PrimitiveBlock primitiveBlock;
    if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
      std::cerr << "Failed to parse primitive block" << std::endl;
      return 1;
    }

    const auto& stringtable = primitiveBlock.stringtable();

    for (const auto& group : primitiveBlock.primitivegroup()) {
      std::vector<DiskWay> diskWays;
      std::vector<google::protobuf::int64> wayNodes;

      for (const auto& way : group.ways()) {
        const auto& keys = way.keys();
        const auto& values = way.vals();

        auto highwayIt = std::find_if(
            keys.begin(), keys.end(),
            [&](uint32_t key) { return stringtable.s(key) == "highway"; });
        if (highwayIt == keys.end()) {
          continue;
        }

        std::string highwayType =
            stringtable.s(values[std::distance(keys.begin(), highwayIt)]);

        if (parser::supportedHighwayTypes.find(highwayType) ==
            parser::supportedHighwayTypes.end()) {
          continue;
        }

        auto owIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
          return stringtable.s(key) == "oneway";
        });

        bool oneway = false;
        if (owIt != keys.end()) {
          uint64_t index = std::distance(keys.begin(), owIt);
          auto onewayVal = stringtable.s(values[index]);

          if (onewayVal == "yes" || onewayVal == "1") {
            oneway = true;
          }
        }

        diskWays.push_back(DiskWay{way.id(), offset, way.refs_size(), oneway});

        google::protobuf::int64 id = 0;

        for (uint64_t index = 0; index < way.refs_size(); index++) {
          id += way.refs(index);

          wayNodes.push_back(id);

          offset++;

          const auto it = usedNodes.find(id);

          const auto lambda = [](uint64_t i, int size) {
            return i == 0 || i == size - 1 ? 2l : 1l;
          };

          if (it == usedNodes.end()) {
            usedNodes.emplace(
                std::piecewise_construct, std::forward_as_tuple(id),
                std::forward_as_tuple(lambda(index, way.refs_size()), false));
          } else {
            it->second.num += lambda(index, way.refs_size());
          }
        }
      }

      if (diskWays.size() != 0) {
        if (ways_size != 0) {
          if (munmap(map_ways, ways_size) == -1) {
            std::cerr << "Failed to unmap ways-file" << std::endl;
            return 1;
          }
        }

        ways_size += sizeof(DiskWay) * diskWays.size();

        if (ftruncate(fd_ways, ways_size) == -1) {
          std::cerr << "failed to alloc memory for ways";
          return 1;
        }
        map_ways = mmap(nullptr, ways_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        fd_ways, 0);
        if (map_ways == MAP_FAILED) {
          std::cerr << "Failed ways mapping" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map_ways) + ways_size -
                        sizeof(DiskWay) * diskWays.size(),
                    diskWays.data(), sizeof(DiskWay) * diskWays.size());
      }

      if (wayNodes.size() != 0) {
        if (size_way_nodes != 0) {
          if (munmap(map_way_nodes, size_way_nodes) == -1) {
            throw std::runtime_error("Failed to unmap ways file");
          }
        }

        size_way_nodes += sizeof(google::protobuf::int64) * wayNodes.size();

        if (ftruncate(fd_way_nodes, size_way_nodes) == -1) {
          std::cerr << "failed to alloc memory for ways";
          return 1;
        }
        map_way_nodes = mmap(nullptr, size_way_nodes, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd_way_nodes, 0);

        if (map_way_nodes == MAP_FAILED) {
          std::cerr << "Failed mapping way-nodes file" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map_way_nodes) + size_way_nodes -
                        sizeof(google::protobuf::int64) * wayNodes.size(),
                    wayNodes.data(),
                    sizeof(google::protobuf::int64) * wayNodes.size());
      }

      const auto& denseNodes = group.dense();

      if (denseNodes.id_size() != denseNodes.lon_size() ||
          denseNodes.id_size() != denseNodes.lat_size()) {
        throw std::runtime_error("Lacking information for dense nodes\n");
      }

      int64_t id = 0;
      int64_t lonInt = 0;
      int64_t latInt = 0;

      std::vector<DiskNode> diskNodes;

      for (int64_t i = 0; i < denseNodes.id_size(); i++) {
        id += denseNodes.id(i);
        latInt += denseNodes.lat(i);
        lonInt += denseNodes.lon(i);

        auto lat = parser::primitive_block::convertCoord(
            primitiveBlock.lat_offset(), primitiveBlock.granularity(), latInt);
        auto lon = parser::primitive_block::convertCoord(
            primitiveBlock.lon_offset(), primitiveBlock.granularity(), lonInt);

        diskNodes.push_back(DiskNode{id, lat, lon});
      }

      if (diskNodes.size() != 0) {
        if (nodes_size != 0) {
          munmap(map_nodes, nodes_size);
        }
        nodes_size += diskNodes.size() * sizeof(DiskNode);
        if (ftruncate(fd_nodes, nodes_size) == -1) {
          std::cerr << "failed to alloc memory";
          return 1;
        }

        map_nodes = mmap(nullptr, nodes_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd_nodes, 0);
        if (map_nodes == MAP_FAILED) {
          std::cerr << "Failed to map nodes file" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map_nodes) + nodes_size -
                        diskNodes.size() * sizeof(DiskNode),
                    diskNodes.data(), diskNodes.size() * sizeof(DiskNode));
      }

      std::vector<DiskRestriction> rests;

      for (auto& rel : group.relations()) {
        const auto& keys = rel.keys();
        const auto& values = rel.vals();

        auto restIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
          return stringtable.s(key) == "restriction";
        });

        if (restIt == keys.end()) {
          continue;
        }

        const auto restrictionType =
            stringtable.s(values[std::distance(keys.begin(), restIt)]);

        const auto& ids = rel.memids();
        const auto& roles = rel.roles_sid();
        const auto& types = rel.types();

        if (rel.memids_size() != 3) {
          continue;
          ;
        }

        google::protobuf::int64 from = -1;
        google::protobuf::int64 to = -1;
        google::protobuf::int64 via = -1;

        google::protobuf::int64 prevId = 0;

        for (uint64_t i = 0; i < rel.memids_size(); i++) {
          auto id = ids[i] + prevId;
          auto role = stringtable.s(roles[i]);
          auto type = types[i];

          if (role == "from" &&
              type == OSMPBF::Relation::MemberType::Relation_MemberType_WAY) {
            from = id;
          } else if (role == "to" && type == OSMPBF::Relation::MemberType::
                                                 Relation_MemberType_WAY) {
            to = id;
          } else if (role == "via" && type == OSMPBF::Relation::MemberType::
                                                  Relation_MemberType_NODE) {
            via = id;
          }

          prevId = id;
        }

        if (from == -1 || to == -1 || via == -1) {
          continue;
          ;
        }

        if (restrictionType != "no_right_turn" &&
            restrictionType != "no_left_turn" &&
            restrictionType != "no_straight_on" &&
            restrictionType != "only_right_turn" &&
            restrictionType != "only_left_turn" &&
            restrictionType != "only_straight_on") {
          continue;
        }

        char t;
        char w;
        if (restrictionType == "no_right_turn") {
          t = 'n';
          w = 'r';
        } else if (restrictionType == "no_left_turn") {
          t = 'n';
          w = 'l';
        } else if (restrictionType == "no_straight_on") {
          t = 'n';
          w = 's';
        } else if (restrictionType == "only_left_turn") {
          t = 'o';
          w = 'l';
        } else if (restrictionType == "only_right_turn") {
          t = 'o';
          w = 'r';
        } else if (restrictionType == "only_straight_on") {
          t = 'o';
          w = 's';
        }

        rests.push_back(DiskRestriction{rel.id(), from, via, to, t, w});
      }

      if (rests.size() != 0) {
        if (rests_size != 0) {
          if (munmap(map_rests, rests_size) == -1) {
            throw std::runtime_error("Failed to unmap ways file");
          }
        }

        rests_size += sizeof(DiskRestriction) * rests.size();

        if (ftruncate(fd_rests, rests_size) == -1) {
          std::cerr << "failed to alloc memory for ways";
          return 1;
        }
        map_rests = mmap(nullptr, rests_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd_rests, 0);
        if (map_rests == MAP_FAILED) {
          std::cerr << "Failed to map restictions-file" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map_rests) + rests_size -
                        sizeof(DiskRestriction) * rests.size(),
                    rests.data(), rests.size() * sizeof(DiskRestriction));
      }
    }
    delete[] uncompressedData;
  }

  if (munmap(mapped_pbf, pbf_size) == -1) {
    std::cerr << "Failed unmapping .pbf.osm" << std::endl;
    return 1;
  }
  if (munmap(map_nodes, nodes_size) == -1) {
    std::cerr << "Failed unmapping nodes (1)" << std::endl;
    return 1;
  }
  if (munmap(map_ways, ways_size) == -1) {
    std::cerr << "Failed unmapping ways" << std::endl;
    return 1;
  }
  if (munmap(map_way_nodes, size_way_nodes) == -1) {
    std::cerr << "Failed unmapping way nodes" << std::endl;
    return 1;
  }
  if (munmap(map_rests, rests_size) == -1) {
    std::cerr << "Failed unmapping restrictions" << std::endl;
    return 1;
  }

  close(fd_nodes);
  close(fd_ways);
  close(fd_way_nodes);
  close(fd_rests);

  fd_nodes = open("./disk-buffers/nodes.bin", O_RDONLY);

  if (fd_nodes == -1) {
    std::cerr << "failed opening fd" << std::endl;
    return 1;
  }

  struct stat sb_nodes;
  if (fstat(fd_nodes, &sb_nodes) == -1) {
    close(fd_nodes);
    throw std::runtime_error("Failed to obtain file size");
  }
  nodes_size = sb_nodes.st_size;

  map_nodes = mmap(nullptr, nodes_size, PROT_READ, MAP_PRIVATE, fd_nodes, 0);

  if (map_nodes == MAP_FAILED) {
    close(fd_nodes);
    throw std::runtime_error("Failed mapping");
  }
  close(fd_nodes);

  size_t nodesCount = nodes_size / sizeof(DiskNode);

  std::unordered_set<uint32_t> pixels;
  std::unordered_map<uint32_t, std::vector<parser::Node>> nodePartitions;

  for (uint64_t i = 0; i < nodesCount; i++) {
    DiskNode* n = reinterpret_cast<DiskNode*>(static_cast<char*>(map_nodes) +
                                              i * sizeof(DiskNode));
    const auto usedNodePairIt = usedNodes.find(n->id);
    if (usedNodePairIt == usedNodes.end()) {
      continue;
    }

    auto theta = (90 - n->lat) * M_PI / 180;
    auto phi = n->lon * M_PI / 180;

    long ipix;

    ang2pix_ring(50, theta, phi, &ipix);

    pixels.insert(ipix);

    const auto nodeBufIt = nodePartitions.find(ipix);

    if (nodeBufIt == nodePartitions.end()) {
      const auto it =
          nodePartitions
              .emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                       std::forward_as_tuple())
              .first;

      it->second.emplace_back(n->id, n->lat, n->lon,
                              usedNodePairIt->second.num);
    } else {
      nodeBufIt->second.emplace_back(n->id, n->lat, n->lon,
                                     usedNodePairIt->second.num);
    }
    usedNodePairIt->second.convert(ipix);
  }

  if (munmap(map_nodes, nodes_size) == -1) {
    std::cerr << "Failed unmapping nodes (2)" << std::endl;
    return 1;
  }

  fd_ways = open("./disk-buffers/ways.bin", O_RDONLY);
  if (fd_ways == -1) {
    std::cerr << "Failed opening fd" << std::endl;
    return 1;
  }
  fd_way_nodes = open("./disk-buffers/way_nodes.bin", O_RDONLY);
  if (fd_way_nodes == -1) {
    std::cerr << "Failed opening fd" << std::endl;
    return 1;
  }

  struct stat sb_ways;
  if (fstat(fd_ways, &sb_ways) == -1) {
    close(fd_ways);
    throw std::runtime_error("Failed to obtain file size");
  }
  ways_size = sb_ways.st_size;

  struct stat sb_way_nodes;
  if (fstat(fd_way_nodes, &sb_way_nodes) == -1) {
    close(fd_way_nodes);
    throw std::runtime_error("Failed to obtain file size");
  }
  size_way_nodes = sb_way_nodes.st_size;

  map_ways = mmap(nullptr, ways_size, PROT_READ, MAP_PRIVATE, fd_ways, 0);
  if (map_ways == MAP_FAILED) {
    close(fd_ways);
    throw std::runtime_error("Failed mapping");
  }
  close(fd_ways);

  map_way_nodes =
      mmap(nullptr, size_way_nodes, PROT_READ, MAP_PRIVATE, fd_way_nodes, 0);
  if (map_way_nodes == MAP_FAILED) {
    close(fd_way_nodes);
    throw std::runtime_error("Failed mapping");
  }
  close(fd_way_nodes);

  size_t waysCount = ways_size / sizeof(DiskWay);

  std::vector<parser::BorderWay> borderWays;
  std::unordered_map<uint32_t, std::vector<parser::Way>> wayPartitions;

  for (uint64_t i = 0; i < waysCount; i++) {
    DiskWay* dw = reinterpret_cast<DiskWay*>(static_cast<char*>(map_ways) +
                                             i * sizeof(DiskWay));
    google::protobuf::int64* refs = reinterpret_cast<google::protobuf::int64*>(
        static_cast<char*>(map_way_nodes) +
        dw->offset * sizeof(google::protobuf::int64));

    const auto refIt = usedNodes.find(*refs);
    google::protobuf::int64 prevValue = *refs;
    if (refIt == usedNodes.end() || !refIt->second.converted) {
      throw std::runtime_error("Missing Node!");
    }

    const auto refPart = refIt->second.num;

    bool partitionable = true;
    for (uint64_t j = 1; j < dw->size; j++) {
      google::protobuf::int64 id = *(refs + j);
      const auto it = usedNodes.find(id);
      if (it == usedNodes.end() || !it->second.converted) {
        throw std::runtime_error("Missing Node (2)");
      }

      if (it->second.num != refPart) {
        partitionable = false;
        break;
      }
    }

    std::vector<google::protobuf::int64> nodesInWay;
    nodesInWay.reserve(dw->size);

    for (uint64_t k = 0; k < dw->size; k++) {
      nodesInWay.push_back(*(refs + k));
    }

    if (!partitionable) {
      auto& bw = borderWays.emplace_back(dw->id, dw->oneway);
      bw.nodes.reserve(dw->size);
      for (uint64_t k = 0; k < dw->size; k++) {
        auto nodeId = *(refs + k);

        const auto uit = usedNodes.find(nodeId);
        if (uit == usedNodes.end() || !uit->second.converted) {
          throw std::runtime_error("Missing Node (3)");
        }

        if (bw.nodes.size() == 0 ||
            bw.nodes[bw.nodes.size() - 1].first != uit->second.num) {
          auto& [_, vec] = bw.nodes.emplace_back(
              std::piecewise_construct, std::forward_as_tuple(uit->second.num),
              std::forward_as_tuple());
          vec.push_back(nodeId);
          continue;
        }

        bw.nodes[bw.nodes.size() - 1].second.push_back(nodeId);
      }

      continue;
    }

    const auto partIt = wayPartitions.find(refPart);

    if (partIt == wayPartitions.end()) {
      const auto& [tup, _] = wayPartitions.emplace(
          std::piecewise_construct, std::forward_as_tuple(refPart),
          std::forward_as_tuple());

      tup->second.emplace_back(dw->id, dw->oneway, nodesInWay.begin(),
                               nodesInWay.end());
      continue;
    }

    partIt->second.emplace_back(dw->id, dw->oneway, nodesInWay.begin(),
                                nodesInWay.end());
  }

  usedNodes.clear();

  if (munmap(map_ways, ways_size) == -1) {
    std::cerr << "Failed unmapping ways" << std::endl;
    return 1;
  }

  if (munmap(map_way_nodes, size_way_nodes) == -1) {
    std::cerr << "Failed unmapping way nodes" << std::endl;
    return 1;
  }

  fd_rests = open("./disk-buffers/restrictions.bin", O_RDONLY);

  struct stat sb_r;
  if (fstat(fd_rests, &sb_r) == -1) {
    close(fd_rests);
    throw std::runtime_error("Failed to obtain file size");
  }
  rests_size = sb_r.st_size;

  map_rests = mmap(nullptr, rests_size, PROT_READ, MAP_PRIVATE, fd_rests, 0);

  if (map_rests == MAP_FAILED) {
    close(fd_rests);
    throw std::runtime_error("Failed mapping");
  }
  close(fd_rests);

  size_t restsCount = rests_size / sizeof(DiskRestriction);

  std::vector<parser::Restriction> restrictions;

  for (uint64_t i = 0; i < restsCount; i++) {
    DiskRestriction* rest = reinterpret_cast<DiskRestriction*>(
        static_cast<char*>(map_rests) + i * sizeof(DiskRestriction));

    std::string restrictionType = "";

    switch (rest->t) {
      case 'o':
        restrictionType += "only_";
        break;
      case 'n':
        restrictionType += "no_";
        break;
      default:
        restrictionType = "error";
        break;
    }

    switch (rest->w) {
      case 'l':
        restrictionType += "left_turn";
        break;
      case 'r':
        restrictionType += "right_turn";
        break;
      case 's':
        restrictionType += "straight_on";
        break;
      default:
        restrictionType = "error";
        break;
    }

    if (restrictionType == "error") {
      continue;
    }

    restrictions.emplace_back(rest->id, rest->from, rest->via, rest->to,
                              restrictionType);
  }

  munmap(map_rests, rests_size);

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

  for (const auto& [_, buf] : wayPartitions) {
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
    const auto nodesIt = nodePartitions.find(ipix);
    const auto waysIt = wayPartitions.find(ipix);

    if (nodesIt == nodePartitions.end()) {
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

    if (waysIt == wayPartitions.end()) {
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

  std::cerr << "Border Edges: " << borderEdges.size() << std::endl;

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