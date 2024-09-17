#include <arpa/inet.h>
#include <chealpix.h>
#include <fcntl.h>
#include <osmpbf/fileformat.pb.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmath>
#include <format>
#include <fstream>
#include <graph/graph.hpp>
#include <graph/invert-source-border.hpp>
#include <graph/invert-source-partition.hpp>
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
#include <types/disk-way.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/used-node.hpp>
#include <types/way.hpp>
#include <utils/hashing.hpp>
#include <utils/libdeflate_decomp.hpp>

const uint64_t HASH_PARTITIONS_NUM = 64;
const uint64_t HASH_MASK = 0x3F;
const uint64_t MAX_NODE_BUF_SIZE = 1'000'000;
const uint64_t MAX_WAY_NODES_BUF_SIZE = 900'000;
const uint64_t MAX_USED_NODES_BUF_SIZE = 900'000;
const uint64_t MAX_WAY_BUF_SIZE = 75000;
const uint64_t MAX_GEO_PART_BUF_SIZE = 600'000;
const uint64_t MAX_NODE_BUF_SIZES_SUM = 48'000'000;
const uint64_t MAX_WAY_BUF_SIZES_SUM = 14'900'000;
// const uint64_t MAX_WAY_NODE_BUF_SIZES_SUM = 115'200'000;

struct DiskNode {
  google::protobuf::int64 id;
  double lat;
  double lon;
};

struct WayHashedNode {
  google::protobuf::int64 id;
  double lat;
  double lon;
  uint64_t used;
  uint64_t offset;
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

  // open and map pbf file for reading

  int fd_pbf = open(argv[1], O_RDONLY);
  if (fd_pbf == -1) {
    std::cerr << "Failed to open .osm.pbf file" << std::endl;
    return 1;
  }
  struct stat sb_pbf;
  if (fstat(fd_pbf, &sb_pbf) == -1) {
    close(fd_pbf);
    std::cerr << "Failed fstat when opening .pbf file" << std::endl;
    return 1;
  }
  size_t pbf_size = sb_pbf.st_size;
  void* mapped_pbf = mmap(nullptr, pbf_size, PROT_READ, MAP_PRIVATE, fd_pbf, 0);
  if (mapped_pbf == MAP_FAILED) {
    close(fd_pbf);
    std::cerr << ".osm.pbf mmap failed" << std::endl;
    return 1;
  }
  close(fd_pbf);
  const char* pbf_data = static_cast<const char*>(mapped_pbf);
  const char* eof_pbf = pbf_data + pbf_size;

  // open node files in node-hash partitions

  std::vector<int> node_fds(HASH_PARTITIONS_NUM);
  std::vector<size_t> node_sizes(HASH_PARTITIONS_NUM);

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::ostringstream stream;
    stream << "./bin/node-hash-partitions/nodes/nodes-" << std::to_string(i)
           << ".bin";
    std::string filename = stream.str();
    node_fds[i] =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (node_fds[i] == -1) {
      std::cerr << "Failed to open file: " << filename << std::endl;
      return 1;
    }
  }

  // open way files in way-hash partitions

  std::vector<int> way_fds(HASH_PARTITIONS_NUM);
  std::vector<size_t> way_sizes(HASH_PARTITIONS_NUM);

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::ostringstream stream;
    stream << "./bin/way-hash-partitions/ways/ways-" << std::to_string(i)
           << ".bin";
    std::string filename = stream.str();
    way_fds[i] =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (way_fds[i] == -1) {
      std::cerr << "Failed to open file: " << filename << std::endl;
      return 1;
    }
  }

  // open way-node files in way-hash partitions

  std::vector<int> way_node_fds(HASH_PARTITIONS_NUM);
  std::vector<size_t> way_node_sizes(HASH_PARTITIONS_NUM);

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::ostringstream stream;
    stream << "./bin/way-hash-partitions/way-nodes/way-nodes-"
           << std::to_string(i) << ".bin";
    std::string filename = stream.str();
    way_node_fds[i] =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (way_node_fds[i] == -1) {
      std::cerr << "Failed to open file: " << filename << std::endl;
      return 1;
    }
  }

  // open used-nodes files in node-hash partitions

  std::vector<int> used_nodes_fds(HASH_PARTITIONS_NUM);
  std::vector<size_t> used_nodes_sizes(HASH_PARTITIONS_NUM);

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::ostringstream stream;
    stream << "./bin/node-hash-partitions/used-nodes/used-nodes-"
           << std::to_string(i) << ".bin";
    std::string filename = stream.str();
    used_nodes_fds[i] =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (used_nodes_fds[i] == -1) {
      std::cerr << "Failed to open file: " << filename << std::endl;
      return 1;
    }
  }

  // open restrictions file for writing

  int fd_rests = open("./bin/restrictions.bin", O_RDWR | O_CREAT | O_TRUNC,
                      S_IRUSR | S_IWUSR);
  if (fd_rests == -1) {
    std::cerr << "failed to open file: ./bin/restrictions.bin" << std::endl;
    return 1;
  }
  size_t rests_size = 0;

  auto parseWaysDuration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::high_resolution_clock::now() -
          std::chrono::high_resolution_clock::now());
  auto parseNodesDuration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::high_resolution_clock::now() -
          std::chrono::high_resolution_clock::now());

  // initialize buffers

  std::vector<std::vector<DiskNode>> nodeHnBuffers;
  nodeHnBuffers.resize(HASH_PARTITIONS_NUM);

  std::vector<std::vector<DiskWay>> wayHwBuffers;
  wayHwBuffers.resize(HASH_PARTITIONS_NUM);

  std::vector<std::vector<google::protobuf::int64>> wayNodeBuffers;
  wayNodeBuffers.resize(HASH_PARTITIONS_NUM);

  std::vector<uint64_t> offsets(HASH_PARTITIONS_NUM);

  std::vector<std::vector<std::pair<google::protobuf::int64, uint16_t>>>
      usedNodesBuffers;
  usedNodesBuffers.resize(HASH_PARTITIONS_NUM);

  std::unordered_set<uint32_t> pixels;

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
      auto beforeWays = std::chrono::high_resolution_clock::now();
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

        uint64_t hash = MurmurHash64A_1(way.id()) & HASH_MASK;

        wayHwBuffers[hash].push_back(
            DiskWay{way.id(), offsets[hash], way.refs_size(), oneway});

        if (wayHwBuffers[hash].size() >= MAX_WAY_BUF_SIZE) {
          way_sizes[hash] += sizeof(DiskWay) * wayHwBuffers[hash].size();

          if (ftruncate(way_fds[hash], way_sizes[hash]) == -1) {
            std::cerr << "failed to truncate memory for ways" << std::endl;
            return 1;
          }
          void* map = mmap(nullptr, way_sizes[hash], PROT_READ | PROT_WRITE,
                           MAP_SHARED, way_fds[hash], 0);
          if (map == MAP_FAILED) {
            std::cerr << "Failed ways mapping" << std::endl;
            return 1;
          }
          std::memcpy(static_cast<char*>(map) + way_sizes[hash] -
                          sizeof(DiskWay) * wayHwBuffers[hash].size(),
                      wayHwBuffers[hash].data(),
                      sizeof(DiskWay) * wayHwBuffers[hash].size());
          if (munmap(map, way_sizes[hash]) == -1) {
            std::cerr << "Failed to unmap ways-file" << std::endl;
            return 1;
          }
          wayHwBuffers[hash].clear();
        }

        google::protobuf::int64 id = 0;

        for (uint64_t index = 0; index < way.refs_size(); index++) {
          id += way.refs(index);

          wayNodeBuffers[hash].push_back(id);

          if (wayNodeBuffers[hash].size() >= MAX_WAY_NODES_BUF_SIZE) {
            way_node_sizes[hash] +=
                sizeof(google::protobuf::int64) * wayNodeBuffers[hash].size();

            if (ftruncate(way_node_fds[hash], way_node_sizes[hash]) == -1) {
              std::cerr << "failed to alloc memory for ways";
              return 1;
            }
            void* map =
                mmap(nullptr, way_node_sizes[hash], PROT_READ | PROT_WRITE,
                     MAP_SHARED, way_node_fds[hash], 0);

            if (map == MAP_FAILED) {
              std::cerr << "Failed mapping way-nodes file" << std::endl;
              return 1;
            }
            std::memcpy(
                static_cast<char*>(map) + way_node_sizes[hash] -
                    sizeof(google::protobuf::int64) *
                        wayNodeBuffers[hash].size(),
                wayNodeBuffers[hash].data(),
                sizeof(google::protobuf::int64) * wayNodeBuffers[hash].size());
            if (munmap(map, way_node_sizes[hash]) == -1) {
              throw std::runtime_error("Failed to unmap ways file");
            }
            wayNodeBuffers[hash].clear();
          }

          offsets[hash]++;

          uint64_t nodeHash = MurmurHash64A_1(id) & HASH_MASK;

          usedNodesBuffers[nodeHash].push_back(std::make_pair(
              index == 0 || index == way.refs_size() - 1 ? -id : id, hash));

          if (usedNodesBuffers[nodeHash].size() >= MAX_USED_NODES_BUF_SIZE) {
            used_nodes_sizes[nodeHash] +=
                sizeof(std::pair<google::protobuf::int64, uint16_t>) *
                usedNodesBuffers[nodeHash].size();

            if (ftruncate(used_nodes_fds[nodeHash],
                          used_nodes_sizes[nodeHash]) == -1) {
              std::cerr << "failed to alloc memory for ways";
              return 1;
            }
            void* map = mmap(nullptr, used_nodes_sizes[nodeHash],
                             PROT_READ | PROT_WRITE, MAP_SHARED,
                             used_nodes_fds[nodeHash], 0);

            if (map == MAP_FAILED) {
              std::cerr << "Failed mapping way-nodes file" << std::endl;
              return 1;
            }
            std::memcpy(
                static_cast<char*>(map) + used_nodes_sizes[nodeHash] -
                    sizeof(std::pair<google::protobuf::int64, uint16_t>) *
                        usedNodesBuffers[nodeHash].size(),
                usedNodesBuffers[nodeHash].data(),
                sizeof(std::pair<google::protobuf::int64, uint16_t>) *
                    usedNodesBuffers[nodeHash].size());
            if (munmap(map, used_nodes_sizes[nodeHash]) == -1) {
              throw std::runtime_error("Failed to unmap ways file");
            }
            usedNodesBuffers[nodeHash].clear();
          }
        }
      }

      parseWaysDuration +=
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::high_resolution_clock::now() - beforeWays);

      auto beforeNodes = std::chrono::high_resolution_clock::now();

      const auto& denseNodes = group.dense();

      if (denseNodes.id_size() != denseNodes.lon_size() ||
          denseNodes.id_size() != denseNodes.lat_size()) {
        std::cerr << "Number of ids/lats/lons unequal in dense nodes"
                  << std::endl;
        return 1;
      }

      int64_t id = 0;
      int64_t lonInt = 0;
      int64_t latInt = 0;

      for (int64_t i = 0; i < denseNodes.id_size(); i++) {
        id += denseNodes.id(i);
        latInt += denseNodes.lat(i);
        lonInt += denseNodes.lon(i);

        auto lat = parser::primitive_block::convertCoord(
            primitiveBlock.lat_offset(), primitiveBlock.granularity(), latInt);
        auto lon = parser::primitive_block::convertCoord(
            primitiveBlock.lon_offset(), primitiveBlock.granularity(), lonInt);

        auto theta = (90 - lat) * M_PI / 180;
        auto phi = lon * M_PI / 180;

        long ipix;

        ang2pix_ring(50, theta, phi, &ipix);

        pixels.insert(ipix);

        uint64_t hash = MurmurHash64A_1(id) & HASH_MASK;

        nodeHnBuffers[hash].push_back(DiskNode{id, lat, lon});

        if (nodeHnBuffers[hash].size() >= MAX_NODE_BUF_SIZE) {
          node_sizes[hash] += nodeHnBuffers[hash].size() * sizeof(DiskNode);
          if (ftruncate(node_fds[hash], node_sizes[hash]) == -1) {
            std::cerr << "failed to alloc memory";
            return 1;
          }

          void* map = mmap(nullptr, node_sizes[hash], PROT_READ | PROT_WRITE,
                           MAP_SHARED, node_fds[hash], 0);
          if (map == MAP_FAILED) {
            std::cerr << "Failed to map nodes file" << std::endl;
            return 1;
          }
          std::memcpy(static_cast<char*>(map) + node_sizes[hash] -
                          nodeHnBuffers[hash].size() * sizeof(DiskNode),
                      nodeHnBuffers[hash].data(),
                      nodeHnBuffers[hash].size() * sizeof(DiskNode));
          if (node_sizes[hash] != 0) {
            munmap(map, node_sizes[hash]);
          }

          nodeHnBuffers[hash].clear();
        }
      }

      parseNodesDuration +=
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::high_resolution_clock::now() - beforeNodes);

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
        rests_size += sizeof(DiskRestriction) * rests.size();

        if (ftruncate(fd_rests, rests_size) == -1) {
          std::cerr << "failed to alloc memory for ways";
          return 1;
        }
        void* map = mmap(nullptr, rests_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd_rests, 0);
        if (map == MAP_FAILED) {
          std::cerr << "Failed to map restictions-file" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map) + rests_size -
                        sizeof(DiskRestriction) * rests.size(),
                    rests.data(), rests.size() * sizeof(DiskRestriction));
      }
    }
    delete[] uncompressedData;
  }

  // unmap .pbf

  if (munmap(mapped_pbf, pbf_size) == -1) {
    std::cerr << "Failed unmapping .pbf.osm" << std::endl;
    return 1;
  }

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    node_sizes[i] += nodeHnBuffers[i].size() * sizeof(DiskNode);
    if (ftruncate(node_fds[i], node_sizes[i]) == -1) {
      std::cerr << "failed to alloc memory";
      return 1;
    }

    void* map = mmap(nullptr, node_sizes[i], PROT_READ | PROT_WRITE, MAP_SHARED,
                     node_fds[i], 0);
    if (map == MAP_FAILED) {
      std::cerr << "Failed to map nodes file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + node_sizes[i] -
                    nodeHnBuffers[i].size() * sizeof(DiskNode),
                nodeHnBuffers[i].data(),
                nodeHnBuffers[i].size() * sizeof(DiskNode));
    if (node_sizes[i] != 0) {
      munmap(map, node_sizes[i]);
    }

    nodeHnBuffers[i].clear();
    close(node_fds[i]);
  }
  nodeHnBuffers.clear();

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    way_sizes[i] += wayHwBuffers[i].size() * sizeof(DiskWay);
    if (ftruncate(way_fds[i], way_sizes[i]) == -1) {
      std::cerr << "failed to alloc memory";
      return 1;
    }

    void* map = mmap(nullptr, way_sizes[i], PROT_READ | PROT_WRITE, MAP_SHARED,
                     way_fds[i], 0);
    if (map == MAP_FAILED) {
      std::cerr << "Failed to map nodes file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + way_sizes[i] -
                    wayHwBuffers[i].size() * sizeof(DiskWay),
                wayHwBuffers[i].data(),
                wayHwBuffers[i].size() * sizeof(DiskWay));
    if (way_sizes[i] != 0) {
      munmap(map, way_sizes[i]);
    }

    wayHwBuffers[i].clear();
    close(way_fds[i]);
  }
  wayHwBuffers.clear();

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    way_node_sizes[i] +=
        wayNodeBuffers[i].size() * sizeof(google::protobuf::int64);
    if (ftruncate(way_node_fds[i], way_node_sizes[i]) == -1) {
      std::cerr << "failed to alloc memory";
      return 1;
    }

    void* map = mmap(nullptr, way_node_sizes[i], PROT_READ | PROT_WRITE,
                     MAP_SHARED, way_node_fds[i], 0);
    if (map == MAP_FAILED) {
      std::cerr << "Failed to map nodes file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + way_node_sizes[i] -
                    wayNodeBuffers[i].size() * sizeof(google::protobuf::int64),
                wayNodeBuffers[i].data(),
                wayNodeBuffers[i].size() * sizeof(google::protobuf::int64));
    if (way_node_sizes[i] != 0) {
      munmap(map, way_node_sizes[i]);
    }

    wayNodeBuffers[i].clear();
    close(way_node_fds[i]);
  }
  wayNodeBuffers.clear();

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    used_nodes_sizes[i] += usedNodesBuffers[i].size() *
                           sizeof(std::pair<google::protobuf::int64, uint16_t>);
    if (ftruncate(used_nodes_fds[i], used_nodes_sizes[i]) == -1) {
      std::cerr << "failed to alloc memory";
      return 1;
    }

    void* map = mmap(nullptr, used_nodes_sizes[i], PROT_READ | PROT_WRITE,
                     MAP_SHARED, used_nodes_fds[i], 0);
    if (map == MAP_FAILED) {
      std::cerr << "Failed to map nodes file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + used_nodes_sizes[i] -
                    usedNodesBuffers[i].size() *
                        sizeof(std::pair<google::protobuf::int64, uint16_t>),
                usedNodesBuffers[i].data(),
                usedNodesBuffers[i].size() *
                    sizeof(std::pair<google::protobuf::int64, uint16_t>));
    if (used_nodes_sizes[i] != 0) {
      munmap(map, used_nodes_sizes[i]);
    }

    usedNodesBuffers[i].clear();
    close(used_nodes_fds[i]);
  }
  usedNodesBuffers.clear();

  close(fd_rests);

  std::cerr << "Time for parsing ways: " << parseWaysDuration.count() << "ms"
            << std::endl;
  std::cerr << "Time for parsing nodes: " << parseNodesDuration.count() << "ms"
            << std::endl;

  auto parsingDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now() - start);
  std::cerr << "Time for .osm.pbf parsing: " << parsingDuration.count() << "ms"
            << std::endl;

  std::vector<int> geo_part_fds(HASH_PARTITIONS_NUM);

  std::vector<size_t> geo_part_sizes(HASH_PARTITIONS_NUM);

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::ostringstream stream;
    stream << "./bin/way-hash-partitions/nodes-geo-partitions/geo-part-"
           << std::to_string(i) << ".bin";
    std::string filename = stream.str();
    geo_part_fds[i] =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (geo_part_fds[i] == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }
  }

  std::vector<std::vector<WayHashedNode>> geoPartsBuffers;
  geoPartsBuffers.resize(HASH_PARTITIONS_NUM);

  std::unordered_map<uint32_t, std::vector<parser::Node>> nodePartitions;
  std::unordered_map<uint32_t, std::pair<int, size_t>> pixelNodeFiles;

  for (long ipix : pixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/nodes/nodes-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
      std::cerr << "Failed to open file.\n";
      return -1;
    }
    pixelNodeFiles.emplace(std::piecewise_construct,
                           std::forward_as_tuple(ipix),
                           std::forward_as_tuple(fd, 0));
  }
  uint32_t maxBufIpix = -1;
  uint32_t maxBufSize = 0;
  uint64_t bufSizesSum = 0;

  std::unordered_set<uint32_t> usedPixels;

  std::unordered_map<uint32_t, uint64_t> nodeOffsets;

  for (uint16_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    auto hashPartitionStart = std::chrono::high_resolution_clock::now();

    std::ostringstream un_fn_stream;
    un_fn_stream << "./bin/node-hash-partitions/used-nodes/used-nodes-"
                 << std::to_string(i) << ".bin";
    std::string un_fn = un_fn_stream.str();
    int un_fd = open(un_fn.data(), O_RDONLY);
    if (un_fd == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    unlink(un_fn.data());

    struct stat sb_un;
    if (fstat(un_fd, &sb_un) == -1) {
      close(un_fd);
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t un_size = sb_un.st_size;

    void* un_map =
        mmap(nullptr, used_nodes_sizes[i], PROT_READ, MAP_PRIVATE, un_fd, 0);
    if (un_map == MAP_FAILED) {
      close(un_fd);
      throw std::runtime_error("Failed mapping");
    }
    close(un_fd);

    size_t usedNodesCount =
        used_nodes_sizes[i] /
        sizeof(std::pair<google::protobuf::int64, uint16_t>);

    ska::flat_hash_map<google::protobuf::int64, uint16_t> usedNodes;
    ska::flat_hash_map<google::protobuf::int64, std::vector<uint16_t>>
        nodeWayPartitions;

    for (uint64_t j = 0; j < usedNodesCount; j++) {
      std::pair<google::protobuf::int64, uint16_t>* un =
          reinterpret_cast<std::pair<google::protobuf::int64, uint16_t>*>(
              static_cast<char*>(un_map) +
              j * sizeof(std::pair<google::protobuf::int64, uint16_t>));

      google::protobuf::int64 id = un->first > 0 ? un->first : -un->first;

      const auto unIt = usedNodes.find(id);

      if (unIt == usedNodes.end()) {
        usedNodes.emplace(id, un->first < 0 ? 2 : 1);
      } else {
        unIt->second += un->first < 0 ? 2 : 1;
      }

      const auto wayHashIt = nodeWayPartitions.find(id);
      if (wayHashIt == nodeWayPartitions.end()) {
        std::vector<uint16_t> vec;
        vec.push_back(un->second);
        nodeWayPartitions.emplace(id, vec);
      } else {
        wayHashIt->second.push_back(un->second);
      }
    }

    if (munmap(un_map, used_nodes_sizes[i]) == -1) {
      std::cerr << "Failed unmapping" << std::endl;
      return 1;
    }

    std::ostringstream stream;
    stream << "./bin/node-hash-partitions/nodes/nodes-" << std::to_string(i)
           << ".bin";
    std::string filename = stream.str();
    int fd_part = open(filename.data(), O_RDONLY);
    if (fd_part == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    unlink(filename.data());

    struct stat sb;
    if (fstat(fd_part, &sb) == -1) {
      close(fd_part);
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t f_size = sb.st_size;

    void* n_map =
        mmap(nullptr, node_sizes[i], PROT_READ, MAP_PRIVATE, fd_part, 0);
    if (n_map == MAP_FAILED) {
      close(fd_part);
      throw std::runtime_error("Failed mapping");
    }
    close(fd_part);

    size_t nodesCount = node_sizes[i] / sizeof(DiskNode);

    for (uint64_t j = 0; j < nodesCount; j++) {
      DiskNode* n = reinterpret_cast<DiskNode*>(static_cast<char*>(n_map) +
                                                j * sizeof(DiskNode));
      const auto usedNodePairIt = usedNodes.find(n->id);
      if (usedNodePairIt == usedNodes.end()) {
        continue;
      }
      auto theta = (90 - n->lat) * M_PI / 180;
      auto phi = n->lon * M_PI / 180;

      long ipix;

      ang2pix_ring(50, theta, phi, &ipix);

      usedPixels.insert(ipix);

      auto nodeBufIt = nodePartitions.find(ipix);

      if (nodeBufIt == nodePartitions.end()) {
        nodeBufIt =
            nodePartitions
                .emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                         std::forward_as_tuple())
                .first;

        nodeBufIt->second.emplace_back(n->id, n->lat, n->lon,
                                       usedNodePairIt->second);
        if (1 > maxBufSize) {
          maxBufSize = 1;
          maxBufIpix = ipix;
        }
      } else {
        nodeBufIt->second.emplace_back(n->id, n->lat, n->lon,
                                       usedNodePairIt->second);
        if (nodeBufIt->second.size() > maxBufSize) {
          maxBufSize = nodeBufIt->second.size();
          maxBufIpix = ipix;
        }
      }

      auto offsetIt = nodeOffsets.find(ipix);
      if (offsetIt == nodeOffsets.end()) {
        offsetIt = nodeOffsets.emplace(ipix, 0).first;
      } else {
        offsetIt->second++;
      }

      bufSizesSum++;

      if (bufSizesSum >= MAX_NODE_BUF_SIZES_SUM) {
        auto fileIt = pixelNodeFiles.find(maxBufIpix);
        const auto maxBufIt = nodePartitions.find(maxBufIpix);

        if (fileIt == pixelNodeFiles.end() ||
            maxBufIt == nodePartitions.end()) {
          std::cerr << maxBufIpix << std::endl;
          std::cerr << "No pixels found" << std::endl;
          return 1;
        }

        fileIt->second.second += sizeof(parser::Node) * maxBufIt->second.size();

        if (ftruncate(fileIt->second.first, fileIt->second.second) == -1) {
          std::cerr << "failed to alloc memory for ways";
          return 1;
        }
        void* map = mmap(nullptr, fileIt->second.second, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fileIt->second.first, 0);

        if (map == MAP_FAILED) {
          std::cerr << "Failed mapping geo-parts file" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map) + fileIt->second.second -
                        sizeof(parser::Node) * maxBufIt->second.size(),
                    maxBufIt->second.data(),
                    sizeof(parser::Node) * maxBufIt->second.size());
        if (munmap(map, fileIt->second.second) == -1) {
          throw std::runtime_error("Failed to unmap ways file");
        }
        bufSizesSum -= maxBufIt->second.size();
        maxBufIt->second.clear();

        maxBufSize = 0;
        maxBufIpix = -1;

        for (const auto& [ipix, buf] : nodePartitions) {
          if (buf.size() >= maxBufSize) {
            maxBufSize = buf.size();
            maxBufIpix = ipix;
          }
        }
      }

      const auto inWaysIt = nodeWayPartitions.find(n->id);

      if (inWaysIt == nodeWayPartitions.end()) {
        std::cerr << "Failing info about ways containing node" << std::endl;
        return 1;
      }

      for (const auto& wayHash : inWaysIt->second) {
        geoPartsBuffers[wayHash].push_back(WayHashedNode{
            n->id, n->lat, n->lon, usedNodePairIt->second, offsetIt->second});

        if (geoPartsBuffers[wayHash].size() >= MAX_GEO_PART_BUF_SIZE) {
          geo_part_sizes[wayHash] +=
              sizeof(WayHashedNode) * geoPartsBuffers[wayHash].size();

          if (ftruncate(geo_part_fds[wayHash], geo_part_sizes[wayHash]) == -1) {
            std::cerr << "failed to alloc memory for ways";
            return 1;
          }
          void* map =
              mmap(nullptr, geo_part_sizes[wayHash], PROT_READ | PROT_WRITE,
                   MAP_SHARED, geo_part_fds[wayHash], 0);

          if (map == MAP_FAILED) {
            std::cerr << "Failed mapping geo-parts file" << std::endl;
            return 1;
          }
          std::memcpy(
              static_cast<char*>(map) + geo_part_sizes[wayHash] -
                  sizeof(WayHashedNode) * geoPartsBuffers[wayHash].size(),
              geoPartsBuffers[wayHash].data(),
              sizeof(WayHashedNode) * geoPartsBuffers[wayHash].size());
          if (munmap(map, geo_part_sizes[wayHash]) == -1) {
            throw std::runtime_error("Failed to unmap ways file");
          }
          geoPartsBuffers[wayHash].clear();
        }
      }
    }

    if (munmap(n_map, node_sizes[i]) == -1) {
      std::cerr << "Failed unmapping file" << std::endl;
      return 1;
    }
  }

  nodeOffsets.clear();

  for (uint64_t wayHash = 0; wayHash < HASH_PARTITIONS_NUM; wayHash++) {
    geo_part_sizes[wayHash] +=
        sizeof(WayHashedNode) * geoPartsBuffers[wayHash].size();

    if (ftruncate(geo_part_fds[wayHash], geo_part_sizes[wayHash]) == -1) {
      std::cerr << "failed to alloc memory for ways";
      return 1;
    }
    void* map = mmap(nullptr, geo_part_sizes[wayHash], PROT_READ | PROT_WRITE,
                     MAP_SHARED, geo_part_fds[wayHash], 0);

    if (map == MAP_FAILED) {
      std::cerr << "Failed mapping geo-parts file (rest) " << wayHash
                << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + geo_part_sizes[wayHash] -
                    sizeof(WayHashedNode) * geoPartsBuffers[wayHash].size(),
                geoPartsBuffers[wayHash].data(),
                sizeof(WayHashedNode) * geoPartsBuffers[wayHash].size());
    if (munmap(map, geo_part_sizes[wayHash]) == -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    geoPartsBuffers[wayHash].clear();
    close(geo_part_fds[wayHash]);
  }

  for (long ipix : pixels) {
    auto fileIt = pixelNodeFiles.find(ipix);

    if (fileIt == pixelNodeFiles.end()) {
      std::cerr << "No pixels found" << std::endl;
      return 1;
    }

    const auto nodeBufIt = nodePartitions.find(ipix);
    if (nodeBufIt == nodePartitions.end()) {
      // std::cerr << "No partition for this ipix" << std::endl;
      continue;
    }

    fileIt->second.second += sizeof(parser::Node) * nodeBufIt->second.size();

    if (ftruncate(fileIt->second.first, fileIt->second.second) == -1) {
      std::cerr << "failed to alloc memory for ways";
      return 1;
    }
    void* map = mmap(nullptr, fileIt->second.second, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fileIt->second.first, 0);

    if (map == MAP_FAILED) {
      std::cerr << "Failed mapping geo-parts file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + fileIt->second.second -
                    sizeof(parser::Node) * nodeBufIt->second.size(),
                nodeBufIt->second.data(),
                sizeof(parser::Node) * nodeBufIt->second.size());
    if (munmap(map, fileIt->second.second) == -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    nodeBufIt->second.clear();
    close(fileIt->second.first);
  }

  pixelNodeFiles.clear();
  pixels.clear();
  nodePartitions.clear();
  geoPartsBuffers.clear();

  std::vector<parser::Edge> borderEdges;
  std::unordered_map<uint32_t, std::vector<parser::Edge>> edgePartitions;

  // fd ways, size ways, fd way-nodes, size way-nodes, way-nodes offset
  std::unordered_map<uint32_t, std::tuple<int, size_t>> pixelWayFiles;
  std::unordered_map<uint32_t, std::vector<google::protobuf::int64>>
      wayNodePartitions;

  uint64_t edgeBufSizesSum = 0;
  uint64_t maxEdgeBufSize = 0;
  long maxEdgeBufIpix = -1;

  uint64_t wayNodeBufSizesSum = 0;
  uint64_t maxWayNodeBufSize = 0;
  uint32_t maxWayNodeBufIpix = -1;

  google::protobuf::int64 edgeId = 0;

  for (long ipix : usedPixels) {
    std::ostringstream stream_e;
    stream_e << "./bin/geo-partitions/edges/edges-" << std::to_string(ipix)
             << ".bin";
    std::string filename_e = stream_e.str();
    int fd_e =
        open(filename_e.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_e == -1) {
      std::cerr << "Failed to open file.\n";
      return -1;
    }

    pixelWayFiles.emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                          std::forward_as_tuple(fd_e, 0));
  }

  std::vector<parser::Node> borderNodes;

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::ostringstream gp_fn_stream;
    gp_fn_stream << "./bin/way-hash-partitions/nodes-geo-partitions/geo-part-"
                 << std::to_string(i) << ".bin";
    std::string gp_fn = gp_fn_stream.str();
    int gp_fd = open(gp_fn.data(), O_RDONLY);
    if (gp_fd == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    unlink(gp_fn.data());

    struct stat sb_gp;
    if (fstat(gp_fd, &sb_gp) == -1) {
      close(gp_fd);
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t gp_size = sb_gp.st_size;

    void* gp_map = mmap(nullptr, gp_size, PROT_READ, MAP_PRIVATE, gp_fd, 0);
    if (gp_map == MAP_FAILED) {
      close(gp_fd);
      throw std::runtime_error("Failed mapping");
    }
    close(gp_fd);

    size_t nodeCount = gp_size / sizeof(WayHashedNode);

    ska::flat_hash_map<google::protobuf::int64, WayHashedNode*>
        nodeGeoPartitions;
    for (uint64_t j = 0; j < nodeCount; j++) {
      WayHashedNode* gp = reinterpret_cast<WayHashedNode*>(
          static_cast<char*>(gp_map) + j * sizeof(WayHashedNode));
      nodeGeoPartitions.emplace(gp->id, gp);
    }

    std::ostringstream w_fn_stream;
    w_fn_stream << "./bin/way-hash-partitions/ways/ways-" << std::to_string(i)
                << ".bin";
    std::string w_fn = w_fn_stream.str();
    int w_fd = open(w_fn.data(), O_RDONLY);
    if (w_fd == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    unlink(w_fn.data());

    struct stat sb_w;
    if (fstat(w_fd, &sb_w) == -1) {
      close(w_fd);
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t w_size = sb_w.st_size;

    void* w_map = mmap(nullptr, w_size, PROT_READ, MAP_PRIVATE, w_fd, 0);
    if (w_map == MAP_FAILED) {
      close(gp_fd);
      throw std::runtime_error("Failed mapping");
    }
    close(w_fd);

    size_t wayCount = w_size / sizeof(DiskWay);

    // ------------------------------------------

    std::ostringstream wn_fn_stream;
    wn_fn_stream << "./bin/way-hash-partitions/way-nodes/way-nodes-"
                 << std::to_string(i) << ".bin";
    std::string wn_fn = wn_fn_stream.str();
    int wn_fd = open(wn_fn.data(), O_RDONLY);
    if (wn_fd == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    unlink(wn_fn.data());

    struct stat sb_wn;
    if (fstat(wn_fd, &sb_wn) == -1) {
      close(wn_fd);
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t wn_size = sb_wn.st_size;

    void* wn_map = mmap(nullptr, wn_size, PROT_READ, MAP_PRIVATE, wn_fd, 0);
    if (wn_map == MAP_FAILED) {
      close(wn_fd);
      throw std::runtime_error("Failed mapping");
    }
    close(wn_fd);

    for (uint64_t j = 0; j < wayCount; j++) {
      DiskWay* way = reinterpret_cast<DiskWay*>(static_cast<char*>(w_map) +
                                                j * sizeof(DiskWay));

      google::protobuf::int64* refs =
          reinterpret_cast<google::protobuf::int64*>(
              static_cast<char*>(wn_map) +
              way->offset * sizeof(google::protobuf::int64));

      const auto sourcePairIt = nodeGeoPartitions.find(*refs);
      google::protobuf::int64 prevValue = *refs;
      if (sourcePairIt == nodeGeoPartitions.end()) {
        std::cerr << "Way id: " << way->id << std::endl;
        std::cerr << "Node id: " << prevValue << std::endl;
        throw std::runtime_error("Missing Node!");
      }

      auto sourceNodePtr = sourcePairIt->second;
      auto prevNodePtr = sourceNodePtr;

      long sourceIpix;

      auto theta = (90 - sourceNodePtr->lat) * M_PI / 180;
      auto phi = sourceNodePtr->lon * M_PI / 180;

      ang2pix_ring(50, theta, phi, &sourceIpix);

      double cost = 0;

      // bool partitionable = true;
      for (uint64_t k = 1; k < way->size; k++) {
        google::protobuf::int64 id = *(refs + k);
        const auto it = nodeGeoPartitions.find(id);
        if (it == nodeGeoPartitions.end()) {
          throw std::runtime_error("Missing Node (2)");
        }

        auto nodePtr = it->second;

        cost += geopointsDistance(
            std::make_pair(prevNodePtr->lat, prevNodePtr->lon),
            std::make_pair(nodePtr->lat, nodePtr->lon));

        prevNodePtr = nodePtr;

        if (nodePtr->used <= 1) {
          continue;
        }

        auto theta = (90 - nodePtr->lat) * M_PI / 180;
        auto phi = nodePtr->lon * M_PI / 180;

        long ipix;

        ang2pix_ring(50, theta, phi, &ipix);

        if (ipix == sourceIpix) {
          const auto partFilesIt = pixelWayFiles.find(sourceIpix);

          if (partFilesIt == pixelWayFiles.end()) {
            std::cerr << "No way files for pixel found" << std::endl;
            return 1;
          }

          const auto partIt = edgePartitions.find(sourceIpix);

          if (partIt == edgePartitions.end()) {
            const auto& [tup, _] = edgePartitions.emplace(
                std::piecewise_construct, std::forward_as_tuple(sourceIpix),
                std::forward_as_tuple());
            tup->second.push_back(parser::Edge{
                id, way->id, 0, sourceNodePtr->id, sourceNodePtr->offset,
                nodePtr->id, nodePtr->offset, cost, sourceIpix});
            edgeBufSizesSum++;
            if (!way->oneway) {
              tup->second.push_back(parser::Edge{
                  id, way->id, 0, nodePtr->id, nodePtr->offset,
                  sourceNodePtr->id, sourceNodePtr->offset, cost, sourceIpix});
              edgeBufSizesSum++;
            }
            if (tup->second.size() > edgeBufSizesSum) {
              maxEdgeBufSize = tup->second.size();
              maxEdgeBufIpix = sourceIpix;
            }
          } else {
            partIt->second.push_back(parser::Edge{
                id, way->id, 0, sourceNodePtr->id, sourceNodePtr->offset,
                nodePtr->id, nodePtr->offset, cost, sourceIpix});
            edgeBufSizesSum++;
            if (!way->oneway) {
              partIt->second.push_back(parser::Edge{
                  id, way->id, 0, nodePtr->id, nodePtr->offset,
                  sourceNodePtr->id, sourceNodePtr->offset, cost, sourceIpix});
              edgeBufSizesSum++;
            }
            if (partIt->second.size() >= maxEdgeBufSize) {
              maxEdgeBufSize = partIt->second.size();
              maxEdgeBufIpix = sourceIpix;
            }
          }

          if (edgeBufSizesSum >= MAX_WAY_BUF_SIZES_SUM) {
            auto fileIt = pixelWayFiles.find(maxEdgeBufIpix);
            const auto maxBufIt = edgePartitions.find(maxEdgeBufIpix);

            if (fileIt == pixelWayFiles.end() ||
                maxBufIt == edgePartitions.end()) {
              std::cerr << maxBufIpix << std::endl;
              std::cerr << "No pixels found" << std::endl;
              return 1;
            }

            get<1>(fileIt->second) +=
                sizeof(parser::Edge) * maxBufIt->second.size();

            if (ftruncate(get<0>(fileIt->second), get<1>(fileIt->second)) ==
                -1) {
              std::cerr << "failed to alloc memory for ways";
              return 1;
            }
            void* map =
                mmap(nullptr, get<1>(fileIt->second), PROT_READ | PROT_WRITE,
                     MAP_SHARED, get<0>(fileIt->second), 0);

            if (map == MAP_FAILED) {
              std::cerr << "Failed mapping geo-parts file" << std::endl;
              return 1;
            }
            std::memcpy(static_cast<char*>(map) + get<1>(fileIt->second) -
                            sizeof(parser::Edge) * maxBufIt->second.size(),
                        maxBufIt->second.data(),
                        sizeof(parser::Edge) * maxBufIt->second.size());
            if (munmap(map, get<1>(fileIt->second)) == -1) {
              throw std::runtime_error("Failed to unmap ways file");
            }
            edgeBufSizesSum -= maxBufIt->second.size();
            maxBufIt->second.clear();

            maxEdgeBufSize = 0;
            maxEdgeBufIpix = -1;

            for (const auto& [ipix, buf] : edgePartitions) {
              if (buf.size() >= maxEdgeBufSize) {
                maxEdgeBufSize = buf.size();
                maxEdgeBufIpix = ipix;
              }
            }
          }
        } else {
          borderNodes.emplace_back(sourceNodePtr->id, sourceNodePtr->lat,
                                   sourceNodePtr->lon, sourceNodePtr->used);
          borderNodes.emplace_back(nodePtr->id, nodePtr->lat, nodePtr->lon,
                                   nodePtr->used);
          borderEdges.push_back(parser::Edge{
              id, way->id, 0, sourceNodePtr->id, borderNodes.size() - 2,
              nodePtr->id, borderNodes.size() - 1, cost, 0});
          if (!way->oneway) {
            borderEdges.push_back(parser::Edge{
                id, way->id, 0, nodePtr->id, borderNodes.size() - 1,
                sourceNodePtr->id, borderNodes.size() - 2, cost, 0});
          }
        }

        sourceNodePtr = nodePtr;
        sourceIpix = ipix;
        cost = 0;
        edgeId++;
      }
    }

    if (munmap(gp_map, gp_size) == -1) {
      std::cerr << "Failed unmapping edges file" << std::endl;
      return 1;
    }

    if (munmap(w_map, w_size) == -1) {
      std::cerr << "Failed unmapping ways file" << std::endl;
      return 1;
    }

    if (munmap(wn_map, wn_size) == -1) {
      std::cerr << "Failed unmapping way-nodes file" << std::endl;
      return 1;
    }
  }

  for (auto& [ipix, buf] : edgePartitions) {
    auto fileIt = pixelWayFiles.find(ipix);

    if (fileIt == pixelWayFiles.end()) {
      std::cerr << maxBufIpix << std::endl;
      std::cerr << "No pixels found" << std::endl;
      return 1;
    }

    get<1>(fileIt->second) += sizeof(parser::Edge) * buf.size();

    if (ftruncate(get<0>(fileIt->second), get<1>(fileIt->second)) == -1) {
      std::cerr << "failed to alloc memory for ways";
      return 1;
    }
    void* map = mmap(nullptr, get<1>(fileIt->second), PROT_READ | PROT_WRITE,
                     MAP_SHARED, get<0>(fileIt->second), 0);

    if (map == MAP_FAILED) {
      std::cerr << "Failed mapping geo-parts file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + get<1>(fileIt->second) -
                    sizeof(parser::Edge) * buf.size(),
                buf.data(), sizeof(parser::Edge) * buf.size());
    if (munmap(map, get<1>(fileIt->second)) == -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    buf.clear();
  }

  edgePartitions.clear();

  pixelWayFiles.clear();

  std::cerr << "Border nodes: " << borderNodes.size() << std::endl;

  std::cerr << "Partitioned edges" << std::endl;

  fd_rests = open("./bin/restrictions.bin", O_RDONLY);

  unlink("./bin/restrictions.bin");

  struct stat sb_r;
  if (fstat(fd_rests, &sb_r) == -1) {
    close(fd_rests);
    throw std::runtime_error("Failed to obtain file size");
  }
  rests_size = sb_r.st_size;

  void* map_rests =
      mmap(nullptr, rests_size, PROT_READ, MAP_PRIVATE, fd_rests, 0);

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

  if (munmap(map_rests, rests_size) == -1) {
    std::cerr << "Failed unmapping restrictions file" << std::endl;
    return 1;
  }

  std::cerr << "Read restrictions from file" << std::endl;

  std::unordered_multimap<google::protobuf::int64, parser::Restriction*>
      onlyRestrictionsByTo;
  std::unordered_map<
      std::tuple<google::protobuf::int64, google::protobuf::int64>,
      parser::Restriction*>
      noRestrictionsHash;

  parser::processing::hash_restrictions(restrictions, onlyRestrictionsByTo,
                                        noRestrictionsHash);

  std::cerr << "Restrictions hashed" << std::endl;

  std::unordered_multimap<
      std::tuple<google::protobuf::int64, google::protobuf::int64>,
      parser::Restriction*>
      onlyRestrictionsMap;

  std::unordered_map<uint32_t, int> geoWayPartsFds;

  for (const auto ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/edges/edges-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      std::cerr << "Failed to open file.\n";
      return -1;
    }
    geoWayPartsFds.emplace(ipix, fd);
  }

  for (const auto& ipix : usedPixels) {
    const auto fdIt = geoWayPartsFds.find(ipix);

    if (fdIt == geoWayPartsFds.end()) {
      std::cerr << "File descriptor not found for geo partition ways"
                << std::endl;
      return 1;
    }

    struct stat sb;
    if (fstat(fdIt->second, &sb) == -1) {
      close(fdIt->second);
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t size = sb.st_size;

    if (size == 0) {
      continue;
    }

    void* map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fdIt->second, 0);

    if (map == MAP_FAILED) {
      close(fdIt->second);
      throw std::runtime_error("Failed mapping");
    }
    close(fdIt->second);

    size_t wayCount = size / sizeof(parser::Edge);
    for (uint64_t i = 0; i < wayCount; i++) {
      parser::Edge* edge = reinterpret_cast<parser::Edge*>(
          static_cast<char*>(map) + i * sizeof(parser::Edge));
      const auto& [first, second] =
          onlyRestrictionsByTo.equal_range(edge->wayId);
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

    if (munmap(map, size) == -1) {
      std::cerr << "Failed unmapping geo partition ways" << std::endl;
      return 1;
    }
  }

  geoWayPartsFds.clear();

  for (const auto& edge : borderEdges) {
    const auto& [first, second] = onlyRestrictionsByTo.equal_range(edge.wayId);
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

  onlyRestrictionsByTo.clear();

  std::unordered_map<google::protobuf::int64, std::vector<parser::Edge*>>
      borderGraph;

  for (auto& edge : borderEdges) {
    auto entryIt = borderGraph.find(edge.sourceNodeId);
    if (entryIt == borderGraph.end()) {
      auto vecIt = borderGraph
                       .emplace(std::piecewise_construct,
                                std::forward_as_tuple(edge.sourceNodeId),
                                std::forward_as_tuple())
                       .first;
      vecIt->second.push_back(&edge);
    } else {
      entryIt->second.push_back(&edge);
    }
  }

  // Open fds (read-only) for node and way geo-partitions
  std::unordered_map<uint32_t, std::tuple<int, int>> geoPartFds;

  for (const auto ipix : usedPixels) {
    std::ostringstream stream1;
    stream1 << "./bin/geo-partitions/nodes/nodes-" << std::to_string(ipix)
            << ".bin";
    std::string filename_n = stream1.str();
    int fd_n = open(filename_n.data(), O_RDONLY);
    if (fd_n == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }

    unlink(filename_n.data());

    std::ostringstream stream2;
    stream2 << "./bin/geo-partitions/edges/edges-" << std::to_string(ipix)
            << ".bin";
    std::string filename_e = stream2.str();
    int fd_e = open(filename_e.data(), O_RDONLY);
    if (fd_e == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }

    unlink(filename_e.data());

    geoPartFds.emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                       std::forward_as_tuple(fd_n, fd_e));
  }

  // process partitions

  google::protobuf::int64 expEdgeId = 0;
  uint64_t sum = 0;

  for (auto ipix : usedPixels) {
    const auto fdsIt = geoPartFds.find(ipix);
    if (fdsIt == geoPartFds.end()) {
      std::cerr << "No file descriptors for partition" << std::endl;
      return 1;
    }

    // open file with nodes in this partition
    struct stat sb_n;
    if (fstat(get<0>(fdsIt->second), &sb_n) == -1) {
      close(get<0>(fdsIt->second));
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t size_n = sb_n.st_size;

    void* map_n =
        mmap(nullptr, size_n, PROT_READ, MAP_PRIVATE, get<0>(fdsIt->second), 0);

    if (map_n == MAP_FAILED) {
      close(get<0>(fdsIt->second));
      throw std::runtime_error("Failed mapping");
    }
    close(get<0>(fdsIt->second));

    // open file with ways in this partition
    struct stat sb_e;
    if (fstat(get<1>(fdsIt->second), &sb_e) == -1) {
      close(get<1>(fdsIt->second));
      throw std::runtime_error("Failed to obtain file size");
    }
    size_t size_e = sb_e.st_size;

    if (size_e == 0) {
      continue;
    }

    void* map_e =
        mmap(nullptr, size_e, PROT_READ, MAP_PRIVATE, get<1>(fdsIt->second), 0);

    if (map_e == MAP_FAILED) {
      close(get<1>(fdsIt->second));
      throw std::runtime_error("Failed mapping");
    }
    close(get<1>(fdsIt->second));

    size_t edgeCount = size_e / sizeof(parser::Edge);

    std::vector<parser::ExpandedEdge> expEdgesBuf;

    parser::graph::Graph graph(
        reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e)), edgeCount);

    for (uint64_t j = 0; j < edgeCount; j++) {
      auto edgePtr = reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e) +
                                                     j * sizeof(parser::Edge));
      parser::graph::invert::applyRestrictions(
          edgePtr, graph.graph(),
          reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
          onlyRestrictionsMap, noRestrictionsHash, expEdgeId, expEdgesBuf);
    }

    for (auto& edge : borderEdges) {
      parser::graph::invert::applyRestrictionsSourceBorder(
          &edge, graph.graph(),
          reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
          borderNodes.data(), onlyRestrictionsMap, noRestrictionsHash,
          expEdgeId, expEdgesBuf);
    }

    for (uint64_t j = 0; j < edgeCount; j++) {
      auto edgePtr = reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e) +
                                                     j * sizeof(parser::Edge));
      parser::graph::invert::applyRestrictionsSourcePartition(
          edgePtr, borderGraph,
          reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
          borderNodes.data(), onlyRestrictionsMap, noRestrictionsHash,
          expEdgeId, expEdgesBuf);
    }

    if (munmap(map_n, size_n) == -1) {
      std::cerr << "Failed unmapping nodes" << std::endl;
      return 1;
    }

    if (munmap(map_e, size_e) == -1) {
      std::cerr << "Failed unmapping edges" << std::endl;
      return 1;
    }

    if (expEdgesBuf.size() == 0) {
      continue;
    }

    std::ostringstream stream_ee;
    stream_ee << "./bin/geo-partitions/exp-edges/exp-edges-"
              << std::to_string(ipix) << ".bin";
    std::string filename_ee = stream_ee.str();
    int fd_ee =
        open(filename_ee.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_ee == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }
    if (ftruncate(fd_ee, expEdgesBuf.size() * sizeof(parser::ExpandedEdge)) ==
        -1) {
      std::cerr << "failed to alloc memory for edges";
      return 1;
    }
    void* map_ee =
        mmap(nullptr, expEdgesBuf.size() * sizeof(parser::ExpandedEdge),
             PROT_READ | PROT_WRITE, MAP_SHARED, fd_ee, 0);

    if (map_ee == MAP_FAILED) {
      std::cerr << "Failed mapping exp-edges file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map_ee), expEdgesBuf.data(),
                sizeof(parser::ExpandedEdge) * expEdgesBuf.size());
    if (munmap(map_ee, sizeof(parser::ExpandedEdge) * expEdgesBuf.size()) ==
        -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    close(fd_ee);

    sum += expEdgesBuf.size();
  }

  geoPartFds.clear();

  std::vector<parser::ExpandedEdge> borderExpEdges;

  for (auto& sourceEdge : borderEdges) {
    parser::graph::invert::applyRestrictions(
        &sourceEdge, borderGraph, borderNodes.data(), onlyRestrictionsMap,
        noRestrictionsHash, expEdgeId, borderExpEdges);
  }

  std::cerr << "Total expanded edges: " << sum + borderExpEdges.size()
            << std::endl;

  for (uint32_t ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/exp-edges/exp-edges-"
           << std::to_string(ipix) << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }

    unlink(filename.data());
    close(fd);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << "Execution time: " << duration.count() << "ms" << std::endl;

  return 0;
}