#include <arpa/inet.h>
#include <chealpix.h>
#include <fcntl.h>
#include <osmpbf/fileformat.pb.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmath>
#include <disk/file-read.hpp>
#include <disk/utils.hpp>
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
const uint64_t MAX_WAY_NODES_BUF_SIZE = 57'600'000;
const uint64_t MAX_USED_NODES_BUF_SIZE = 57'600'000;
const uint64_t MAX_WAY_BUF_SIZE = 4'800'000;
const uint64_t MAX_REDUCED_NODES_BUF_SIZE = 600'000;
const uint64_t MAX_GEO_PART_BUF_SIZE = 600'000;
const uint64_t MAX_NODE_BUF_SIZES_SUM = 48'000'000;
const uint64_t MAX_WAY_BUF_SIZES_SUM = 1'638'400;
const uint64_t MAX_BORDER_NODES_SIZES_SUM = 2'000'000;
const uint64_t MAX_BORDER_EDGES_SIZES_SUM = 330'000;
const uint64_t MAX_HASH_TUPLES_BUF_SIZE = 1'000'000;
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

  // benchmarks

  auto start = std::chrono::high_resolution_clock::now();
  uint64_t nodesNum = 0;
  uint64_t usedWaysNum = 0;
  uint64_t totalWaysNum = 0;
  uint64_t totalWayNodes = 0;
  uint64_t usedWayNodes = 0;
  uint64_t totalRestrictions = 0;
  uint64_t usedRestrictions = 0;
  uint64_t totalDecompressedSize = 0;

  if (argc < 2) {
    std::cerr << "No input file passed" << std::endl;
    return 1;
  }

  // open and map pbf file for reading

  int fd_pbf = open(argv[1], O_RDONLY);

  FileRead pbf(fd_pbf);

  const char* pbf_data;
  const char* eof_pbf;

  try {
    pbf_data = static_cast<const char*>(pbf.mmap_file());
    eof_pbf = pbf.eof();
    pbf.close_fd();
  } catch (std::runtime_error& e) {
    pbf.close_fd();
    std::cerr << "Failed opening pbf file" << std::endl;
    return 1;
  }

  // open node files in node-hash partitions

  std::vector<int> node_fds(HASH_PARTITIONS_NUM);
  std::vector<uint64_t> node_sizes(HASH_PARTITIONS_NUM);

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::string filename =
        numFilename("./bin/node-hash-partitions/nodes/nodes-", i, "bin");
    node_fds[i] =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (node_fds[i] == -1) {
      std::cerr << "Failed to open file: " << filename << std::endl;
      return 1;
    }
  }

  int way_fd =
      open("./bin/ways.bin", O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (way_fd == -1) {
    std::cerr << "Failed to open file: "
              << "./bin/ways.bin" << std::endl;
    return 1;
  }

  uint64_t ways_size = 0;

  int way_node_fd = open("./bin/way-nodes.bin", O_RDWR | O_CREAT | O_TRUNC,
                         S_IRUSR | S_IWUSR);
  if (way_node_fd == -1) {
    std::cerr << "Failed to open file: "
              << "./bin/way-nodes.bin" << std::endl;
    return 1;
  }

  uint64_t way_node_size = 0;

  // open used-nodes files in node-hash partitions

  std::vector<int> used_nodes_fds(HASH_PARTITIONS_NUM);
  std::vector<uint64_t> used_nodes_sizes(HASH_PARTITIONS_NUM);

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::string filename = numFilename(
        "./bin/node-hash-partitions/used-nodes/used-nodes-", i, "bin");
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
  uint64_t rests_size = 0;

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

  std::vector<DiskWay> waysBuffer;
  std::vector<std::pair<google::protobuf::int64, uint64_t>> wayNodes;

  std::vector<uint64_t> hashPartOffsets(HASH_PARTITIONS_NUM);
  uint64_t wayNodesOffset = 0;

  std::vector<std::vector<google::protobuf::int64>> usedNodesBuffers;
  usedNodesBuffers.resize(HASH_PARTITIONS_NUM);

  std::unordered_set<uint32_t> pixels;

  while (pbf_data < eof_pbf) {
    uint32_t headerSize;
    std::memcpy(&headerSize, pbf_data, sizeof(headerSize));
    headerSize = ntohl(headerSize);

    totalDecompressedSize += sizeof(uint32_t);

    pbf_data += sizeof(headerSize);

    OSMPBF::BlobHeader header;
    if (!header.ParseFromArray(pbf_data, headerSize)) {
      std::cerr << "Failed to parse BlobHeader" << std::endl;
      return 1;
    }

    pbf_data += headerSize;
    totalDecompressedSize += headerSize;

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

    totalDecompressedSize += header.datasize();
    totalDecompressedSize += blob.raw_size();

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
        totalWaysNum++;
        totalWayNodes += way.refs_size();
        const auto& keys = way.keys();
        const auto& values = way.vals();

        if (way.id() == 18752656) {
          std::cerr << "Way: " << way.refs_size() << std::endl;
        }

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

        usedWaysNum++;
        usedWayNodes += way.refs_size();

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

        // uint64_t hash = MurmurHash64A_1(way.id()) & HASH_MASK;

        // wayHwBuffers[hash].push_back(
        //     DiskWay{way.id(), offsets[hash], way.refs_size(), oneway});

        if (way.id() == 18752656) {
          std::cerr << "Offset: " << wayNodesOffset << std::endl;
        }

        waysBuffer.push_back(
            DiskWay{way.id(), wayNodesOffset, way.refs_size(), oneway});
        wayNodesOffset += way.refs_size();

        if (waysBuffer.size() >= MAX_WAY_BUF_SIZE) {
          ways_size += sizeof(DiskWay) * waysBuffer.size();

          if (ftruncate(way_fd, ways_size) == -1) {
            std::cerr << "failed to truncate memory for ways" << std::endl;
            return 1;
          }
          void* map = mmap(nullptr, ways_size, PROT_READ | PROT_WRITE,
                           MAP_SHARED, way_fd, 0);
          if (map == MAP_FAILED) {
            std::cerr << "Failed ways mapping" << std::endl;
            return 1;
          }
          std::memcpy(static_cast<char*>(map) + ways_size -
                          sizeof(DiskWay) * waysBuffer.size(),
                      waysBuffer.data(), sizeof(DiskWay) * waysBuffer.size());
          if (munmap(map, ways_size) == -1) {
            std::cerr << "Failed to unmap ways-file" << std::endl;
            return 1;
          }
          waysBuffer.clear();
        }

        google::protobuf::int64 id = 0;

        for (uint64_t index = 0; index < way.refs_size(); index++) {
          id += way.refs(index);

          if (id == 211669) {
            std::cerr << way.id() << std::endl;
            return 1;
          }

          uint64_t hash = MurmurHash64A_1(id) & HASH_MASK;

          wayNodes.emplace_back(id, hashPartOffsets[hash]);
          hashPartOffsets[hash]++;

          if (wayNodes.size() >= MAX_WAY_NODES_BUF_SIZE) {
            way_node_size +=
                sizeof(std::pair<google::protobuf::int64, uint64_t>) *
                wayNodes.size();

            if (ftruncate(way_node_fd, way_node_size) == -1) {
              std::cerr << "failed to alloc memory for ways";
              return 1;
            }
            void* map = mmap(nullptr, way_node_size, PROT_READ | PROT_WRITE,
                             MAP_SHARED, way_node_fd, 0);

            if (map == MAP_FAILED) {
              std::cerr << "Failed mapping way-nodes file" << std::endl;
              return 1;
            }
            std::memcpy(
                static_cast<char*>(map) + way_node_size -
                    sizeof(std::pair<google::protobuf::int64, uint64_t>) *
                        wayNodes.size(),
                wayNodes.data(),
                sizeof(std::pair<google::protobuf::int64, uint64_t>) *
                    wayNodes.size());
            if (munmap(map, way_node_size) == -1) {
              throw std::runtime_error("Failed to unmap ways file");
            }
            wayNodes.clear();
          }

          // offsets[hash]++;

          // uint64_t nodeHash = MurmurHash64A_1(id) & HASH_MASK;

          usedNodesBuffers[hash].push_back(
              index == 0 || index == way.refs_size() - 1 ? -id : id);

          if (usedNodesBuffers[hash].size() >= MAX_USED_NODES_BUF_SIZE) {
            used_nodes_sizes[hash] +=
                sizeof(google::protobuf::int64) * usedNodesBuffers[hash].size();

            if (ftruncate(used_nodes_fds[hash], used_nodes_sizes[hash]) == -1) {
              std::cerr << "failed to alloc memory for ways";
              return 1;
            }
            void* map =
                mmap(nullptr, used_nodes_sizes[hash], PROT_READ | PROT_WRITE,
                     MAP_SHARED, used_nodes_fds[hash], 0);

            if (map == MAP_FAILED) {
              std::cerr << "Failed mapping way-nodes file" << std::endl;
              return 1;
            }
            std::memcpy(static_cast<char*>(map) + used_nodes_sizes[hash] -
                            sizeof(google::protobuf::int64) *
                                usedNodesBuffers[hash].size(),
                        usedNodesBuffers[hash].data(),
                        sizeof(google::protobuf::int64) *
                            usedNodesBuffers[hash].size());
            if (munmap(map, used_nodes_sizes[hash]) == -1) {
              throw std::runtime_error("Failed to unmap ways file");
            }
            usedNodesBuffers[hash].clear();
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

      nodesNum += denseNodes.id_size();

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
        totalRestrictions++;
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
        }

        if (restrictionType != "no_right_turn" &&
            restrictionType != "no_left_turn" &&
            restrictionType != "no_straight_on" &&
            restrictionType != "only_right_turn" &&
            restrictionType != "only_left_turn" &&
            restrictionType != "only_straight_on") {
          continue;
        }

        usedRestrictions++;

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

  try {
    pbf.unmap_file();
  } catch (std::runtime_error& error) {
    std::cerr << error.what() << std::endl;
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

    if (munmap(map, node_sizes[i]) == -1) {
      std::cerr << "Failed unmapping file" << std::endl;
      return 1;
    }

    nodeHnBuffers[i].clear();
    close(node_fds[i]);
  }
  nodeHnBuffers.clear();
  node_fds.clear();
  node_sizes.clear();

  ways_size += waysBuffer.size() * sizeof(DiskWay);
  if (ftruncate(way_fd, ways_size) == -1) {
    std::cerr << "failed to alloc memory";
    return 1;
  }

  void* map_w =
      mmap(nullptr, ways_size, PROT_READ | PROT_WRITE, MAP_SHARED, way_fd, 0);
  if (map_w == MAP_FAILED) {
    std::cerr << "Failed to map nodes file" << std::endl;
    return 1;
  }
  std::memcpy(static_cast<char*>(map_w) + ways_size -
                  waysBuffer.size() * sizeof(DiskWay),
              waysBuffer.data(), waysBuffer.size() * sizeof(DiskWay));
  if (munmap(map_w, ways_size) == -1) {
    std::cerr << "Failed unmapping ways" << std::endl;
    return 1;
  }
  waysBuffer.clear();
  close(way_fd);

  way_node_size +=
      wayNodes.size() * sizeof(std::pair<google::protobuf::int64, uint64_t>);
  if (ftruncate(way_node_fd, way_node_size) == -1) {
    std::cerr << "failed to alloc memory";
    return 1;
  }

  void* map_wn = mmap(nullptr, way_node_size, PROT_READ | PROT_WRITE,
                      MAP_SHARED, way_node_fd, 0);
  if (map_wn == MAP_FAILED) {
    std::cerr << "Failed to map nodes file" << std::endl;
    return 1;
  }
  std::memcpy(
      static_cast<char*>(map_wn) + way_node_size -
          wayNodes.size() *
              sizeof(std::pair<google::protobuf::int64, uint64_t>),
      wayNodes.data(),
      wayNodes.size() * sizeof(std::pair<google::protobuf::int64, uint64_t>));
  if (munmap(map_wn, way_node_size) == -1) {
    std::cerr << "Failed unmapping ways" << std::endl;
    return 1;
  }

  wayNodes.clear();
  close(way_node_fd);
  hashPartOffsets.clear();

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    used_nodes_sizes[i] +=
        usedNodesBuffers[i].size() * sizeof(google::protobuf::int64);
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
    std::memcpy(
        static_cast<char*>(map) + used_nodes_sizes[i] -
            usedNodesBuffers[i].size() * sizeof(google::protobuf::int64),
        usedNodesBuffers[i].data(),
        usedNodesBuffers[i].size() * sizeof(google::protobuf::int64));

    if (munmap(map, used_nodes_sizes[i]) == -1) {
      std::cerr << "Failed unmapping used-nodes" << std::endl;
      return 1;
    }

    usedNodesBuffers[i].clear();
    close(used_nodes_fds[i]);
  }
  usedNodesBuffers.clear();
  used_nodes_fds.clear();
  used_nodes_sizes.clear();

  close(fd_rests);

  std::cerr << "Time for parsing ways: " << parseWaysDuration.count() << "ms"
            << std::endl;
  std::cerr << "Time for parsing nodes: " << parseNodesDuration.count() << "ms"
            << std::endl;

  auto parsingDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now() - start);
  std::cerr << "Time for .osm.pbf parsing: " << parsingDuration.count() << "ms"
            << std::endl;

  std::cerr << "Total data size after decompression: " << totalDecompressedSize
            << std::endl;
  std::cerr << "Total nodes: " << nodesNum << std::endl;
  std::cerr << "Total ways: " << totalWaysNum << " (used: " << usedWaysNum
            << ")" << std::endl;
  std::cerr << "Total way-nodes: " << totalWayNodes
            << " (used: " << usedWayNodes << ")" << std::endl;
  std::cerr << "Total restrictions: " << totalRestrictions
            << " (used: " << usedRestrictions << ")" << std::endl;

  auto partNodesStart = std::chrono::high_resolution_clock::now();

  std::unordered_map<uint32_t, std::vector<parser::Node>> nodePartitions;
  std::unordered_map<uint32_t, std::pair<int, uint64_t>> pixelNodeFiles;

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

    std::ostringstream rn_fn_stream;
    rn_fn_stream << "./bin/node-hash-partitions/reduced-nodes/nodes-"
                 << std::to_string(i) << ".bin";
    std::string rn_fn = rn_fn_stream.str();
    int rn_fd =
        open(rn_fn.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (rn_fd == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    uint64_t rn_size = 0;

    std::ostringstream un_fn_stream;
    un_fn_stream << "./bin/node-hash-partitions/used-nodes/used-nodes-"
                 << std::to_string(i) << ".bin";
    std::string un_fn = un_fn_stream.str();
    int un_fd = open(un_fn.data(), O_RDONLY);
    if (un_fd == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    struct stat sb_un;
    if (fstat(un_fd, &sb_un) == -1) {
      close(un_fd);
      throw std::runtime_error("Failed to obtain file size");
    }
    uint64_t un_size = sb_un.st_size;

    void* un_map = mmap(nullptr, un_size, PROT_READ, MAP_PRIVATE, un_fd, 0);
    if (un_map == MAP_FAILED) {
      close(un_fd);
      throw std::runtime_error("Failed mapping");
    }
    close(un_fd);

    uint64_t usedNodesCount =
        used_nodes_sizes[i] / sizeof(google::protobuf::int64);

    ska::flat_hash_map<google::protobuf::int64, uint16_t> usedNodes;

    for (uint64_t j = 0; j < usedNodesCount; j++) {
      google::protobuf::int64* un = reinterpret_cast<google::protobuf::int64*>(
          static_cast<char*>(un_map) + j * sizeof(google::protobuf::int64));

      google::protobuf::int64 id = *un > 0 ? *un : -(*un);

      const auto unIt = usedNodes.find(id);

      if (unIt == usedNodes.end()) {
        usedNodes.emplace(id, *un < 0 ? 2 : 1);
      } else {
        unIt->second += *un < 0 ? 2 : 1;
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
    uint64_t f_size = sb.st_size;

    void* n_map = mmap(nullptr, f_size, PROT_READ, MAP_PRIVATE, fd_part, 0);
    if (n_map == MAP_FAILED) {
      close(fd_part);
      throw std::runtime_error("Failed mapping");
    }
    close(fd_part);

    uint64_t nodesCount = f_size / sizeof(DiskNode);

    std::vector<WayHashedNode> nodesBuf;

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

      bufSizesSum++;

      auto offsetIt = nodeOffsets.find(ipix);
      if (offsetIt == nodeOffsets.end()) {
        offsetIt = nodeOffsets.emplace(ipix, 0).first;
      } else {
        offsetIt->second++;
      }

      nodesBuf.push_back(WayHashedNode{
          n->id, n->lat, n->lon, usedNodePairIt->second, offsetIt->second});

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

      if (nodesBuf.size() >= MAX_REDUCED_NODES_BUF_SIZE) {
        rn_size += sizeof(WayHashedNode) * nodesBuf.size();

        if (ftruncate(rn_fd, rn_size) == -1) {
          std::cerr << "failed to alloc memory for ways";
          return 1;
        }
        void* map = mmap(nullptr, rn_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                         rn_fd, 0);

        if (map == MAP_FAILED) {
          std::cerr << "Failed mapping geo-parts file" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map) + rn_size -
                        sizeof(WayHashedNode) * nodesBuf.size(),
                    nodesBuf.data(), sizeof(WayHashedNode) * nodesBuf.size());
        if (munmap(map, rn_size) == -1) {
          throw std::runtime_error("Failed to unmap ways file");
        }

        nodesBuf.clear();
      }
    }

    if (munmap(n_map, node_sizes[i]) == -1) {
      std::cerr << "Failed unmapping file" << std::endl;
      return 1;
    }

    rn_size += sizeof(WayHashedNode) * nodesBuf.size();

    if (ftruncate(rn_fd, rn_size) == -1) {
      std::cerr << "failed to alloc memory for ways";
      return 1;
    }
    void* map_rn =
        mmap(nullptr, rn_size, PROT_READ | PROT_WRITE, MAP_SHARED, rn_fd, 0);

    if (map_rn == MAP_FAILED) {
      std::cerr << "Failed mapping geo-parts file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map_rn) + rn_size -
                    sizeof(WayHashedNode) * nodesBuf.size(),
                nodesBuf.data(), sizeof(WayHashedNode) * nodesBuf.size());
    if (munmap(map_rn, rn_size) == -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    close(rn_fd);

    nodesBuf.clear();
    usedNodes.clear();

    rn_fd = open(rn_fn.data(), O_RDONLY);

    if (rn_fd == -1) {
      close(rn_fd);
      return 1;
    }

    map_rn = mmap(nullptr, rn_size, PROT_READ, MAP_SHARED, rn_fd, 0);

    if (map_rn == MAP_FAILED) {
      std::cerr << "Failed mapping reduced nodes file" << std::endl;
      return 1;
    }
    close(rn_fd);

    uint64_t reducedNodesCount = rn_size / sizeof(WayHashedNode);

    ska::flat_hash_map<google::protobuf::int64, WayHashedNode*> nodesHash;

    for (uint64_t j = 0; j < reducedNodesCount; j++) {
      WayHashedNode* n = reinterpret_cast<WayHashedNode*>(
          static_cast<char*>(map_rn) + j * sizeof(WayHashedNode));

      nodesHash.emplace(n->id, n);
    }

    std::ostringstream tuples_stream;

    tuples_stream << "./bin/node-hash-partitions/way-node-tuples/tuples-"
                  << std::to_string(i) << ".bin";
    std::string tuples_filename = tuples_stream.str();
    int tuples_fd = open(tuples_filename.data(), O_RDWR | O_CREAT | O_TRUNC,
                         S_IRUSR | S_IWUSR);
    if (tuples_fd == -1) {
      std::cerr << "Failed fd for tuples" << std::endl;
      return 1;
    }
    uint64_t tuples_size = 0;

    un_fd = open(un_fn.data(), O_RDONLY);
    if (un_fd == -1) {
      std::cerr << "Failed opening fd" << std::endl;
      return 1;
    }

    unlink(un_fn.data());

    // if (fstat(un_fd, &sb_un) == -1) {
    //   close(un_fd);
    //   throw std::runtime_error("Failed to obtain file size");
    // }
    // un_size = sb_un.st_size;

    un_map = mmap(nullptr, un_size, PROT_READ, MAP_PRIVATE, un_fd, 0);
    if (un_map == MAP_FAILED) {
      close(un_fd);
      throw std::runtime_error("Failed mapping");
    }
    close(un_fd);

    usedNodesCount = un_size / sizeof(google::protobuf::int64);

    std::vector<std::tuple<uint64_t, double, double, uint64_t>> nodeTuplesBuf;

    for (uint64_t j = 0; j < usedNodesCount; j++) {
      google::protobuf::int64* n = reinterpret_cast<google::protobuf::int64*>(
          static_cast<char*>(un_map) + j * sizeof(google::protobuf::int64));

      auto id = *n > 0 ? *n : -(*n);

      const auto nodeIt = nodesHash.find(id);

      if (nodeIt == nodesHash.end()) {
        std::cerr << j << std::endl;
        std::cerr << "No node found for used node" << std::endl;
        return 1;
      }

      nodeTuplesBuf.emplace_back(nodeIt->second->offset, nodeIt->second->lat,
                                 nodeIt->second->lon, nodeIt->second->used);

      if (nodeTuplesBuf.size() >= MAX_HASH_TUPLES_BUF_SIZE) {
        tuples_size += sizeof(std::tuple<uint64_t, double, double, uint64_t>) *
                       nodeTuplesBuf.size();

        if (ftruncate(tuples_fd, tuples_size) == -1) {
          std::cerr << "failed to alloc memory for ways";
          return 1;
        }
        void* map = mmap(nullptr, tuples_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, tuples_fd, 0);

        if (map == MAP_FAILED) {
          std::cerr << "Failed mapping geo-parts file" << std::endl;
          return 1;
        }
        std::memcpy(static_cast<char*>(map) + tuples_size -
                        sizeof(std::tuple<uint64_t, double, double, uint64_t>) *
                            nodeTuplesBuf.size(),
                    nodeTuplesBuf.data(),
                    sizeof(std::tuple<uint64_t, double, double, uint64_t>) *
                        nodeTuplesBuf.size());
        if (munmap(map, tuples_size) == -1) {
          throw std::runtime_error("Failed to unmap ways file");
        }

        nodeTuplesBuf.clear();
      }
    }
    tuples_size += sizeof(std::tuple<uint64_t, double, double, uint64_t>) *
                   nodeTuplesBuf.size();

    if (ftruncate(tuples_fd, tuples_size) == -1) {
      std::cerr << "failed to alloc memory for ways";
      return 1;
    }
    void* map = mmap(nullptr, tuples_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                     tuples_fd, 0);

    if (map == MAP_FAILED) {
      std::cerr << "Failed mapping geo-parts file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + tuples_size -
                    sizeof(std::tuple<uint64_t, double, double, uint64_t>) *
                        nodeTuplesBuf.size(),
                nodeTuplesBuf.data(),
                sizeof(std::tuple<uint64_t, double, double, uint64_t>) *
                    nodeTuplesBuf.size());
    if (munmap(map, tuples_size) == -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    close(tuples_fd);

    if (munmap(un_map, un_size) == -1) {
      return 1;
    }

    nodeTuplesBuf.clear();
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
      std::cerr << "failed to alloc memory for nodes (2)";
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

  nodeOffsets.clear();
  nodePartitions.clear();
  pixelNodeFiles.clear();

  std::cerr << "Partitioned nodes" << std::endl;

  int fd_ways_read = open("./bin/ways.bin", O_RDONLY);

  if (fd_ways_read == -1) {
    std::cerr << "Failed opening ways for reading" << std::endl;
    return 1;
  }

  struct stat sb_ways_read;
  if (fstat(fd_ways_read, &sb_ways_read) == -1) {
    std::cerr << "Failed fstat for reading ways" << std::endl;
    return 1;
  }

  uint64_t ways_total_size = sb_ways_read.st_size;

  uint64_t WAY_CHUNK_SIZE = 49'152'000;
  uint64_t WAYS_IN_CHUNK = WAY_CHUNK_SIZE / sizeof(DiskWay);

  void* ways_map =
      // mmap(nullptr,
      //      ways_total_size < WAY_CHUNK_SIZE ? ways_total_size :
      //      WAY_CHUNK_SIZE, PROT_READ, MAP_PRIVATE, fd_ways_read, 0);
      mmap(nullptr, ways_total_size, PROT_READ, MAP_PRIVATE, fd_ways_read, 0);

  if (ways_map == MAP_FAILED) {
    close(fd_ways_read);
    std::cerr << "Failed mapping" << std::endl;
    return 1;
  }
  // close(fd_ways_read);

  uint64_t totalWaysCount = ways_total_size / sizeof(DiskWay);

  int fd_way_nodes_read = open("./bin/way-nodes.bin", O_RDONLY);

  if (fd_way_nodes_read == -1) {
    std::cerr << "Failed opening way-nodes for reading" << std::endl;
    return 1;
  }

  struct stat sb_way_nodes_read;
  if (fstat(fd_way_nodes_read, &sb_way_nodes_read) == -1) {
    std::cerr << "Failed fstat for reading ways" << std::endl;
    return 1;
  }

  uint64_t way_nodes_total_size = sb_way_nodes_read.st_size;

  const uint64_t WAY_NODES_CHUNK_SIZE = WAY_CHUNK_SIZE * 3;
  const uint64_t WAY_NODES_IN_CHUNK =
      WAY_NODES_CHUNK_SIZE /
      sizeof(std::pair<google::protobuf::int64, uint64_t>);
  void* way_nodes_map = mmap(nullptr, way_nodes_total_size, PROT_READ,
                             MAP_PRIVATE, fd_way_nodes_read, 0);

  std::cerr << way_nodes_total_size << std::endl;

  if (way_nodes_map == MAP_FAILED) {
    close(fd_way_nodes_read);
    std::cerr << "Failed mapping" << std::endl;
    return 1;
  }

  std::vector<std::tuple<int, uint64_t, void*, uint64_t>> nodeTupleFiles;
  nodeTupleFiles.resize(HASH_PARTITIONS_NUM);

  const uint64_t TUPLES_CHUNK_SIZE = 327'680;
  const uint64_t TUPLES_IN_CHUNK =
      TUPLES_CHUNK_SIZE /
      sizeof(std::tuple<uint64_t, double, double, uint64_t>);
  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::ostringstream stream;
    stream << "./bin/node-hash-partitions/way-node-tuples/tuples-"
           << std::to_string(i) << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      std::cerr << "Failed opening file" << std::endl;
      return 1;
    }
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
      std::cerr << "Failed obtaining file size: " << filename << std::endl;
      return 1;
    }
    uint64_t size = sb.st_size;
    void* map;
    if (size > TUPLES_CHUNK_SIZE) {
      map = mmap(nullptr, TUPLES_CHUNK_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    } else {
      map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    }
    if (map == MAP_FAILED) {
      close(fd);
      std::cerr << "Failed mapping initial chunk" << std::endl;
      return 1;
    }
    // close(fd);
    // unlink(filename.data());
    nodeTupleFiles[i] = std::make_tuple(fd, size, map, 0);
  }

  std::unordered_map<uint32_t, std::tuple<int, uint64_t>> pixelWayFiles;
  std::unordered_map<uint32_t,
                     std::tuple<int, uint64_t, int, uint64_t, uint64_t>>
      pixelBorderFiles;

  for (long ipix : usedPixels) {
    std::ostringstream stream_e;
    stream_e << "./bin/geo-partitions/edges/edges-" << std::to_string(ipix)
             << ".bin";
    std::string filename_e = stream_e.str();
    int fd_e =
        open(filename_e.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_e == -1) {
      std::cerr << "Failed to open file" << std::endl;
      return 1;
    }

    std::ostringstream stream_bn;
    stream_bn << "./bin/geo-partitions/border-nodes/nodes-"
              << std::to_string(ipix) << ".bin";
    std::string filename_bn = stream_bn.str();
    int fd_bn =
        open(filename_bn.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_bn == -1) {
      std::cerr << "Failed to open file : " << filename_bn << std::endl;
      return 1;
    }

    std::ostringstream stream_be;
    stream_be << "./bin/geo-partitions/border-edges/edges-"
              << std::to_string(ipix) << ".bin";
    std::string filename_be = stream_be.str();
    int fd_be =
        open(filename_be.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_be == -1) {
      std::cerr << "Failed to open file : " << filename_be << std::endl;
      return 1;
    }

    pixelWayFiles.emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                          std::forward_as_tuple(fd_e, 0));
    pixelBorderFiles.emplace(std::piecewise_construct,
                             std::forward_as_tuple(ipix),
                             std::forward_as_tuple(fd_bn, 0, fd_be, 0, 0));
  }

  google::protobuf::int64 edgeId = 0;
  std::unordered_map<uint32_t, std::vector<parser::Edge>> edgePartitions;
  std::unordered_map<uint32_t, std::vector<parser::Node>> borderNodesPartitions;
  std::unordered_map<uint32_t, std::vector<parser::Edge>> borderEdgePartitions;
  uint64_t edgeBufSizesSum = 0;
  uint64_t maxEdgeBufSize = 0;
  long maxEdgeBufIpix = -1;
  uint64_t borderNodesBufSizesSum = 0;
  uint64_t maxBorderNodesBufSize = 0;
  long maxBorderNodesIpix = -1;

  uint64_t borderEdgeBufSizesSum = 0;
  uint64_t maxBorderEdgeBufSize = 0;
  long maxBorderEdgeIpix = -1;

  uint64_t totalBorderEdges = 0;
  uint64_t totalBorderNodes = 0;

  std::cerr << "Mapped all" << std::endl;

  for (uint64_t i = 0; i < totalWaysCount; i++) {
    DiskWay way = *reinterpret_cast<DiskWay*>(static_cast<char*>(ways_map) +
                                              i * sizeof(DiskWay));

    if (way.id == 18752656) {
      std::cerr << "size: " << way.size << std::endl;
      std::cerr << "offset: " << way.offset << std::endl;
      std::cerr << "way nodes count: "
                << way_nodes_total_size /
                       sizeof(std::pair<google::protobuf::int64, uint64_t>)
                << std::endl;
    }

    std::pair<google::protobuf::int64, uint64_t>* wayNodePairs =
        reinterpret_cast<std::pair<google::protobuf::int64, uint64_t>*>(
            static_cast<char*>(way_nodes_map) +
            way.offset * sizeof(std::pair<google::protobuf::int64, uint64_t>));

    auto sourceWayNodePair = *wayNodePairs;

    uint64_t sourceHash = MurmurHash64A_1(wayNodePairs->first) & HASH_MASK;

    if (get<3>(nodeTupleFiles[sourceHash]) != 0 &&
        get<3>(nodeTupleFiles[sourceHash]) % TUPLES_IN_CHUNK == 0) {
      if (munmap(get<2>(nodeTupleFiles[sourceHash]), TUPLES_CHUNK_SIZE) == -1) {
        std::cerr << "Failed unmapping tuple chunk" << std::endl;
        return 1;
      }
      get<2>(nodeTupleFiles[sourceHash]) = mmap(
          nullptr,
          get<1>(nodeTupleFiles[sourceHash]) -
                      get<3>(nodeTupleFiles[sourceHash]) *
                          sizeof(
                              std::tuple<uint64_t, double, double, uint64_t>) <
                  TUPLES_CHUNK_SIZE
              ? get<1>(nodeTupleFiles[sourceHash]) -
                    get<3>(nodeTupleFiles[sourceHash]) *
                        sizeof(std::tuple<uint64_t, double, double, uint64_t>)
              : TUPLES_CHUNK_SIZE,
          PROT_READ, MAP_PRIVATE, get<0>(nodeTupleFiles[sourceHash]),
          get<3>(nodeTupleFiles[sourceHash]) *
              sizeof(std::tuple<uint64_t, double, double, uint64_t>));
      if (get<2>(nodeTupleFiles[sourceHash]) == MAP_FAILED) {
        std::cerr << "Failed mapping tuple chunk (dynamic)" << std::endl;
        return 1;
      }
    }

    std::tuple<uint64_t, double, double, uint64_t> sourceTuple =
        *reinterpret_cast<std::tuple<uint64_t, double, double, uint64_t>*>(
            static_cast<char*>(get<2>(nodeTupleFiles[sourceHash])) +
            (wayNodePairs->second % TUPLES_IN_CHUNK) *
                sizeof(std::tuple<uint64_t, double, double, uint64_t>));
    get<3>(nodeTupleFiles[sourceHash])++;

    auto prevTuple = sourceTuple;

    long sourceIpix;

    auto theta = (90 - get<1>(sourceTuple)) * M_PI / 180;
    auto phi = get<2>(sourceTuple) * M_PI / 180;

    ang2pix_ring(50, theta, phi, &sourceIpix);

    double cost = 0;

    for (uint64_t j = 1; j < way.size; j++) {
      if (way.id == 18752656 && j == 63) {
        std::cerr << "Processing" << std::endl;
        std::cerr << (wayNodePairs + 63)->first << " "
                  << (wayNodePairs + 63)->second << std::endl;
      }
      auto wayNodePair = *(wayNodePairs + j);
      auto nodeId = wayNodePair.first;
      auto hash = MurmurHash64A_1(nodeId) & HASH_MASK;

      auto tupleHashPos = get<3>(nodeTupleFiles[hash]);
      auto tupleHashMap = get<2>(nodeTupleFiles[hash]);
      if (tupleHashPos != 0 && tupleHashPos % TUPLES_IN_CHUNK == 0) {
        if (munmap(tupleHashMap, TUPLES_CHUNK_SIZE) == -1) {
          std::cerr << "Failed unmapping tuple chunk" << std::endl;
          return 1;
        }
        get<2>(nodeTupleFiles[hash]) = mmap(
            nullptr,
            get<1>(nodeTupleFiles[hash]) -
                        tupleHashPos * sizeof(std::tuple<uint64_t, double,
                                                         double, uint64_t>) <
                    TUPLES_CHUNK_SIZE
                ? get<1>(nodeTupleFiles[hash]) -
                      tupleHashPos *
                          sizeof(std::tuple<uint64_t, double, double, uint64_t>)
                : TUPLES_CHUNK_SIZE,
            PROT_READ, MAP_PRIVATE, get<0>(nodeTupleFiles[hash]),
            get<3>(nodeTupleFiles[hash]) *
                sizeof(std::tuple<uint64_t, double, double, uint64_t>));
        if (get<2>(nodeTupleFiles[hash]) == MAP_FAILED) {
          std::cerr << "Failed mapping tuple chunk (dynamic)" << std::endl;
          return 1;
        }
      }

      std::tuple<uint64_t, double, double, uint64_t> wayNodeTuple =
          *reinterpret_cast<std::tuple<uint64_t, double, double, uint64_t>*>(
              static_cast<char*>(get<2>(nodeTupleFiles[hash])) +
              (wayNodePair.second % TUPLES_IN_CHUNK) *
                  sizeof(std::tuple<uint64_t, double, double, uint64_t>));
      get<3>(nodeTupleFiles[hash])++;

      cost += geopointsDistance(
          std::make_pair(get<1>(prevTuple), get<2>(prevTuple)),
          std::make_pair(get<1>(wayNodeTuple), get<2>(wayNodeTuple)));

      prevTuple = wayNodeTuple;

      if (get<3>(wayNodeTuple) <= 1) {
        continue;
      }

      auto theta = (90 - get<1>(wayNodeTuple)) * M_PI / 180;
      auto phi = get<2>(wayNodeTuple) * M_PI / 180;

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
              edgeId, way.id, 0, sourceWayNodePair.first, get<0>(sourceTuple),
              wayNodePair.first, get<0>(wayNodeTuple), cost, sourceIpix});
          edgeBufSizesSum++;
          if (!way.oneway) {
            edgeId++;
            tup->second.push_back(
                parser::Edge{edgeId, way.id, 0, wayNodePair.first,
                             get<0>(wayNodeTuple), sourceWayNodePair.first,
                             get<0>(sourceTuple), cost, sourceIpix});
            edgeBufSizesSum++;
          }
          if (tup->second.size() > edgeBufSizesSum) {
            maxEdgeBufSize = tup->second.size();
            maxEdgeBufIpix = sourceIpix;
          }
        } else {
          partIt->second.push_back(parser::Edge{
              edgeId, way.id, 0, sourceWayNodePair.first, get<0>(sourceTuple),
              wayNodePair.first, get<0>(wayNodeTuple), cost, sourceIpix});
          edgeBufSizesSum++;
          if (!way.oneway) {
            edgeId++;
            partIt->second.push_back(
                parser::Edge{edgeId, way.id, 0, wayNodePair.first,
                             get<0>(wayNodeTuple), sourceWayNodePair.first,
                             get<0>(sourceTuple), cost, sourceIpix});
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

          if (ftruncate(get<0>(fileIt->second), get<1>(fileIt->second)) == -1) {
            std::cerr << "failed to alloc memory for edges";
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
        totalBorderEdges++;
        totalBorderNodes += 2;
        auto bnSourcePartIt = borderNodesPartitions.find(sourceIpix);
        if (bnSourcePartIt == borderNodesPartitions.end()) {
          bnSourcePartIt = borderNodesPartitions
                               .emplace(std::piecewise_construct,
                                        std::forward_as_tuple(sourceIpix),
                                        std::forward_as_tuple())
                               .first;
        }

        bnSourcePartIt->second.emplace_back(
            sourceWayNodePair.first, get<1>(sourceTuple), get<2>(sourceTuple),
            get<3>(sourceTuple));
        bnSourcePartIt->second.emplace_back(
            wayNodePair.first, get<1>(wayNodeTuple), get<2>(wayNodeTuple),
            get<3>(wayNodeTuple));

        borderNodesBufSizesSum += 2;

        if (bnSourcePartIt->second.size() > maxBorderNodesBufSize) {
          maxBorderNodesBufSize = bnSourcePartIt->second.size();
          maxBorderNodesIpix = sourceIpix;
        }

        const auto partFilesIt = pixelBorderFiles.find(sourceIpix);

        if (partFilesIt == pixelBorderFiles.end()) {
          std::cerr << "Source ipix: " << sourceIpix << std::endl;
          std::cerr << "Way id: " << way.id << std::endl;
          std::cerr << get<0>(sourceTuple) << get<1>(sourceTuple) << " "
                    << get<2>(sourceTuple) << std::endl;
          std::cerr << "No border files for pixel found" << std::endl;
          return 1;
        }

        get<4>(partFilesIt->second) += 2;

        auto bnTargetPartIt = borderNodesPartitions.find(ipix);
        if (bnTargetPartIt == borderNodesPartitions.end()) {
          bnTargetPartIt =
              borderNodesPartitions
                  .emplace(std::piecewise_construct,
                           std::forward_as_tuple(ipix), std::forward_as_tuple())
                  .first;
        }

        bnTargetPartIt->second.emplace_back(
            sourceWayNodePair.first, get<1>(sourceTuple), get<2>(sourceTuple),
            get<3>(sourceTuple));
        bnTargetPartIt->second.emplace_back(
            wayNodePair.first, get<1>(wayNodeTuple), get<2>(wayNodeTuple),
            get<3>(wayNodeTuple));

        borderNodesBufSizesSum += 2;

        if (bnTargetPartIt->second.size() > maxBorderNodesBufSize) {
          maxBorderNodesBufSize = bnTargetPartIt->second.size();
          maxBorderNodesIpix = ipix;
        }

        const auto partFilesTargetIt = pixelBorderFiles.find(ipix);

        if (partFilesTargetIt == pixelBorderFiles.end()) {
          std::cerr << "No border files for pixel found" << std::endl;
          return 1;
        }

        get<4>(partFilesTargetIt->second) += 2;

        if (borderNodesBufSizesSum >= MAX_BORDER_NODES_SIZES_SUM) {
          const auto fileIt = pixelBorderFiles.find(maxBorderNodesIpix);
          const auto maxBufIt = borderNodesPartitions.find(maxBorderNodesIpix);

          if (fileIt == pixelBorderFiles.end() ||
              maxBufIt == borderNodesPartitions.end()) {
            std::cerr << maxBufIpix << std::endl;
            std::cerr << "No pixels found" << std::endl;
            return 1;
          }

          get<1>(fileIt->second) +=
              sizeof(parser::Node) * maxBufIt->second.size();

          if (ftruncate(get<0>(fileIt->second), get<1>(fileIt->second)) == -1) {
            std::cerr << "failed to alloc memory for border nodes";
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
                          sizeof(parser::Node) * maxBufIt->second.size(),
                      maxBufIt->second.data(),
                      sizeof(parser::Node) * maxBufIt->second.size());
          if (munmap(map, get<1>(fileIt->second)) == -1) {
            throw std::runtime_error("Failed to unmap ways file");
          }
          borderNodesBufSizesSum -= maxBufIt->second.size();
          maxBufIt->second.clear();

          maxBorderNodesBufSize = 0;
          maxBorderNodesIpix = -1;

          for (const auto& [ipix, buf] : borderNodesPartitions) {
            if (buf.size() >= maxBorderNodesBufSize) {
              maxBorderNodesBufSize = buf.size();
              maxBorderNodesIpix = ipix;
            }
          }
        }

        auto beSourcePartIt = borderEdgePartitions.find(sourceIpix);
        if (beSourcePartIt == borderEdgePartitions.end()) {
          beSourcePartIt = borderEdgePartitions
                               .emplace(std::piecewise_construct,
                                        std::forward_as_tuple(sourceIpix),
                                        std::forward_as_tuple())
                               .first;
        }

        beSourcePartIt->second.push_back(
            parser::Edge{edgeId, way.id, 0, sourceWayNodePair.first,
                         get<4>(partFilesIt->second) - 2, wayNodePair.first,
                         get<4>(partFilesIt->second) - 1, cost, 0});
        borderEdgeBufSizesSum += 1;

        if (!way.oneway) {
          totalBorderEdges++;
          edgeId++;
          beSourcePartIt->second.push_back(parser::Edge{
              edgeId, way.id, 0, wayNodePair.first,
              get<4>(partFilesIt->second) - 1, sourceWayNodePair.first,
              get<4>(partFilesIt->second) - 2, cost, 0});
          borderEdgeBufSizesSum += 1;
        }

        if (beSourcePartIt->second.size() > maxBorderEdgeBufSize) {
          maxBorderEdgeBufSize = beSourcePartIt->second.size();
          maxBorderEdgeIpix = sourceIpix;
        }

        auto beTargetPartIt = borderEdgePartitions.find(ipix);
        if (beTargetPartIt == borderEdgePartitions.end()) {
          beTargetPartIt =
              borderEdgePartitions
                  .emplace(std::piecewise_construct,
                           std::forward_as_tuple(ipix), std::forward_as_tuple())
                  .first;
        }

        beTargetPartIt->second.push_back(parser::Edge{
            edgeId - 1, way.id, 0, sourceWayNodePair.first,
            get<4>(partFilesTargetIt->second) - 2, wayNodePair.first,
            get<4>(partFilesTargetIt->second) - 1, cost, 0});
        borderEdgeBufSizesSum += 1;
        if (!way.oneway) {
          beTargetPartIt->second.push_back(parser::Edge{
              edgeId, way.id, 0, wayNodePair.first,
              get<4>(partFilesTargetIt->second) - 1, sourceWayNodePair.first,
              get<4>(partFilesTargetIt->second) - 2, cost, 0});
          borderEdgeBufSizesSum += 1;
        }

        if (beTargetPartIt->second.size() > maxBorderEdgeBufSize) {
          maxBorderEdgeBufSize = beTargetPartIt->second.size();
          maxBorderEdgeIpix = ipix;
        }

        if (borderEdgeBufSizesSum >= MAX_BORDER_EDGES_SIZES_SUM) {
          const auto fileIt = pixelBorderFiles.find(maxBorderEdgeIpix);
          const auto maxBufIt = borderEdgePartitions.find(maxBorderEdgeIpix);

          if (fileIt == pixelBorderFiles.end() ||
              maxBufIt == borderEdgePartitions.end()) {
            std::cerr << "No pixels found" << std::endl;
            return 1;
          }

          get<3>(fileIt->second) +=
              sizeof(parser::Edge) * maxBufIt->second.size();

          if (ftruncate(get<2>(fileIt->second), get<3>(fileIt->second)) == -1) {
            std::cerr << "failed to alloc memory for edges";
            return 1;
          }
          void* map =
              mmap(nullptr, get<3>(fileIt->second), PROT_READ | PROT_WRITE,
                   MAP_SHARED, get<2>(fileIt->second), 0);

          if (map == MAP_FAILED) {
            std::cerr << "Failed mapping geo-parts file" << std::endl;
            return 1;
          }
          std::memcpy(static_cast<char*>(map) + get<3>(fileIt->second) -
                          sizeof(parser::Edge) * maxBufIt->second.size(),
                      maxBufIt->second.data(),
                      sizeof(parser::Edge) * maxBufIt->second.size());
          if (munmap(map, get<3>(fileIt->second)) == -1) {
            throw std::runtime_error("Failed to unmap ways file");
          }
          borderEdgeBufSizesSum -= maxBufIt->second.size();
          maxBufIt->second.clear();

          maxBorderEdgeBufSize = 0;
          maxBorderEdgeIpix = -1;

          for (const auto& [ipix, buf] : borderEdgePartitions) {
            if (buf.size() >= maxBorderEdgeBufSize) {
              maxBorderEdgeBufSize = buf.size();
              maxBorderEdgeIpix = ipix;
            }
          }
        }
      }

      sourceTuple = wayNodeTuple;
      sourceWayNodePair = wayNodePair;
      sourceIpix = ipix;
      cost = 0;
      edgeId++;
    }
    if (way.id == 18752656) {
      std::cerr << "Successful" << std::endl;
    }
  }

  std::cerr << "Partitioned ways" << std::endl;

  for (const auto& t : nodeTupleFiles) {
    if (munmap(get<2>(t),
               (get<3>(t) *
                sizeof(std::tuple<uint64_t, double, double, uint64_t>)) %
                   TUPLES_CHUNK_SIZE) == -1) {
      std::cerr << "failed unmapping" << std::endl;
      return 1;
    }
    close(get<0>(t));
  }

  if (munmap(ways_map, ways_size) == -1) {
    std::cerr << "Failed unmapping ways file" << std::endl;
    return 1;
  }

  if (munmap(way_nodes_map, way_node_size) == -1) {
    std::cerr << "Failed unmapping way-nodes file" << std::endl;
    return 1;
  }

  for (auto& [ipix, buf] : edgePartitions) {
    auto fileIt = pixelWayFiles.find(ipix);

    if (fileIt == pixelWayFiles.end()) {
      std::cerr << "No pixels found" << std::endl;
      return 1;
    }

    get<1>(fileIt->second) += sizeof(parser::Edge) * buf.size();

    if (ftruncate(get<0>(fileIt->second), get<1>(fileIt->second)) == -1) {
      std::cerr << "failed to alloc memory for edges (2)";
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

  for (auto& [ipix, buf] : borderNodesPartitions) {
    auto fileIt = pixelBorderFiles.find(ipix);

    if (fileIt == pixelBorderFiles.end()) {
      std::cerr << "No pixels found" << std::endl;
      return 1;
    }

    get<1>(fileIt->second) += sizeof(parser::Node) * buf.size();

    if (ftruncate(get<0>(fileIt->second), get<1>(fileIt->second)) == -1) {
      std::cerr << "failed to alloc memory for border nodes (2)";
      return 1;
    }
    void* map = mmap(nullptr, get<1>(fileIt->second), PROT_READ | PROT_WRITE,
                     MAP_SHARED, get<0>(fileIt->second), 0);

    if (map == MAP_FAILED) {
      std::cerr << "Failed mapping geo-parts file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + get<1>(fileIt->second) -
                    sizeof(parser::Node) * buf.size(),
                buf.data(), sizeof(parser::Node) * buf.size());
    if (munmap(map, get<1>(fileIt->second)) == -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    close(get<0>(fileIt->second));
    buf.clear();
  }

  for (auto& [ipix, buf] : borderEdgePartitions) {
    auto fileIt = pixelBorderFiles.find(ipix);

    if (fileIt == pixelBorderFiles.end()) {
      std::cerr << "No pixels found" << std::endl;
      return 1;
    }

    get<3>(fileIt->second) += sizeof(parser::Edge) * buf.size();

    if (ftruncate(get<2>(fileIt->second), get<3>(fileIt->second)) == -1) {
      std::cerr << "failed to alloc memory for border edge (2)";
      return 1;
    }
    void* map = mmap(nullptr, get<3>(fileIt->second), PROT_READ | PROT_WRITE,
                     MAP_SHARED, get<2>(fileIt->second), 0);

    if (map == MAP_FAILED) {
      std::cerr << "Failed mapping geo-parts file" << std::endl;
      return 1;
    }
    std::memcpy(static_cast<char*>(map) + get<3>(fileIt->second) -
                    sizeof(parser::Edge) * buf.size(),
                buf.data(), sizeof(parser::Edge) * buf.size());
    if (munmap(map, get<3>(fileIt->second)) == -1) {
      throw std::runtime_error("Failed to unmap ways file");
    }
    close(get<2>(fileIt->second));
    buf.clear();
  }

  edgePartitions.clear();
  borderNodesPartitions.clear();
  borderEdgePartitions.clear();
  pixelWayFiles.clear();
  pixelBorderFiles.clear();

  auto startHashingRests = std::chrono::high_resolution_clock::now();

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

  uint64_t restsCount = rests_size / sizeof(DiskRestriction);

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

  std::cerr << "Time for reading and hashing restrictions: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() -
                   startHashingRests)
                   .count()
            << " ms" << std::endl;
  std::cerr << "Total time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms" << std::endl;

  auto restsCheckStart = std::chrono::high_resolution_clock::now();

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
    uint64_t size = sb.st_size;

    if (size == 0) {
      continue;
    }

    void* map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fdIt->second, 0);

    if (map == MAP_FAILED) {
      close(fdIt->second);
      throw std::runtime_error("Failed mapping: opening edges");
    }
    close(fdIt->second);

    uint64_t wayCount = size / sizeof(parser::Edge);
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

  for (auto ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/border-edges/edges-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
      close(fd);
      throw std::runtime_error("Failed to obtain file size");
    }
    uint64_t size = sb.st_size;

    if (size == 0) {
      close(fd);
      continue;
    }

    void* map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);

    if (map == MAP_FAILED) {
      close(fd);
      throw std::runtime_error("Failed mapping: border edges");
    }
    close(fd);

    uint64_t edgeCount = size / sizeof(parser::Edge);
    for (uint64_t i = 0; i < edgeCount; i++) {
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

  onlyRestrictionsByTo.clear();

  std::cerr << "Time for checking valid restrictions: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - restsCheckStart)
                   .count()
            << " ms" << std::endl;
  std::cerr << "Total time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms" << std::endl;

  std::cerr << "Total partitions: " << usedPixels.size() << std::endl;

  auto processPartStart = std::chrono::high_resolution_clock::now();

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
    uint64_t size_n = sb_n.st_size;

    void* map_n =
        mmap(nullptr, size_n, PROT_READ, MAP_PRIVATE, get<0>(fdsIt->second), 0);

    if (map_n == MAP_FAILED) {
      close(get<0>(fdsIt->second));
      throw std::runtime_error("Failed mapping: nodes in geo");
    }
    close(get<0>(fdsIt->second));

    // open file with ways in this partition
    struct stat sb_e;
    if (fstat(get<1>(fdsIt->second), &sb_e) == -1) {
      close(get<1>(fdsIt->second));
      throw std::runtime_error("Failed to obtain file size");
    }
    uint64_t size_e = sb_e.st_size;

    if (size_e == 0) {
      continue;
    }

    void* map_e =
        mmap(nullptr, size_e, PROT_READ, MAP_PRIVATE, get<1>(fdsIt->second), 0);

    if (map_e == MAP_FAILED) {
      close(get<1>(fdsIt->second));
      throw std::runtime_error("Failed mapping: edges in geo");
    }
    close(get<1>(fdsIt->second));

    uint64_t edgeCount = size_e / sizeof(parser::Edge);

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

    std::ostringstream stream_be;
    stream_be << "./bin/geo-partitions/border-edges/edges-"
              << std::to_string(ipix) << ".bin";
    std::string filename_be = stream_be.str();
    int fd_be = open(filename_be.data(), O_RDONLY);
    if (fd_be == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }

    struct stat sb_be;
    if (fstat(fd_be, &sb_be) == -1) {
      close(fd_be);
      throw std::runtime_error("Failed to obtain file size");
    }
    uint64_t size_be = sb_be.st_size;

    if (size_be != 0) {
      void* map_be = mmap(nullptr, size_be, PROT_READ, MAP_PRIVATE, fd_be, 0);

      if (map_be == MAP_FAILED) {
        close(fd_be);
        throw std::runtime_error("Failed mapping: border edges");
      }
      close(fd_be);

      parser::Edge* borderEdges =
          reinterpret_cast<parser::Edge*>(static_cast<char*>(map_be));

      uint64_t borderEdgeCount = size_be / sizeof(parser::Edge);

      std::ostringstream stream_bn;
      stream_bn << "./bin/geo-partitions/border-nodes/nodes-"
                << std::to_string(ipix) << ".bin";
      std::string filename_bn = stream_bn.str();
      int fd_bn = open(filename_bn.data(), O_RDONLY);
      if (fd_bn == -1) {
        std::cerr << "Failed to open file.\n";
        return 1;
      }

      struct stat sb_bn;
      if (fstat(fd_bn, &sb_bn) == -1) {
        close(fd_bn);
        throw std::runtime_error("Failed to obtain file size");
      }
      uint64_t size_bn = sb_bn.st_size;

      void* map_bn = mmap(nullptr, size_bn, PROT_READ, MAP_PRIVATE, fd_bn, 0);

      if (map_bn == MAP_FAILED) {
        close(fd_bn);
        throw std::runtime_error("Failed mapping: border nodes");
      }
      close(fd_bn);

      parser::Node* borderNodes =
          reinterpret_cast<parser::Node*>(static_cast<char*>(map_bn));

      std::unordered_map<google::protobuf::int64, std::vector<parser::Edge*>>
          borderGraph;

      for (uint64_t i = 0; i < borderEdgeCount; i++) {
        auto edge = borderEdges + i;
        auto entryIt = borderGraph.find(edge->sourceNodeId);
        if (entryIt == borderGraph.end()) {
          auto vecIt = borderGraph
                           .emplace(std::piecewise_construct,
                                    std::forward_as_tuple(edge->sourceNodeId),
                                    std::forward_as_tuple())
                           .first;
          vecIt->second.push_back(edge);
        } else {
          entryIt->second.push_back(edge);
        }
      }

      for (uint64_t i = 0; i < borderEdgeCount; i++) {
        parser::graph::invert::applyRestrictionsSourceBorder(
            borderEdges + i, graph.graph(),
            reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
            borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            expEdgesBuf);
      }

      for (uint64_t i = 0; i < edgeCount; i++) {
        auto edgePtr = reinterpret_cast<parser::Edge*>(
            static_cast<char*>(map_e) + i * sizeof(parser::Edge));
        parser::graph::invert::applyRestrictionsSourcePartition(
            edgePtr, borderGraph,
            reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
            borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            expEdgesBuf);
      }

      if (munmap(map_be, size_be) == -1) {
        std::cerr << "Failed unmapping border edges" << std::endl;
        return 1;
      }

      if (munmap(map_bn, size_bn) == -1) {
        std::cerr << "Failed unmapping border nodes" << std::endl;
        return 1;
      }
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

  std::cerr << "Time for processing partitions: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - processPartStart)
                   .count()
            << " ms" << std::endl;
  std::cerr << "Total time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms" << std::endl;

  auto bordersStart = std::chrono::high_resolution_clock::now();

  std::unordered_map<uint32_t, std::pair<int, int>> borderFileFdPartitions;

  for (auto ipix : usedPixels) {
    std::ostringstream stream_n;
    stream_n << "./bin/geo-partitions/border-nodes/nodes-"
             << std::to_string(ipix) << ".bin";
    std::string filename_n = stream_n.str();
    int fd_n = open(filename_n.data(), O_RDONLY);
    if (fd_n == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }

    std::ostringstream stream_e;
    stream_e << "./bin/geo-partitions/border-edges/edges-"
             << std::to_string(ipix) << ".bin";
    std::string filename_e = stream_e.str();
    int fd_e = open(filename_e.data(), O_RDONLY);
    if (fd_e == -1) {
      std::cerr << "Failed to open file.\n";
      return 1;
    }

    borderFileFdPartitions.emplace(std::piecewise_construct,
                                   std::forward_as_tuple(ipix),
                                   std::forward_as_tuple(fd_n, fd_e));
  }

  std::vector<parser::ExpandedEdge> borderExpEdges;

  for (auto ipix : usedPixels) {
    const auto filesIt = borderFileFdPartitions.find(ipix);

    if (filesIt == borderFileFdPartitions.end()) {
      std::cerr << "Failed finding files for border" << std::endl;
      return 1;
    }

    struct stat sb_e;
    if (fstat(filesIt->second.second, &sb_e) == -1) {
      close(filesIt->second.second);
      throw std::runtime_error("Failed to obtain file size");
    }
    uint64_t size_e = sb_e.st_size;

    if (size_e == 0) {
      continue;
    }

    void* map_e = mmap(nullptr, size_e, PROT_READ, MAP_PRIVATE,
                       filesIt->second.second, 0);

    if (map_e == MAP_FAILED) {
      close(filesIt->second.second);
      throw std::runtime_error("Failed mapping: border edges (2)");
    }

    struct stat sb_n;
    if (fstat(filesIt->second.first, &sb_n) == -1) {
      close(filesIt->second.first);
      throw std::runtime_error("Failed to obtain file size");
    }
    uint64_t size_n = sb_n.st_size;

    void* map_n =
        mmap(nullptr, size_n, PROT_READ, MAP_PRIVATE, filesIt->second.first, 0);

    if (map_n == MAP_FAILED) {
      close(filesIt->second.first);
      throw std::runtime_error("Failed mapping: border nodes (2)");
    }

    parser::Node* nodes =
        reinterpret_cast<parser::Node*>(static_cast<char*>(map_n));

    auto edges = reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e));

    uint64_t edgeCount = size_e / sizeof(parser::Edge);

    std::unordered_map<uint32_t, std::vector<parser::Edge*>>
        edgeHashByTargetIpix;

    for (uint64_t i = 0; i < edgeCount; i++) {
      auto edge = reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e) +
                                                  i * sizeof(parser::Edge));
      auto sourceNode = nodes + edge->sourceNodeOffset;

      auto sourceTheta = (90 - sourceNode->lat) * M_PI / 180;
      auto sourcePhi = sourceNode->lon * M_PI / 180;

      long sourceIpix;

      ang2pix_ring(50, sourceTheta, sourcePhi, &sourceIpix);

      if (sourceIpix != ipix) {
        continue;
      }

      auto targetNode = nodes + edge->targetNodeOffset;

      auto targetTheta = (90 - targetNode->lat) * M_PI / 180;
      auto targetPhi = targetNode->lon * M_PI / 180;

      long targetIpix;

      ang2pix_ring(50, targetTheta, targetPhi, &targetIpix);

      auto partIt = edgeHashByTargetIpix.find(targetIpix);

      if (partIt == edgeHashByTargetIpix.end()) {
        partIt = edgeHashByTargetIpix
                     .emplace(std::piecewise_construct,
                              std::forward_as_tuple(targetIpix),
                              std::forward_as_tuple())
                     .first;
      }

      partIt->second.push_back(edge);
    }

    for (const auto& [targetIpix, buf] : edgeHashByTargetIpix) {
      const auto targetFilesIt = borderFileFdPartitions.find(targetIpix);
      if (targetFilesIt == borderFileFdPartitions.end()) {
        std::cerr << "Files for pixel not found" << std::endl;
        return 1;
      }
      struct stat sb_n;
      if (fstat(targetFilesIt->second.first, &sb_n) == -1) {
        close(targetFilesIt->second.first);
        throw std::runtime_error("Failed to obtain file size");
      }
      uint64_t size_n = sb_n.st_size;

      void* map_n = mmap(nullptr, size_n, PROT_READ, MAP_PRIVATE,
                         targetFilesIt->second.first, 0);

      if (map_n == MAP_FAILED) {
        close(targetFilesIt->second.first);
        throw std::runtime_error("Failed mapping");
      }

      parser::Node* targetNodes =
          reinterpret_cast<parser::Node*>(static_cast<char*>(map_n));

      struct stat sb_e;
      if (fstat(targetFilesIt->second.second, &sb_e) == -1) {
        close(targetFilesIt->second.second);
        throw std::runtime_error("Failed to obtain file size");
      }
      uint64_t size_e = sb_e.st_size;

      void* map_e = mmap(nullptr, size_e, PROT_READ, MAP_PRIVATE,
                         targetFilesIt->second.second, 0);

      if (map_e == MAP_FAILED) {
        close(targetFilesIt->second.second);
        throw std::runtime_error("Failed mapping");
      }

      parser::Edge* targetEdges =
          reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e));

      uint64_t targetEdgeCount = size_e / sizeof(parser::Edge);

      std::unordered_map<google::protobuf::int64, std::vector<parser::Edge*>>
          borderGraph;

      for (uint64_t i = 0; i < targetEdgeCount; i++) {
        auto edge = targetEdges + i;
        auto entryIt = borderGraph.find(edge->sourceNodeId);
        if (entryIt == borderGraph.end()) {
          auto vecIt = borderGraph
                           .emplace(std::piecewise_construct,
                                    std::forward_as_tuple(edge->sourceNodeId),
                                    std::forward_as_tuple())
                           .first;
          vecIt->second.push_back(edge);
        } else {
          entryIt->second.push_back(edge);
        }
      }

      for (auto edge : buf) {
        parser::graph::invert::applyRestrictionsSourcePartition(
            edge, borderGraph, nodes, targetNodes, onlyRestrictionsMap,
            noRestrictionsHash, expEdgeId, borderExpEdges);
      }

      if (munmap(map_n, size_n) == -1) {
        return 1;
      }
      if (munmap(map_e, size_e) == -1) {
        return 1;
      }
    }
    if (munmap(map_n, size_n) == -1) {
      return 1;
    }
    if (munmap(map_e, size_e) == -1) {
      return 1;
    }
  }

  std::cerr << "Time for processing borders: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - bordersStart)
                   .count()
            << " ms" << std::endl;
  std::cerr << "Total time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << " ms" << std::endl;

  std::cerr << "Total expanded edges: " << sum + borderExpEdges.size()
            << std::endl;

  for (uint32_t ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/exp-edges/exp-edges-"
           << std::to_string(ipix) << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      continue;
    }

    unlink(filename.data());
    close(fd);
  }

  for (uint32_t ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/border-edges/edges-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      continue;
    }

    unlink(filename.data());
    close(fd);
  }

  for (uint32_t ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/border-nodes/nodes-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      continue;
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