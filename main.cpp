#include <arpa/inet.h>
#include <chealpix.h>
#include <fcntl.h>
#include <osmpbf/fileformat.pb.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_for_each.h>
#include <unistd.h>

#include <cmath>
#include <disk/file-read.hpp>
#include <disk/file-write.hpp>
#include <disk/key-file-cluster-write.hpp>
#include <disk/utils.hpp>
#include <format>
#include <fstream>
#include <graph/graph.hpp>
#include <graph/invert-source-border.hpp>
#include <graph/invert-source-partition.hpp>
#include <graph/invert.hpp>
#include <iostream>
#include <parsing/primitive-block-parser.hpp>
#include <processing.hpp>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <tables/ska/flat_hash_map.hpp>
#include <types/edge.hpp>
#include <types/expanded-edge.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>
#include <utils/geomath.hpp>
#include <utils/hashing.hpp>
#include <utils/libdeflate_decomp.hpp>

const uint64_t HASH_PARTITIONS_NUM = 64;
const uint16_t N_SIDE = 50;
const uint64_t HASH_MASK = 0x3F;
const uint64_t MAX_NODE_BUF_SIZE = 1'000'000;
const uint64_t MAX_WAY_NODES_BUF_SIZE = 57'600'000;
const uint64_t MAX_USED_NODES_BUF_SIZE = 57'600'000;
const uint64_t MAX_WAY_BUF_SIZE = 4'800'000;
// --------------
const uint64_t MAX_REDUCED_NODES_BUF_SIZE = 600'000;
const uint64_t MAX_NODE_BUF_SIZES_SUM = 48'000'000;
// --------------
const uint64_t MAX_WAY_BUF_SIZES_SUM = 1'638'400;
const uint64_t MAX_BORDER_NODES_SIZES_SUM = 2'000'000;
const uint64_t MAX_BORDER_EDGES_SIZES_SUM = 330'000;
const uint64_t MAX_HASH_TUPLES_BUF_SIZE = 1'000'000;
const uint64_t MAX_RESTRICTIONS_BUF_SIZE = 235'930;
// const uint64_t MAX_WAY_NODE_BUF_SIZES_SUM = 115'200'000;

std::queue<std::tuple<const char*, int32_t>> tuple_queue;
std::mutex queue_mutex;
std::condition_variable cv;
std::atomic<bool> done(false);

uint64_t wayNodesOffset = 0;
std::vector<uint64_t> hashPartOffsets(HASH_PARTITIONS_NUM);

std::mutex waysBlockMutex;
std::vector<std::mutex> nodesBlockMutexes(HASH_PARTITIONS_NUM);
std::mutex restrictionsBlockMutex;

struct ExtendedNode {
  google::protobuf::int64 id;
  double lat;
  double lon;
  uint64_t used;
  uint64_t offset;
};

struct NodeData {
  uint64_t offset;
  double lat;
  double lon;
  uint64_t used;
};

std::mutex filestat_mutex;

struct Filestat {
  uint64_t total_decomp_size;
  uint64_t total_nodes;
  uint64_t used_nodes;
  uint64_t total_ways;
  uint64_t used_ways;
  uint64_t total_nodes_in_ways;
  uint64_t used_nodes_in_ways;
  uint64_t total_relations;
  uint64_t used_relations;

  int64_t total_parse_nodes_time;
  int64_t total_parse_ways_time;
};

Filestat filestat;

void worker(
    std::vector<FileWrite<parser::Node>>& nodeFiles,
    FileWrite<parser::Way>& waysFile,
    FileWrite<std::pair<google::protobuf::int64, uint64_t>>& wayNodesFile,
    std::vector<FileWrite<google::protobuf::int64>>& usedNodesFiles,
    FileWrite<parser::Restriction>& restrictionFile,
    ska::flat_hash_set<long>& pixels) {
  while (true) {
    std::tuple<const char*, int32_t> tuple;

    {
      std::unique_lock<std::mutex> lock(queue_mutex);

      cv.wait(lock, [] { return !tuple_queue.empty() || done; });

      if (tuple_queue.empty() && done) {
        return;
      }

      tuple = std::move(tuple_queue.front());
      tuple_queue.pop();
    }

    const char* pos = get<0>(tuple);
    uint64_t compSize = get<1>(tuple);

    OSMPBF::Blob blob;
    if (!blob.ParseFromArray(pos, compSize)) {
      throw std::runtime_error("Failed parsing blob");
    }

    unsigned char* uncompressedData = new unsigned char[blob.raw_size()];

    if (blob.has_raw()) {
      const std::string rawData = blob.raw();
      std::copy(rawData.begin(), rawData.end(), uncompressedData);
    } else if (blob.has_zlib_data()) {
      ldeflate_decompress(blob.zlib_data(), uncompressedData, blob.raw_size());
    } else {
      throw std::runtime_error("Unsupported compression format");
    }

    OSMPBF::PrimitiveBlock primitiveBlock;
    if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
      throw std::runtime_error("");
    }

    const auto& stringtable = primitiveBlock.stringtable();
    int64_t waysParseTime = 0;
    int64_t nodesParseTime = 0;
    uint64_t totalWaysNum = 0;
    uint64_t totalWayNodes = 0;
    uint64_t usedWaysNum = 0;
    uint64_t usedWayNodes = 0;
    uint64_t nodesNum = 0;
    uint64_t totalRelations = 0;
    uint64_t usedRestrictions = 0;

    for (const auto& group : primitiveBlock.primitivegroup()) {
      auto beforeWays = std::chrono::high_resolution_clock::now();
      for (const auto& way : group.ways()) {
        totalWaysNum++;
        totalWayNodes += way.refs_size();
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

        {
          std::unique_lock<std::mutex> lock(waysBlockMutex);

          waysFile.add(
              parser::Way{way.id(), wayNodesOffset, way.refs_size(), oneway});

          wayNodesOffset += way.refs_size();

          google::protobuf::int64 id = 0;

          for (uint64_t index = 0; index < way.refs_size(); index++) {
            id += way.refs(index);

            uint64_t hash = MurmurHash64A_1(id) & HASH_MASK;

            wayNodesFile.add(std::make_pair(id, hashPartOffsets[hash]));
            hashPartOffsets[hash]++;

            usedNodesFiles[hash].add(
                index == 0 || index == way.refs_size() - 1 ? -id : id);
          }
        }
      }

      waysParseTime +=
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::high_resolution_clock::now() - beforeWays)
              .count();

      auto beforeNodes = std::chrono::high_resolution_clock::now();

      const auto& denseNodes = group.dense();

      if (denseNodes.id_size() != denseNodes.lon_size() ||
          denseNodes.id_size() != denseNodes.lat_size()) {
        throw std::runtime_error("");
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

        uint64_t hash = MurmurHash64A_1(id) & HASH_MASK;

        {
          std::unique_lock<std::mutex> lock(nodesBlockMutexes[hash]);
          pixels.insert(ipix);
          nodeFiles[hash].add(parser::Node{id, lat, lon});
        }
      }

      nodesParseTime +=
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::high_resolution_clock::now() - beforeNodes)
              .count();

      for (auto& rel : group.relations()) {
        totalRelations++;
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

        int8_t type;
        if (restrictionType == "no_right_turn") {
          type = 0;
        } else if (restrictionType == "no_left_turn") {
          type = 1;
        } else if (restrictionType == "no_straight_on") {
          type = 2;
        } else if (restrictionType == "only_left_turn") {
          type = 3;
        } else if (restrictionType == "only_right_turn") {
          type = 4;
        } else if (restrictionType == "only_straight_on") {
          type = 5;
        } else {
          type = -1;
        }
        {
          std::unique_lock<std::mutex> lock(restrictionsBlockMutex);
          restrictionFile.add(
              parser::Restriction{rel.id(), from, via, to, type});
        }
      }
    }

    {
      std::unique_lock<std::mutex> lock(filestat_mutex);
      filestat.total_decomp_size += blob.raw_size();
      filestat.total_nodes += nodesNum;
      filestat.total_ways += totalWaysNum;
      filestat.total_nodes_in_ways += totalWayNodes;
      filestat.used_ways += usedWaysNum;
      filestat.used_nodes_in_ways += usedWayNodes;
      filestat.total_parse_nodes_time += nodesParseTime;
      filestat.total_parse_ways_time += waysParseTime;
      filestat.total_relations += totalRelations;
      filestat.used_relations += usedRestrictions;
    }

    delete[] uncompressedData;
  }
}

void producer(const char* pbf_data, const char* eof_pbf) {
  while (pbf_data < eof_pbf) {
    uint32_t headerSize;
    std::memcpy(&headerSize, pbf_data, sizeof(headerSize));
    headerSize = ntohl(headerSize);

    pbf_data += sizeof(headerSize);

    OSMPBF::BlobHeader header;
    if (!header.ParseFromArray(pbf_data, headerSize)) {
      throw std::runtime_error("");
    }

    pbf_data += headerSize;

    {
      std::unique_lock<std::mutex> lock(filestat_mutex);
      filestat.total_decomp_size += sizeof(uint32_t) + headerSize;
    }

    if (header.type() != "OSMData") {
      pbf_data += header.datasize();
      continue;
    }

    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      tuple_queue.push(std::make_tuple(pbf_data, header.datasize()));
    }

    pbf_data += header.datasize();
  }

  done = true;
  cv.notify_all();
}

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // benchmarks

  auto start = std::chrono::high_resolution_clock::now();

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

  std::vector<FileWrite<parser::Node>> nodeFiles;

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::string filename =
        numFilename("./bin/node-hash-partitions/nodes/nodes-", i, "bin");
    try {
      int fd =
          open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      nodeFiles.emplace_back(fd, 30'000'000);
    } catch (std::runtime_error& e) {
      std::cerr << e.what() << std::endl;
      return 1;
    }
  }

  int way_fd =
      open("./bin/ways.bin", O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  FileWrite<parser::Way> waysFile(way_fd,
                                  MAX_WAY_BUF_SIZE * sizeof(parser::Way));

  int way_node_fd = open("./bin/way-nodes.bin", O_RDWR | O_CREAT | O_TRUNC,
                         S_IRUSR | S_IWUSR);
  FileWrite<std::pair<google::protobuf::int64, uint64_t>> wayNodesFile(
      way_node_fd, MAX_WAY_NODES_BUF_SIZE *
                       sizeof(std::pair<google::protobuf::int64, uint64_t>));

  // open used-nodes files in node-hash partitions

  std::vector<FileWrite<google::protobuf::int64>> usedNodesFiles;

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::string filename = numFilename(
        "./bin/node-hash-partitions/used-nodes/used-nodes-", i, "bin");
    int fd =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    usedNodesFiles.emplace_back(
        fd, MAX_USED_NODES_BUF_SIZE * sizeof(google::protobuf::int64));
  }

  // open restrictions file for writing

  int fd_rests = open("./bin/restrictions.bin", O_RDWR | O_CREAT | O_TRUNC,
                      S_IRUSR | S_IWUSR);
  FileWrite<parser::Restriction> restrictionFile(
      fd_rests, MAX_RESTRICTIONS_BUF_SIZE * sizeof(parser::Restriction));

  ska::flat_hash_set<long> pixels;

  std::thread producer_thread([&]() { producer(pbf_data, eof_pbf); });

  const uint64_t workers_num = std::thread::hardware_concurrency();
  std::vector<std::thread> workers;

  for (int i = 0; i < workers_num; i++) {
    workers.emplace_back([&]() {
      worker(nodeFiles, waysFile, wayNodesFile, usedNodesFiles, restrictionFile,
             pixels);
    });
  }

  producer_thread.join();

  for (auto& worker_thread : workers) {
    worker_thread.join();
  }

  try {
    pbf.unmap_file();
  } catch (std::runtime_error& error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    try {
      nodeFiles[i].flush();
    } catch (std::runtime_error& e) {
      nodeFiles[i].close_fd();
      std::cerr << e.what() << std::endl;
      return 1;
    }
    nodeFiles[i].close_fd();
  }
  nodeFiles.clear();

  waysFile.flush();
  waysFile.close_fd();

  wayNodesFile.flush();
  wayNodesFile.close_fd();

  hashPartOffsets.clear();

  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    usedNodesFiles[i].flush();
    usedNodesFiles[i].close_fd();
  }
  usedNodesFiles.clear();

  restrictionFile.flush();
  restrictionFile.close_fd();

  std::cerr << "Time for parsing ways: " << filestat.total_parse_ways_time
            << " ms" << std::endl;
  std::cerr << "Time for parsing nodes: " << filestat.total_parse_nodes_time
            << " ms" << std::endl;

  auto parsingDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now() - start);
  std::cerr << "Time for .osm.pbf parsing: " << parsingDuration.count() << "ms"
            << std::endl;

  std::cerr << "-------" << std::endl;

  std::cerr << "Total data size after decompression: "
            << filestat.total_decomp_size << std::endl;
  std::cerr << "Total nodes: " << filestat.total_nodes << std::endl;
  std::cerr << "Total ways: " << filestat.total_ways
            << " (used: " << filestat.used_ways << ")" << std::endl;
  std::cerr << "Total nodes in ways: " << filestat.total_nodes_in_ways
            << " (used: " << filestat.used_nodes_in_ways << ")" << std::endl;
  std::cerr << "Average way size: "
            << static_cast<float>(filestat.total_nodes_in_ways) /
                   filestat.total_ways
            << std::endl;
  std::cerr << "Average used way size: "
            << static_cast<float>(filestat.used_nodes_in_ways) /
                   filestat.used_ways
            << std::endl;
  std::cerr << "Total relations: " << filestat.total_relations
            << " (used: " << filestat.used_relations << ")" << std::endl;

  auto partNodesStart = std::chrono::high_resolution_clock::now();

  std::vector<std::pair<long, int>> pixelNodeFiles;

  for (long ipix : pixels) {
    std::string filename =
        numFilename("./bin/geo-partitions/nodes/nodes-", ipix, "bin");
    int fd =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    pixelNodeFiles.emplace_back(ipix, fd);
  }

  KeyFileClusterWrite<long, parser::Node> nodePartsCluster(
      pixelNodeFiles, MAX_NODE_BUF_SIZES_SUM * sizeof(parser::Node));

  tbb::concurrent_unordered_set<long> usedPixels;

  std::unordered_map<long, uint64_t> nodeOffsets;

  std::mutex nodesClusterMutex;

  for (uint16_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    auto hashPartitionStart = std::chrono::high_resolution_clock::now();

    std::string rn_fn = numFilename(
        "./bin/node-hash-partitions/reduced-nodes/nodes-", i, "bin");
    int rn_fd =
        open(rn_fn.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    FileWrite<ExtendedNode> rnFile(
        rn_fd, MAX_REDUCED_NODES_BUF_SIZE * sizeof(ExtendedNode));

    std::string un_fn = numFilename(
        "./bin/node-hash-partitions/used-nodes/used-nodes-", i, "bin");
    int un_fd = open(un_fn.data(), O_RDONLY);
    FileRead unFileRead(un_fd);

    void* un_map = unFileRead.mmap_file();

    uint64_t usedNodesCount =
        unFileRead.fsize() / sizeof(google::protobuf::int64);
    unFileRead.close_fd();

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

    unFileRead.unmap_file();

    std::string filename =
        numFilename("./bin/node-hash-partitions/nodes/nodes-", i, "bin");
    int fd_part = open(filename.data(), O_RDONLY);
    FileRead nodesFile(fd_part);
    unlink(filename.data());

    void* n_map = nodesFile.mmap_file();

    uint64_t nodesCount = nodesFile.fsize() / sizeof(parser::Node);
    nodesFile.close_fd();

    for (uint64_t j = 0; j < nodesCount; j++) {
      parser::Node* n = reinterpret_cast<parser::Node*>(
          static_cast<char*>(n_map) + j * sizeof(parser::Node));
      const auto usedNodePairIt = usedNodes.find(n->id);
      if (usedNodePairIt == usedNodes.end()) {
        continue;
      }
      auto theta = (90 - n->lat) * M_PI / 180;
      auto phi = n->lon * M_PI / 180;

      long ipix;

      ang2pix_ring(50, theta, phi, &ipix);

      usedPixels.insert(ipix);

      std::unordered_map<long, uint64_t>::iterator offsetIt;

      nodePartsCluster.add(ipix, parser::Node{n->id, n->lat, n->lon});

      offsetIt = nodeOffsets.find(ipix);
      if (offsetIt == nodeOffsets.end()) {
        offsetIt = nodeOffsets.emplace(ipix, 0).first;
      } else {
        offsetIt->second++;
      }

      rnFile.add(ExtendedNode{n->id, n->lat, n->lon, usedNodePairIt->second,
                              offsetIt->second});
    }
    nodesFile.unmap_file();
    rnFile.flush();
    rnFile.close_fd();

    usedNodes.clear();

    rn_fd = open(rn_fn.data(), O_RDONLY);
    FileRead rnFileRead(rn_fd);
    unlink(rn_fn.data());

    void* map_rn = rnFileRead.mmap_file();

    uint64_t reducedNodesCount = rnFileRead.fsize() / sizeof(ExtendedNode);
    rnFileRead.close_fd();

    ska::flat_hash_map<google::protobuf::int64, ExtendedNode*> nodesHash;

    for (uint64_t j = 0; j < reducedNodesCount; j++) {
      ExtendedNode* n = reinterpret_cast<ExtendedNode*>(
          static_cast<char*>(map_rn) + j * sizeof(ExtendedNode));

      nodesHash.emplace(n->id, n);
    }

    std::string tuples_filename = numFilename(
        "./bin/node-hash-partitions/node-data/node-data-", i, "bin");
    int tuples_fd = open(tuples_filename.data(), O_RDWR | O_CREAT | O_TRUNC,
                         S_IRUSR | S_IWUSR);

    FileWrite<NodeData> tupleFileWrite(tuples_fd, MAX_HASH_TUPLES_BUF_SIZE);

    un_fd = open(un_fn.data(), O_RDONLY);
    FileRead unFileRead2(un_fd);
    unlink(un_fn.data());
    un_map = unFileRead2.mmap_file();

    usedNodesCount = unFileRead2.fsize() / sizeof(google::protobuf::int64);

    unFileRead2.close_fd();

    for (uint64_t j = 0; j < usedNodesCount; j++) {
      google::protobuf::int64* n = reinterpret_cast<google::protobuf::int64*>(
          static_cast<char*>(un_map) + j * sizeof(google::protobuf::int64));

      auto id = *n > 0 ? *n : -(*n);

      const auto nodeIt = nodesHash.find(id);

      if (nodeIt == nodesHash.end()) {
        throw std::runtime_error("no node found for used node");
      }

      tupleFileWrite.add(NodeData{nodeIt->second->offset, nodeIt->second->lat,
                                  nodeIt->second->lon, nodeIt->second->used});
    }
    nodesHash.clear();
    tupleFileWrite.flush();
    tupleFileWrite.close_fd();

    unFileRead2.unmap_file();
    rnFileRead.unmap_file();
  }

  nodePartsCluster.flush();
  nodePartsCluster.close_fds();

  nodeOffsets.clear();
  pixelNodeFiles.clear();

  std::cerr << "Partitioned nodes in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - partNodesStart)
                   .count()
            << std::endl;
  std::cerr << "Total time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
                   .count()
            << std::endl;

  int fd_ways_read = open("./bin/ways.bin", O_RDONLY);
  FileRead waysFileRead(fd_ways_read);
  unlink("./bin/ways.bin");

  void* ways_map = waysFileRead.mmap_file();

  uint64_t totalWaysCount = waysFileRead.fsize() / sizeof(parser::Way);
  waysFileRead.close_fd();

  int fd_way_nodes_read = open("./bin/way-nodes.bin", O_RDONLY);
  FileRead wayNodesFileRead(fd_way_nodes_read);
  unlink("./bin/way-nodes.bin");

  void* way_nodes_map = wayNodesFileRead.mmap_file();
  wayNodesFileRead.close_fd();

  std::vector<std::tuple<int, uint64_t, void*>> nodeTupleFiles;
  nodeTupleFiles.resize(HASH_PARTITIONS_NUM);

  const uint64_t TUPLES_CHUNK_SIZE = 327'680;
  const uint64_t TUPLES_IN_CHUNK = TUPLES_CHUNK_SIZE / sizeof(NodeData);
  for (uint64_t i = 0; i < HASH_PARTITIONS_NUM; i++) {
    std::string filename = numFilename(
        "./bin/node-hash-partitions/node-data/node-data-", i, "bin");
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
      map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    } else {
      map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    }
    if (map == MAP_FAILED) {
      close(fd);
      std::cerr << "Failed mapping initial chunk" << std::endl;
      return 1;
    }
    unlink(filename.data());
    nodeTupleFiles[i] = std::make_tuple(fd, size, map);
  }

  std::vector<std::pair<long, int>> pixelWayFiles;
  std::vector<std::pair<long, int>> pixelBorderNodesFiles;
  std::vector<std::pair<long, int>> pixelBorderEdgesFiles;
  std::vector<std::pair<long, int>> pixelEdgeGeometryFiles;
  std::vector<std::pair<long, int>> pixelBorderEdgeGeomFiles;

  ska::flat_hash_map<long, uint64_t> borderOffsets;
  ska::flat_hash_map<long, uint64_t> geomOffsets;
  ska::flat_hash_map<long, uint64_t> borderGeomOffsets;

  for (long ipix : usedPixels) {
    std::string filename_e =
        numFilename("./bin/geo-partitions/edges/edges-", ipix, "bin");
    int fd_e =
        open(filename_e.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

    pixelWayFiles.emplace_back(ipix, fd_e);

    std::string filename_bn =
        numFilename("./bin/geo-partitions/border-nodes/nodes-", ipix, "bin");
    int fd_bn =
        open(filename_bn.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    pixelBorderNodesFiles.emplace_back(ipix, fd_bn);

    std::string filename_be =
        numFilename("./bin/geo-partitions/border-edges/edges-", ipix, "bin");
    int fd_be =
        open(filename_be.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    pixelBorderEdgesFiles.emplace_back(ipix, fd_be);
    borderOffsets.emplace(ipix, 0);

    std::string filename_eg =
        numFilename("./bin/geo-partitions/edge-geometry/geo-", ipix, "bin");
    int fd_eg =
        open(filename_eg.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    pixelEdgeGeometryFiles.emplace_back(ipix, fd_eg);

    std::string filename_beg = numFilename(
        "./bin/geo-partitions/border-edge-geometry/geo-", ipix, "bin");
    int fd_beg = open(filename_beg.data(), O_RDWR | O_CREAT | O_TRUNC,
                      S_IRUSR | S_IWUSR);
    pixelBorderEdgeGeomFiles.emplace_back(ipix, fd_beg);

    geomOffsets.emplace(ipix, 0);
    borderGeomOffsets.emplace(ipix, 0);
  }

  KeyFileClusterWrite<long, parser::Edge> edgeFileClusterWrite(
      pixelWayFiles, MAX_WAY_BUF_SIZES_SUM);
  KeyFileClusterWrite<long, parser::Edge> borderEdgeFileClusterWrite(
      pixelBorderEdgesFiles, MAX_BORDER_EDGES_SIZES_SUM);
  KeyFileClusterWrite<long, parser::Node> borderNodesFileClusterWrite(
      pixelBorderNodesFiles, MAX_BORDER_NODES_SIZES_SUM);
  KeyFileClusterWrite<long, std::pair<double, double>> edgeGeometryClusterWrite(
      pixelEdgeGeometryFiles, MAX_WAY_BUF_SIZES_SUM * 12);
  KeyFileClusterWrite<long, std::pair<double, double>>
      borderEdgeGeometryClusterWrite(pixelBorderEdgeGeomFiles,
                                     MAX_BORDER_EDGES_SIZES_SUM * 3);

  google::protobuf::int64 edgeId = 0;

  uint64_t totalBorderEdges = 0;
  uint64_t totalBorderNodes = 0;

  for (uint64_t i = 0; i < totalWaysCount; i++) {
    parser::Way way = *reinterpret_cast<parser::Way*>(
        static_cast<char*>(ways_map) + i * sizeof(parser::Way));

    std::pair<google::protobuf::int64, uint64_t>* wayNodePairs =
        reinterpret_cast<std::pair<google::protobuf::int64, uint64_t>*>(
            static_cast<char*>(way_nodes_map) +
            way.offset * sizeof(std::pair<google::protobuf::int64, uint64_t>));

    std::vector<std::pair<double, double>> geom;

    auto sourceWayNodePair = *wayNodePairs;

    uint64_t sourceHash = MurmurHash64A_1(wayNodePairs->first) & HASH_MASK;

    NodeData sourceTuple = *reinterpret_cast<NodeData*>(
        static_cast<char*>(get<2>(nodeTupleFiles[sourceHash])) +
        (wayNodePairs->second) * sizeof(NodeData));
    geom.emplace_back(sourceTuple.lat, sourceTuple.lon);

    auto prevTuple = sourceTuple;

    long sourceIpix;

    auto theta = (90 - sourceTuple.lat) * M_PI / 180;
    auto phi = sourceTuple.lon * M_PI / 180;

    ang2pix_ring(50, theta, phi, &sourceIpix);

    double cost = 0;

    for (uint64_t j = 1; j < way.size; j++) {
      auto wayNodePair = *(wayNodePairs + j);
      auto nodeId = wayNodePair.first;
      auto hash = MurmurHash64A_1(nodeId) & HASH_MASK;

      auto tupleHashMap = get<2>(nodeTupleFiles[hash]);

      NodeData wayNodeTuple = *reinterpret_cast<NodeData*>(
          static_cast<char*>(get<2>(nodeTupleFiles[hash])) +
          (wayNodePair.second) * sizeof(NodeData));
      geom.emplace_back(wayNodeTuple.lat, wayNodeTuple.lon);

      cost +=
          geopointsDistance(std::make_pair(prevTuple.lat, prevTuple.lon),
                            std::make_pair(wayNodeTuple.lat, wayNodeTuple.lon));

      prevTuple = wayNodeTuple;

      if (wayNodeTuple.used <= 1) {
        continue;
      }

      auto theta = (90 - wayNodeTuple.lat) * M_PI / 180;
      auto phi = wayNodeTuple.lon * M_PI / 180;

      long ipix;

      ang2pix_ring(50, theta, phi, &ipix);

      if (ipix == sourceIpix) {
        const auto geomOffsetIt = geomOffsets.find(ipix);
        if (geomOffsetIt == geomOffsets.end()) {
          std::cerr << "Geometry offset not found for partition" << std::endl;
          return 1;
        }

        auto geomOffset = geomOffsetIt->second;
        auto geomSize = geom.size();

        for (auto dot : geom) {
          edgeGeometryClusterWrite.add(ipix, dot);
        }

        // edgeFileClusterWrite.add(
        //     sourceIpix, parser::Edge{edgeId, way.id, 0,
        //     sourceWayNodePair.first,
        //                              sourceTuple.offset, wayNodePair.first,
        //                              wayNodeTuple.offset, cost, sourceIpix});

        edgeFileClusterWrite.add(
            sourceIpix,
            parser::Edge{edgeId, way.id, way.oneway, ipix, geomOffset,
                         static_cast<int64_t>(geomSize),
                         sourceWayNodePair.first, sourceTuple.offset,
                         wayNodePair.first, wayNodeTuple.offset, cost});

        edgeId++;
        if (!way.oneway) {
          // edgeFileClusterWrite.add(
          //     sourceIpix,
          // parser::Edge{edgeId, way.id, 0, wayNodePair.first,
          //              wayNodeTuple.offset, sourceWayNodePair.first,
          //              sourceTuple.offset, cost, sourceIpix});
          edgeFileClusterWrite.add(
              sourceIpix,
              parser::Edge{edgeId, way.id, way.oneway, ipix, geomOffset,
                           -static_cast<int64_t>(geomSize), wayNodePair.first,
                           wayNodeTuple.offset, sourceWayNodePair.first,
                           sourceTuple.offset, cost});
          edgeId++;
        }

        geomOffsetIt->second += geomSize;
      } else {
        totalBorderEdges++;
        totalBorderNodes += 2;

        borderNodesFileClusterWrite.add(
            sourceIpix, parser::Node{sourceWayNodePair.first, sourceTuple.lat,
                                     sourceTuple.lon});
        borderNodesFileClusterWrite.add(
            sourceIpix, parser::Node{wayNodePair.first, wayNodeTuple.lat,
                                     wayNodeTuple.lon});

        auto sourceOffsetsIt = borderOffsets.find(sourceIpix);

        if (sourceOffsetsIt == borderOffsets.end()) {
          std::cerr << "Offsets not found for source ipix" << std::endl;
          return 1;
        }

        sourceOffsetsIt->second += 2;

        borderNodesFileClusterWrite.add(
            ipix, parser::Node{sourceWayNodePair.first, sourceTuple.lat,
                               sourceTuple.lon});
        borderNodesFileClusterWrite.add(
            ipix, parser::Node{wayNodePair.first, wayNodeTuple.lat,
                               wayNodeTuple.lon});

        auto targetOffsetsIt = borderOffsets.find(ipix);
        targetOffsetsIt->second += 2;

        const auto geomSourceOffsetIt = borderGeomOffsets.find(sourceIpix);
        const auto geomTargetOffsetIt = borderGeomOffsets.find(ipix);
        if (geomSourceOffsetIt == borderGeomOffsets.end() ||
            geomTargetOffsetIt == borderGeomOffsets.end()) {
          std::cerr << "Geometry offset not found for one of partition"
                    << std::endl;
          return 1;
        }

        auto geomSize = geom.size();

        for (auto dot : geom) {
          borderEdgeGeometryClusterWrite.add(sourceIpix, dot);
          borderEdgeGeometryClusterWrite.add(ipix, dot);
        }

        // borderEdgeFileClusterWrite.add(
        //     sourceIpix,
        //     parser::Edge{edgeId, way.id, 0, sourceWayNodePair.first,
        //                  sourceOffsetsIt->second - 2, wayNodePair.first,
        //                  sourceOffsetsIt->second - 1, cost, 0});
        borderEdgeFileClusterWrite.add(
            sourceIpix,
            parser::Edge{edgeId, way.id, way.oneway, -sourceIpix,
                         geomSourceOffsetIt->second,
                         static_cast<int64_t>(geomSize),
                         sourceWayNodePair.first, sourceOffsetsIt->second - 2,
                         wayNodePair.first, sourceOffsetsIt->second - 1, cost});
        edgeId++;

        if (!way.oneway) {
          borderEdgeFileClusterWrite.add(
              sourceIpix,
              // parser::Edge{edgeId, way.id, 0, wayNodePair.first,
              //              sourceOffsetsIt->second - 1,
              //              sourceWayNodePair.first, sourceOffsetsIt->second -
              //              2, cost, 0});
              parser::Edge{edgeId, way.id, way.oneway, -sourceIpix,
                           geomSourceOffsetIt->second,
                           -static_cast<int64_t>(geomSize), wayNodePair.first,
                           sourceOffsetsIt->second - 1, sourceWayNodePair.first,
                           sourceOffsetsIt->second - 2, cost});
          edgeId++;
        }

        geomSourceOffsetIt->second += geomSize;

        // borderEdgeFileClusterWrite.add(
        //     ipix, parser::Edge{edgeId - 2, way.id, 0,
        //     sourceWayNodePair.first,
        //                        targetOffsetsIt->second - 2,
        //                        wayNodePair.first, targetOffsetsIt->second -
        //                        1, cost, 0});
        borderEdgeFileClusterWrite.add(
            ipix,
            parser::Edge{edgeId - 2, way.id, way.oneway, -ipix,
                         geomTargetOffsetIt->second,
                         static_cast<int64_t>(geomSize),
                         sourceWayNodePair.first, targetOffsetsIt->second - 2,
                         wayNodePair.first, targetOffsetsIt->second - 1, cost});

        if (!way.oneway) {
          // borderEdgeFileClusterWrite.add(
          //     ipix,
          //     parser::Edge{edgeId - 1, way.id, 0, wayNodePair.first,
          //                  targetOffsetsIt->second - 1,
          //                  sourceWayNodePair.first, targetOffsetsIt->second -
          //                  2, cost, 0});
          borderEdgeFileClusterWrite.add(
              ipix,
              parser::Edge{edgeId - 1, way.id, way.oneway, -ipix,
                           geomTargetOffsetIt->second,
                           -static_cast<int64_t>(geomSize), wayNodePair.first,
                           targetOffsetsIt->second - 1, sourceWayNodePair.first,
                           targetOffsetsIt->second - 2, cost});
        }

        geomTargetOffsetIt->second += geomSize;
      }

      geom.clear();
      sourceTuple = wayNodeTuple;
      geom.emplace_back(sourceTuple.lat, sourceTuple.lon);
      sourceWayNodePair = wayNodePair;
      sourceIpix = ipix;
      cost = 0;
      edgeId++;
    }
  }

  std::cerr << "Partitioned ways" << std::endl;

  for (const auto& t : nodeTupleFiles) {
    if (munmap(get<2>(t), get<1>(t))) {
      std::cerr << "failed unmapping" << std::endl;
      return 1;
    }
    close(get<0>(t));
  }

  waysFileRead.unmap_file();
  wayNodesFileRead.unmap_file();

  edgeFileClusterWrite.flush();
  edgeFileClusterWrite.close_fds();

  borderNodesFileClusterWrite.flush();
  borderNodesFileClusterWrite.close_fds();

  borderEdgeFileClusterWrite.flush();
  borderEdgeFileClusterWrite.close_fds();

  edgeGeometryClusterWrite.flush();
  edgeGeometryClusterWrite.close_fds();

  borderEdgeGeometryClusterWrite.flush();
  borderEdgeGeometryClusterWrite.close_fds();

  pixelWayFiles.clear();
  pixelBorderNodesFiles.clear();
  pixelBorderEdgesFiles.clear();
  pixelEdgeGeometryFiles.clear();
  pixelBorderEdgeGeomFiles.clear();

  borderOffsets.clear();
  geomOffsets.clear();

  auto startHashingRests = std::chrono::high_resolution_clock::now();

  fd_rests = open("./bin/restrictions.bin", O_RDONLY);
  unlink("./bin/restrictions.bin");
  FileRead restrictionsFileRead(fd_rests);

  void* map_rests = restrictionsFileRead.mmap_file();

  uint64_t restsCount =
      restrictionsFileRead.fsize() / sizeof(parser::Restriction);
  restrictionsFileRead.close_fd();

  std::vector<parser::Restriction> restrictions;

  for (uint64_t i = 0; i < restsCount; i++) {
    parser::Restriction* rest = reinterpret_cast<parser::Restriction*>(
        static_cast<char*>(map_rests) + i * sizeof(parser::Restriction));

    if (rest->type == -1) {
      continue;
    }

    restrictions.emplace_back(rest->id, rest->from, rest->via, rest->to,
                              rest->type);
  }

  restrictionsFileRead.unmap_file();

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

  std::unordered_map<long, FileRead> geoWayPartFiles;

  for (const auto ipix : usedPixels) {
    std::string filename =
        numFilename("./bin/geo-partitions/edges/edges-", ipix, "bin");
    int fd = open(filename.data(), O_RDONLY);
    geoWayPartFiles.emplace(std::piecewise_construct,
                            std::forward_as_tuple(ipix),
                            std::forward_as_tuple(fd));
  }

  for (const auto& ipix : usedPixels) {
    const auto fileIt = geoWayPartFiles.find(ipix);

    if (fileIt == geoWayPartFiles.end()) {
      std::cerr << "File descriptor not found for geo partition ways"
                << std::endl;
      return 1;
    }

    if (fileIt->second.fsize() == 0) {
      continue;
    }

    void* map = fileIt->second.mmap_file();

    uint64_t wayCount = fileIt->second.fsize() / sizeof(parser::Edge);
    fileIt->second.close_fd();
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

    fileIt->second.unmap_file();
  }

  geoWayPartFiles.clear();

  for (auto ipix : usedPixels) {
    std::string filename =
        numFilename("./bin/geo-partitions/border-edges/edges-", ipix, "bin");
    int fd = open(filename.data(), O_RDONLY);
    FileRead file(fd);

    if (file.fsize() == 0) {
      file.close_fd();
      continue;
    }

    void* map = file.mmap_file();

    uint64_t edgeCount = file.fsize() / sizeof(parser::Edge);
    file.close_fd();
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

    file.unmap_file();
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

  // process partitions

  google::protobuf::int64 expEdgeId = 0;
  std::mutex sumMutex;
  uint64_t sum = 0;

  tbb::parallel_for_each(usedPixels.begin(), usedPixels.end(), [&](long ipix) {
    std::string edgeFilename =
        numFilename("./bin/geo-partitions/edges/edges-", ipix, "bin");
    int fd_e = open(edgeFilename.data(), O_RDONLY);
    FileRead edgesFile(fd_e);
    // unlink(edgeFilename.data());

    if (edgesFile.fsize() == 0) {
      edgesFile.close_fd();
      return;
    }

    std::string nodeFilename =
        numFilename("./bin/geo-partitions/nodes/nodes-", ipix, "bin");
    int fd_n = open(nodeFilename.data(), O_RDONLY);
    FileRead nodesFile(fd_n);
    // unlink(nodeFilename.data());

    void* map_e = edgesFile.mmap_file();

    void* map_n = nodesFile.mmap_file();
    nodesFile.close_fd();

    uint64_t edgeCount = edgesFile.fsize() / sizeof(parser::Edge);
    edgesFile.close_fd();

    std::string filename_ee =
        numFilename("./bin/geo-partitions/exp-edges/exp-edges-", ipix, "bin");
    int fd_ee =
        open(filename_ee.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    FileWrite<parser::ExpandedEdge> expEdgesFileWrite(
        fd_ee, MAX_WAY_BUF_SIZE * 3 * sizeof(parser::ExpandedEdge));

    parser::graph::Graph graph(
        reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e)), edgeCount);

    for (uint64_t j = 0; j < edgeCount; j++) {
      auto edgePtr = reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e) +
                                                     j * sizeof(parser::Edge));
      const auto oeIt = graph.graph().find(edgePtr->targetNodeId);

      if (oeIt == graph.graph().end()) {
        continue;
      }

      std::vector<std::tuple<parser::Edge*, long, uint64_t>> outgoingEdges;

      for (auto ePtr : oeIt->second) {
        outgoingEdges.emplace_back(
            ePtr, ipix,
            ePtr - reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e)));
      }

      // parser::graph::invert::applyRestrictions(
      //     edgePtr, graph.graph(),
      //     reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
      //     onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
      //     expEdgesFileWrite);

      parser::graph::invert::applyRestrictions(
          edgePtr, ipix, j, outgoingEdges,
          reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
          onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
          expEdgesFileWrite);
    }

    std::string filename_be =
        numFilename("./bin/geo-partitions/border-edges/edges-", ipix, "bin");
    int fd_be = open(filename_be.data(), O_RDONLY);

    FileRead beEdgesFileRead(fd_be);

    if (beEdgesFileRead.fsize() != 0) {
      void* map_be = beEdgesFileRead.mmap_file();
      beEdgesFileRead.close_fd();

      parser::Edge* borderEdges =
          reinterpret_cast<parser::Edge*>(static_cast<char*>(map_be));

      uint64_t borderEdgeCount = beEdgesFileRead.fsize() / sizeof(parser::Edge);

      std::string filename_bn =
          numFilename("./bin/geo-partitions/border-nodes/nodes-", ipix, "bin");
      int fd_bn = open(filename_bn.data(), O_RDONLY);
      FileRead borderNodesFileRead(fd_bn);

      void* map_bn = borderNodesFileRead.mmap_file();
      borderNodesFileRead.close_fd();

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
        const auto oeIt = graph.graph().find((borderEdges + i)->targetNodeId);

        if (oeIt == graph.graph().end()) {
          continue;
        }

        std::vector<std::tuple<parser::Edge*, long, uint64_t>> outgoingEdges;

        for (auto ePtr : oeIt->second) {
          outgoingEdges.emplace_back(ePtr, ipix,
                                     ePtr - reinterpret_cast<parser::Edge*>(
                                                static_cast<char*>(map_e)));
        }

        parser::graph::invert::applyRestrictionsSourceBorder(
            borderEdges + i, -ipix, i, outgoingEdges,
            reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
            borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            expEdgesFileWrite);

        // parser::graph::invert::applyRestrictionsSourceBorder(
        //     borderEdges + i, graph.graph(),
        //     reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
        //     borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
        //     expEdgesFileWrite);
      }

      for (uint64_t i = 0; i < edgeCount; i++) {
        auto edgePtr = reinterpret_cast<parser::Edge*>(
            static_cast<char*>(map_e) + i * sizeof(parser::Edge));

        const auto oeIt = borderGraph.find((edgePtr)->targetNodeId);

        if (oeIt == borderGraph.end()) {
          continue;
        }

        std::vector<std::tuple<parser::Edge*, long, uint64_t>> outgoingEdges;

        for (auto ePtr : oeIt->second) {
          outgoingEdges.emplace_back(ePtr, -ipix, ePtr - borderEdges);
        }

        parser::graph::invert::applyRestrictionsSourcePartition(
            edgePtr, ipix, i, outgoingEdges,
            reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
            borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            expEdgesFileWrite);
        // parser::graph::invert::applyRestrictionsSourcePartition(
        //     edgePtr, borderGraph,
        //     reinterpret_cast<parser::Node*>(static_cast<char*>(map_n)),
        //     borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
        //     expEdgesFileWrite);
      }
      beEdgesFileRead.unmap_file();
      borderNodesFileRead.unmap_file();
    }

    nodesFile.unmap_file();
    edgesFile.unmap_file();

    if (expEdgesFileWrite.sizeAfterFlush() == 0) {
      expEdgesFileWrite.close_fd();
      return;
    }

    expEdgesFileWrite.flush();

    {
      std::unique_lock<std::mutex> lock(sumMutex);
      sum += expEdgesFileWrite.fsize() / sizeof(parser::ExpandedEdge);
    }

    expEdgesFileWrite.close_fd();
  });

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

  ska::flat_hash_map<long, FileRead> borderNodesFilesRead;
  ska::flat_hash_map<long, FileRead> borderEdgesFilesRead;

  for (auto ipix : usedPixels) {
    std::string filename_n =
        numFilename("./bin/geo-partitions/border-nodes/nodes-", ipix, "bin");
    int fd_n = open(filename_n.data(), O_RDONLY);

    borderNodesFilesRead.emplace(ipix, fd_n);

    std::string filename_e =
        numFilename("./bin/geo-partitions/border-edges/edges-", ipix, "bin");
    int fd_e = open(filename_e.data(), O_RDONLY);

    borderEdgesFilesRead.emplace(ipix, fd_e);
  }

  std::string bee_filename =
      "./bin/geo-partitions/exp-edges/border-exp-edges.bin";
  int bee_fd =
      open(bee_filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  FileWrite<parser::ExpandedEdge> borderExpEdgeFileWrite(
      bee_fd, MAX_BORDER_EDGES_SIZES_SUM);

  for (auto ipix : usedPixels) {
    const auto edgeFileIt = borderEdgesFilesRead.find(ipix);

    if (edgeFileIt == borderEdgesFilesRead.end()) {
      std::cerr << "Failed finding files for border" << std::endl;
      return 1;
    }

    if (edgeFileIt->second.fsize() == 0) {
      continue;
    }

    void* map_e = edgeFileIt->second.mmap_file();

    const auto nodeFileIt = borderNodesFilesRead.find(ipix);

    if (nodeFileIt == borderNodesFilesRead.end()) {
      std::cerr << "File with border nodes not found" << std::endl;
      return 1;
    }

    void* map_n = nodeFileIt->second.mmap_file();

    parser::Node* nodes =
        reinterpret_cast<parser::Node*>(static_cast<char*>(map_n));

    auto edges = reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e));

    uint64_t edgeCount = edgeFileIt->second.fsize() / sizeof(parser::Edge);

    std::unordered_map<long, std::vector<parser::Edge*>> edgeHashByTargetIpix;

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
      const auto targetNodesIt = borderNodesFilesRead.find(targetIpix);
      const auto targetEdgesIt = borderEdgesFilesRead.find(targetIpix);

      if (targetEdgesIt == borderEdgesFilesRead.end() ||
          targetNodesIt == borderNodesFilesRead.end()) {
        std::cerr << "File for target ipix not found" << std::endl;
        return 1;
      }

      void* map_n = targetNodesIt->second.mmap_file();

      parser::Node* targetNodes =
          reinterpret_cast<parser::Node*>(static_cast<char*>(map_n));

      if (targetEdgesIt->second.fsize() == 0) {
        continue;
      }

      void* map_e = targetEdgesIt->second.mmap_file();

      parser::Edge* targetEdges =
          reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e));

      uint64_t targetEdgeCount =
          targetEdgesIt->second.fsize() / sizeof(parser::Edge);

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
        const auto oeIt = borderGraph.find(edge->targetNodeId);

        if (oeIt == borderGraph.end()) {
          continue;
        }

        std::vector<std::tuple<parser::Edge*, long, uint64_t>> outgoingEdges;

        for (auto ePtr : oeIt->second) {
          outgoingEdges.emplace_back(ePtr, -targetIpix, ePtr - targetEdges);
        }

        parser::graph::invert::applyRestrictionsSourcePartition(
            edge, -ipix, edge - edges, outgoingEdges, nodes, targetNodes,
            onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            borderExpEdgeFileWrite);
        // parser::graph::invert::applyRestrictionsSourcePartition(
        //     edge, borderGraph, nodes, targetNodes, onlyRestrictionsMap,
        //     noRestrictionsHash, expEdgeId, borderExpEdgeFileWrite);
      }

      targetEdgesIt->second.unmap_file();
      targetNodesIt->second.unmap_file();
    }
    edgeFileIt->second.unmap_file();
    nodeFileIt->second.unmap_file();
  }

  borderExpEdgeFileWrite.flush();
  borderExpEdgeFileWrite.close_fd();

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

  std::cerr << "Total expanded edges: "
            << sum +
                   borderExpEdgeFileWrite.fsize() / sizeof(parser::ExpandedEdge)
            << std::endl;

  for (long ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/exp-edges/exp-edges-"
           << std::to_string(ipix) << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      continue;
    }

    // unlink(filename.data());
    close(fd);
  }

  for (long ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/border-edges/edges-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      continue;
    }

    // unlink(filename.data());
    close(fd);
  }

  for (long ipix : usedPixels) {
    std::ostringstream stream;
    stream << "./bin/geo-partitions/border-nodes/nodes-" << std::to_string(ipix)
           << ".bin";
    std::string filename = stream.str();
    int fd = open(filename.data(), O_RDONLY);
    if (fd == -1) {
      continue;
    }

    // unlink(filename.data());
    close(fd);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << "Execution time: " << duration.count() << "ms" << std::endl;

  // .csv output

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

  ofile
      // << "from_vertex_id,"
      // << "to_vertex_id,"
      << "weight,"
      << "geom,"
      << "was_one_way,"
      // << "edge_id,"
      << "osm_way_from,"
      << "osm_way_to,"
      << "osm_way_from_source_node,"
      << "osm_way_from_target_node,"
      << "osm_way_to_source_node,"
      << "osm_way_to_target_node" << std::endl;

  for (auto ipix : usedPixels) {
    int fd_e = open(
        numFilename("./bin/geo-partitions/edges/edges-", ipix, "bin").data(),
        O_RDONLY);
    int fd_be = open(
        numFilename("./bin/geo-partitions/border-edges/edges-", ipix, "bin")
            .data(),
        O_RDONLY);

    int fd_ee = open(
        numFilename("./bin/geo-partitions/exp-edges/exp-edges-", ipix, "bin")
            .data(),
        O_RDONLY);

    int fd_eg =
        open(numFilename("./bin/geo-partitions/edge-geometry/geo-", ipix, "bin")
                 .data(),
             O_RDONLY);
    int fd_beg =
        open(numFilename("./bin/geo-partitions/border-edge-geometry/geo-", ipix,
                         "bin")
                 .data(),
             O_RDONLY);

    FileRead edgesFileRead(fd_e);
    FileRead borderEdgesFileRead(fd_be);
    FileRead expEdgesFileRead(fd_ee);
    FileRead edgeGeomFileRead(fd_eg);
    FileRead borderEdgeGeomFileRead(fd_beg);

    void* map_e = edgesFileRead.mmap_file();
    parser::Edge* edges =
        reinterpret_cast<parser::Edge*>(static_cast<char*>(map_e));
    void* map_be = borderEdgesFileRead.mmap_file();
    parser::Edge* borderEdges =
        reinterpret_cast<parser::Edge*>(static_cast<char*>(map_be));
    void* map_ee = expEdgesFileRead.mmap_file();
    void* map_eg = edgeGeomFileRead.mmap_file();
    std::pair<double, double>* edgeGeom =
        reinterpret_cast<std::pair<double, double>*>(
            static_cast<char*>(map_eg));
    void* map_beg = borderEdgeGeomFileRead.mmap_file();
    std::pair<double, double>* borderEdgeGeom =
        reinterpret_cast<std::pair<double, double>*>(
            static_cast<char*>(map_beg));

    uint64_t eeCount = expEdgesFileRead.fsize() / sizeof(parser::ExpandedEdge);

    uint64_t beCount = borderEdgesFileRead.fsize() / sizeof(parser::Edge);

    for (uint64_t i = 0; i < eeCount; i++) {
      parser::ExpandedEdge* expEdge = reinterpret_cast<parser::ExpandedEdge*>(
          static_cast<char*>(map_ee) + i * sizeof(parser::ExpandedEdge));

      parser::Edge* sourceEdge;

      if (expEdge->sourcePart > 0) {
        sourceEdge = edges + expEdge->sourceEdgeOffset;
      } else {
        sourceEdge = borderEdges + expEdge->sourceEdgeOffset;
      }

      parser::Edge* targetEdge;

      if (expEdge->targetPart > 0) {
        targetEdge = edges + expEdge->targetEdgeOffset;
      } else {
        targetEdge = borderEdges + expEdge->targetEdgeOffset;
      }

      // std::cerr << "before" << std::endl;

      // std::cerr << expEdge->sourcePart << std::endl;
      // std::cerr << expEdge->targetEdgeOffset << std::endl;

      auto from_vertex_id = sourceEdge->id;
      auto to_vertex_id = targetEdge->id;
      auto weight = expEdge->cost;

      // std::cerr << "after" << std::endl;

      std::pair<double, double>* sourceGeom;

      if (sourceEdge->part > 0) {
        sourceGeom = edgeGeom + sourceEdge->geomOffset;
      } else {
        // std::cerr << "offset: " << sourceEdge->geomOffset << std::endl;
        sourceGeom = borderEdgeGeom + sourceEdge->geomOffset;
      }

      std::vector<std::pair<double, double>> sourceGeomList;

      // std::cerr << "before source geom: " << i << std::endl;

      if (sourceEdge->geomSize > 0) {
        for (uint64_t j = 0; j < sourceEdge->geomSize; j++) {
          sourceGeomList.push_back(*(sourceGeom + j));
        }
      } else {
        for (int64_t j = -sourceEdge->geomSize - 1; j >= 0; j--) {
          sourceGeomList.push_back(*(sourceGeom + j));
        }
      }

      // // std::cerr << "after target geom" << std::endl;

      auto midPoint = findMiddlePoint(sourceGeomList);

      std::vector<std::pair<double, double>> firstHalfGeom;
      firstHalfGeom.push_back(midPoint.second);

      for (uint64_t j = midPoint.first + 1; j < sourceGeomList.size(); j++) {
        firstHalfGeom.push_back(sourceGeomList[j]);
      }

      std::pair<double, double>* targetGeom;

      if (targetEdge->part > 0) {
        targetGeom = edgeGeom + targetEdge->geomOffset;
      } else {
        targetGeom = borderEdgeGeom + targetEdge->geomOffset;
      }

      std::vector<std::pair<double, double>> targetGeomList;

      if (targetEdge->geomSize > 0) {
        for (uint64_t j = 0; j < targetEdge->geomSize; j++) {
          targetGeomList.push_back(*(targetGeom + j));
        }
      } else {
        for (int64_t j = -targetEdge->geomSize - 1; j >= 0; j--) {
          targetGeomList.push_back(*(targetGeom + j));
        }
      }

      auto midPointTarget = findMiddlePoint(targetGeomList);

      std::vector<std::pair<double, double>> secondHalfGeom;

      for (uint64_t j = 0; j <= midPointTarget.first; j++) {
        secondHalfGeom.push_back(targetGeomList[j]);
      }

      secondHalfGeom.push_back(midPointTarget.second);

      auto was_one_way = sourceEdge->wasOneWay;
      auto edge_id = expEdge->id;
      auto osm_way_from = sourceEdge->wayId;
      auto osm_way_to = targetEdge->wayId;

      auto osm_way_from_source_node = sourceEdge->sourceNodeId;
      auto osm_way_from_target_node = sourceEdge->targetNodeId;
      auto osm_way_to_source_node = targetEdge->sourceNodeId;
      auto osm_way_to_target_node = targetEdge->targetNodeId;

      // ofile << from_vertex_id << ",";
      // ofile << to_vertex_id << ",";
      ofile << std::fixed << std::setprecision(6) << weight << ",";
      ofile << "LINESTRING(";

      for (uint64_t j = 0; j < firstHalfGeom.size(); j++) {
        ofile << std::fixed << std::setprecision(6) << firstHalfGeom[j].second
              << " " << firstHalfGeom[j].first << ";";
      }
      for (uint64_t j = 0; j < secondHalfGeom.size(); j++) {
        ofile << std::fixed << std::setprecision(6) << secondHalfGeom[j].second
              << " " << secondHalfGeom[j].first;
        if (j != secondHalfGeom.size() - 1) {
          ofile << ";";
        }
      }

      ofile << ")"
            << ",";

      ofile << (was_one_way ? "true" : "false") << ",";
      // ofile << edge_id << ",";
      ofile << osm_way_from << ",";
      ofile << osm_way_to << ",";
      ofile << osm_way_from_source_node << ",";
      ofile << osm_way_from_target_node << ",";
      ofile << osm_way_to_source_node << ",";
      ofile << osm_way_to_target_node << std::endl;
    }

    edgesFileRead.unmap_file();
    edgesFileRead.close_fd();

    borderEdgesFileRead.unmap_file();
    borderEdgesFileRead.close_fd();

    expEdgesFileRead.unmap_file();
    expEdgesFileRead.close_fd();

    edgeGeomFileRead.unmap_file();
    edgeGeomFileRead.close_fd();

    borderEdgeGeomFileRead.unmap_file();
    borderEdgeGeomFileRead.close_fd();
  }

  int fd_bee =
      open("./bin/geo-partitions/exp-edges/border-exp-edges.bin", O_RDONLY);

  FileRead borderExpEdgesRead(fd_bee);

  void* map_bee = borderExpEdgesRead.mmap_file();

  uint64_t beeCount = borderExpEdgesRead.fsize() / sizeof(parser::ExpandedEdge);

  std::unordered_map<long, std::vector<parser::ExpandedEdge*>> beeTab;

  for (uint64_t i = 0; i < beeCount; i++) {
    parser::ExpandedEdge* expEdge = reinterpret_cast<parser::ExpandedEdge*>(
        static_cast<char*>(map_bee) + i * sizeof(parser::ExpandedEdge));

    long ipix = -expEdge->sourcePart;

    auto it = beeTab.find(ipix);

    if (it == beeTab.end()) {
      it = beeTab
               .emplace(std::piecewise_construct, std::forward_as_tuple(ipix),
                        std::forward_as_tuple())
               .first;
      it->second.push_back(expEdge);
    } else {
      it->second.push_back(expEdge);
    }

    // beeTab.emplace(ipix, expEdge);
  }

  for (const auto& [ipix, buf] : beeTab) {
    int fd_be_source = open(
        numFilename("./bin/geo-partitions/border-edges/edges-", ipix, "bin")
            .data(),
        O_RDONLY);
    FileRead borderEdgesSourceRead(fd_be_source);

    void* map_be_source = borderEdgesSourceRead.mmap_file();

    parser::Edge* borderEdgesSource =
        reinterpret_cast<parser::Edge*>(static_cast<char*>(map_be_source));

    int fd_beg_source =
        open(numFilename("./bin/geo-partitions/border-edge-geometry/geo-", ipix,
                         "bin")
                 .data(),
             O_RDONLY);
    FileRead borderEdgeGeomSourceRead(fd_beg_source);

    void* map_beg_source = borderEdgeGeomSourceRead.mmap_file();

    std::pair<double, double>* borderEdgeGeoms =
        reinterpret_cast<std::pair<double, double>*>(
            static_cast<char*>(map_beg_source));

    std::unordered_map<long, std::vector<parser::ExpandedEdge*>>
        expEdgesByTargetIpix;

    for (auto expEdgePtr : buf) {
      long targetIpix = -expEdgePtr->targetPart;

      auto it = expEdgesByTargetIpix.find(targetIpix);

      if (it == expEdgesByTargetIpix.end()) {
        it = expEdgesByTargetIpix
                 .emplace(std::piecewise_construct,
                          std::forward_as_tuple(targetIpix),
                          std::forward_as_tuple())
                 .first;
      }

      it->second.push_back(expEdgePtr);
    }

    for (const auto& [targetIpix, targetBuf] : expEdgesByTargetIpix) {
      int fd_be_target =
          open(numFilename("./bin/geo-partitions/border-edges/edges-",
                           targetIpix, "bin")
                   .data(),
               O_RDONLY);
      FileRead borderEdgesTargetRead(fd_be_target);
      void* map_be_target = borderEdgesTargetRead.mmap_file();
      parser::Edge* borderEdgesTarget =
          reinterpret_cast<parser::Edge*>(static_cast<char*>(map_be_target));

      int fd_beg_target =
          open(numFilename("./bin/geo-partitions/border-edge-geometry/geo-",
                           targetIpix, "bin")
                   .data(),
               O_RDONLY);
      FileRead borderEdgeGeomTargetRead(fd_beg_target);
      void* map_beg_target = borderEdgeGeomTargetRead.mmap_file();
      std::pair<double, double>* borderEdgeGeomsTarget =
          reinterpret_cast<std::pair<double, double>*>(
              static_cast<char*>(map_beg_target));

      for (auto ee : targetBuf) {
        parser::Edge* sourceEdge = borderEdgesSource + ee->sourceEdgeOffset;

        parser::Edge* targetEdge = borderEdgesTarget + ee->targetEdgeOffset;

        auto from_vertex_id = sourceEdge->id;
        auto to_vertex_id = targetEdge->id;
        auto weight = ee->cost;

        std::pair<double, double>* sourceGeom =
            borderEdgeGeoms + sourceEdge->geomOffset;

        std::vector<std::pair<double, double>> sourceGeomList;

        if (sourceEdge->geomSize > 0) {
          for (uint64_t j = 0; j < sourceEdge->geomSize; j++) {
            sourceGeomList.push_back(*(sourceGeom + j));
          }
        } else {
          for (int64_t j = -sourceEdge->geomSize - 1; j >= 0; j--) {
            sourceGeomList.push_back(*(sourceGeom + j));
          }
        }

        auto midPoint = findMiddlePoint(sourceGeomList);

        std::vector<std::pair<double, double>> firstHalfGeom;
        firstHalfGeom.push_back(midPoint.second);

        for (uint64_t j = midPoint.first + 1; j < sourceGeomList.size(); j++) {
          firstHalfGeom.push_back(sourceGeomList[j]);
        }

        std::pair<double, double>* targetGeom =
            borderEdgeGeomsTarget + targetEdge->geomOffset;

        std::vector<std::pair<double, double>> targetGeomList;

        if (targetEdge->geomSize > 0) {
          for (uint64_t j = 0; j < targetEdge->geomSize; j++) {
            targetGeomList.push_back(*(targetGeom + j));
          }
        } else {
          for (int64_t j = -targetEdge->geomSize - 1; j >= 0; j--) {
            targetGeomList.push_back(*(targetGeom + j));
          }
        }

        auto midPointTarget = findMiddlePoint(targetGeomList);

        std::vector<std::pair<double, double>> secondHalfGeom;

        for (uint64_t j = 0; j <= midPointTarget.first; j++) {
          secondHalfGeom.push_back(targetGeomList[j]);
        }

        secondHalfGeom.push_back(midPointTarget.second);

        auto was_one_way = sourceEdge->wasOneWay;
        auto edge_id = ee->id;
        auto osm_way_from = sourceEdge->wayId;
        auto osm_way_to = targetEdge->wayId;

        auto osm_way_from_source_node = sourceEdge->sourceNodeId;
        auto osm_way_from_target_node = sourceEdge->targetNodeId;
        auto osm_way_to_source_node = targetEdge->sourceNodeId;
        auto osm_way_to_target_node = targetEdge->targetNodeId;

        // ofile << from_vertex_id << ",";
        // ofile << to_vertex_id << ",";
        ofile << std::fixed << std::setprecision(6) << weight << ",";
        ofile << "LINESTRING(";

        for (uint64_t j = 0; j < firstHalfGeom.size(); j++) {
          ofile << std::fixed << std::setprecision(6) << firstHalfGeom[j].second
                << " " << firstHalfGeom[j].first << ";";
        }
        for (uint64_t j = 0; j < secondHalfGeom.size(); j++) {
          ofile << std::fixed << std::setprecision(6)
                << secondHalfGeom[j].second << " " << secondHalfGeom[j].first;
          if (j != secondHalfGeom.size() - 1) {
            ofile << ";";
          }
        }

        ofile << ")"
              << ",";

        ofile << (was_one_way ? "true" : "false") << ",";
        // ofile << edge_id << ",";
        ofile << osm_way_from << ",";
        ofile << osm_way_to << ",";
        ofile << osm_way_from_source_node << ",";
        ofile << osm_way_from_target_node << ",";
        ofile << osm_way_to_source_node << ",";
        ofile << osm_way_to_target_node << std::endl;
      }

      borderEdgesTargetRead.unmap_file();
      borderEdgesTargetRead.close_fd();

      borderEdgeGeomTargetRead.unmap_file();
      borderEdgeGeomTargetRead.close_fd();
    }
    borderEdgeGeomSourceRead.unmap_file();
    borderEdgeGeomSourceRead.close_fd();

    borderEdgesSourceRead.unmap_file();
    borderEdgesSourceRead.close_fd();
  }

  return 0;
}