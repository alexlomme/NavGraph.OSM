#include <arpa/inet.h>
#include <chealpix.h>
#include <fcntl.h>
#include <osmpbf/fileformat.pb.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_for_each.h>
#include <unistd.h>

#include <benchmark/filestat.hpp>
#include <cmath>
#include <csv/generate.hpp>
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
#include <hash-part/types/extended-node.hpp>
#include <hash-part/types/node-data.hpp>
#include <hash-part/utils.hpp>
#include <healpix/utils.hpp>
#include <iostream>
#include <mt/mutexes.hpp>
#include <mt/parse/producer.hpp>
#include <mt/parse/worker.hpp>
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

  std::vector<FileWrite<ngosm::types::Node>> nodeFiles;

  for (uint64_t i = 0; i < ngosm::hashpart::HP_NUM; i++) {
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
  FileWrite<ngosm::types::Way> waysFile(
      way_fd,
      ngosm::buf::parse::MAX_WAY_CLUSTER_SIZE * sizeof(ngosm::types::Way));

  int way_node_fd = open("./bin/way-nodes.bin", O_RDWR | O_CREAT | O_TRUNC,
                         S_IRUSR | S_IWUSR);
  FileWrite<std::pair<google::protobuf::int64, uint64_t>> wayNodesFile(
      way_node_fd, ngosm::buf::parse::MAX_WAY_NODES_CLUSTER_SIZE *
                       sizeof(std::pair<google::protobuf::int64, uint64_t>));

  // open used-nodes files in node-hash partitions

  std::vector<FileWrite<google::protobuf::int64>> usedNodesFiles;

  for (uint64_t i = 0; i < ngosm::hashpart::HP_NUM; i++) {
    std::string filename = numFilename(
        "./bin/node-hash-partitions/used-nodes/used-nodes-", i, "bin");
    int fd =
        open(filename.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    usedNodesFiles.emplace_back(fd,
                                ngosm::buf::parse::MAX_USED_NODES_CLUSTER_SIZE *
                                    sizeof(google::protobuf::int64));
  }

  // open restrictions file for writing

  int fd_rests = open("./bin/restrictions.bin", O_RDWR | O_CREAT | O_TRUNC,
                      S_IRUSR | S_IWUSR);
  FileWrite<ngosm::types::Restriction> restrictionFile(
      fd_rests, ngosm::buf::parse::MAX_RESTRICTIONS_CLUSTER_SIZE *
                    sizeof(ngosm::types::Restriction));

  ska::flat_hash_set<long> pixels;

  ngosm::bm::Filestat filestat;

  ngosm::mt::Mutexes mutexes;

  uint64_t wayNodesOffset = 0;
  std::vector<uint64_t> hashPartOffsets(ngosm::hashpart::HP_NUM);

  std::thread producer_thread(
      [&]() { ngosm::mt::parse::producer(pbf_data, eof_pbf, mutexes); });

  const uint64_t workers_num = std::thread::hardware_concurrency();
  std::vector<std::thread> workers;

  for (int i = 0; i < workers_num; i++) {
    workers.emplace_back([&]() {
      ngosm::mt::parse::worker(nodeFiles, waysFile, wayNodesFile,
                               usedNodesFiles, restrictionFile, pixels, mutexes,
                               filestat, wayNodesOffset, hashPartOffsets);
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

  for (uint64_t i = 0; i < ngosm::hashpart::HP_NUM; i++) {
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

  for (uint64_t i = 0; i < ngosm::hashpart::HP_NUM; i++) {
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

  KeyFileClusterWrite<long, ngosm::types::Node> nodePartsCluster(
      pixelNodeFiles,
      ngosm::healpix::MAX_NODE_CLUSTER_SIZE * sizeof(ngosm::types::Node));

  tbb::concurrent_unordered_set<long> usedPixels;

  std::unordered_map<long, uint64_t> nodeOffsets;

  std::mutex nodesClusterMutex;

  for (uint16_t i = 0; i < ngosm::hashpart::HP_NUM; i++) {
    auto hashPartitionStart = std::chrono::high_resolution_clock::now();

    std::string rn_fn = numFilename(
        "./bin/node-hash-partitions/reduced-nodes/nodes-", i, "bin");
    int rn_fd =
        open(rn_fn.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    FileWrite<ngosm::hashpart::types::ExtendedNode> rnFile(
        rn_fd, ngosm::hashpart::MAX_RN_BUF_SIZE *
                   sizeof(ngosm::hashpart::types::ExtendedNode));

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

    uint64_t nodesCount = nodesFile.fsize() / sizeof(ngosm::types::Node);
    nodesFile.close_fd();

    for (uint64_t j = 0; j < nodesCount; j++) {
      ngosm::types::Node* n = reinterpret_cast<ngosm::types::Node*>(
          static_cast<char*>(n_map) + j * sizeof(ngosm::types::Node));
      const auto usedNodePairIt = usedNodes.find(n->id);
      if (usedNodePairIt == usedNodes.end()) {
        continue;
      }
      auto theta = (90 - n->lat) * M_PI / 180;
      auto phi = n->lon * M_PI / 180;

      long ipix;

      ang2pix_ring(ngosm::healpix::N_SIDE, theta, phi, &ipix);

      usedPixels.insert(ipix);

      std::unordered_map<long, uint64_t>::iterator offsetIt;

      nodePartsCluster.add(ipix, ngosm::types::Node{n->id, n->lat, n->lon});

      offsetIt = nodeOffsets.find(ipix);
      if (offsetIt == nodeOffsets.end()) {
        offsetIt = nodeOffsets.emplace(ipix, 0).first;
      } else {
        offsetIt->second++;
      }

      rnFile.add(ngosm::hashpart::types::ExtendedNode{
          n->id, n->lat, n->lon, usedNodePairIt->second, offsetIt->second});
    }
    nodesFile.unmap_file();
    rnFile.flush();
    rnFile.close_fd();

    usedNodes.clear();

    rn_fd = open(rn_fn.data(), O_RDONLY);
    FileRead rnFileRead(rn_fd);
    unlink(rn_fn.data());

    void* map_rn = rnFileRead.mmap_file();

    uint64_t reducedNodesCount =
        rnFileRead.fsize() / sizeof(ngosm::hashpart::types::ExtendedNode);
    rnFileRead.close_fd();

    ska::flat_hash_map<google::protobuf::int64,
                       ngosm::hashpart::types::ExtendedNode*>
        nodesHash;

    for (uint64_t j = 0; j < reducedNodesCount; j++) {
      ngosm::hashpart::types::ExtendedNode* n =
          reinterpret_cast<ngosm::hashpart::types::ExtendedNode*>(
              static_cast<char*>(map_rn) +
              j * sizeof(ngosm::hashpart::types::ExtendedNode));

      nodesHash.emplace(n->id, n);
    }

    std::string tuples_filename = numFilename(
        "./bin/node-hash-partitions/node-data/node-data-", i, "bin");
    int tuples_fd = open(tuples_filename.data(), O_RDWR | O_CREAT | O_TRUNC,
                         S_IRUSR | S_IWUSR);

    FileWrite<ngosm::hashpart::types::NodeData> tupleFileWrite(
        tuples_fd, ngosm::hashpart::MAX_NODEDATA_BUF_SIZE *
                       sizeof(ngosm::hashpart::types::NodeData));

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

      tupleFileWrite.add(ngosm::hashpart::types::NodeData{
          nodeIt->second->offset, nodeIt->second->lat, nodeIt->second->lon,
          nodeIt->second->used});
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

  uint64_t totalWaysCount = waysFileRead.fsize() / sizeof(ngosm::types::Way);
  waysFileRead.close_fd();

  int fd_way_nodes_read = open("./bin/way-nodes.bin", O_RDONLY);
  FileRead wayNodesFileRead(fd_way_nodes_read);
  unlink("./bin/way-nodes.bin");

  void* way_nodes_map = wayNodesFileRead.mmap_file();
  wayNodesFileRead.close_fd();

  std::vector<std::tuple<int, uint64_t, void*>> nodeTupleFiles;
  nodeTupleFiles.resize(ngosm::hashpart::HP_NUM);

  for (uint64_t i = 0; i < ngosm::hashpart::HP_NUM; i++) {
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
    void* map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);

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

  KeyFileClusterWrite<long, ngosm::types::Edge> edgeFileClusterWrite(
      pixelWayFiles,
      ngosm::healpix::MAX_EDGES_CLUSTER_SIZE * sizeof(ngosm::types::Edge));
  KeyFileClusterWrite<long, ngosm::types::Edge> borderEdgeFileClusterWrite(
      pixelBorderEdgesFiles, ngosm::healpix::MAX_BE_CLUSTER_SIZE);
  KeyFileClusterWrite<long, ngosm::types::Node> borderNodesFileClusterWrite(
      pixelBorderNodesFiles, ngosm::healpix::MAX_BN_CLUSTER_SIZE);
  KeyFileClusterWrite<long, std::pair<double, double>> edgeGeometryClusterWrite(
      pixelEdgeGeometryFiles, ngosm::healpix::MAX_EDGES_CLUSTER_SIZE * 12);
  KeyFileClusterWrite<long, std::pair<double, double>>
      borderEdgeGeometryClusterWrite(pixelBorderEdgeGeomFiles,
                                     ngosm::healpix::MAX_BE_CLUSTER_SIZE * 3);

  google::protobuf::int64 edgeId = 0;

  uint64_t totalBorderEdges = 0;
  uint64_t totalBorderNodes = 0;

  for (uint64_t i = 0; i < totalWaysCount; i++) {
    ngosm::types::Way way = *reinterpret_cast<ngosm::types::Way*>(
        static_cast<char*>(ways_map) + i * sizeof(ngosm::types::Way));

    std::pair<google::protobuf::int64, uint64_t>* wayNodePairs =
        reinterpret_cast<std::pair<google::protobuf::int64, uint64_t>*>(
            static_cast<char*>(way_nodes_map) +
            way.offset * sizeof(std::pair<google::protobuf::int64, uint64_t>));

    std::vector<std::pair<double, double>> geom;

    auto sourceWayNodePair = *wayNodePairs;

    uint64_t sourceHash = ngosm::hashpart::partnum(wayNodePairs->first);

    ngosm::hashpart::types::NodeData sourceTuple =
        *reinterpret_cast<ngosm::hashpart::types::NodeData*>(
            static_cast<char*>(get<2>(nodeTupleFiles[sourceHash])) +
            (wayNodePairs->second) * sizeof(ngosm::hashpart::types::NodeData));
    geom.emplace_back(sourceTuple.lat, sourceTuple.lon);

    auto prevTuple = sourceTuple;

    long sourceIpix;

    auto theta = (90 - sourceTuple.lat) * M_PI / 180;
    auto phi = sourceTuple.lon * M_PI / 180;

    ang2pix_ring(ngosm::healpix::N_SIDE, theta, phi, &sourceIpix);

    double cost = 0;

    for (uint64_t j = 1; j < way.size; j++) {
      auto wayNodePair = *(wayNodePairs + j);
      auto nodeId = wayNodePair.first;
      auto hash = ngosm::hashpart::partnum(nodeId);

      auto tupleHashMap = get<2>(nodeTupleFiles[hash]);

      ngosm::hashpart::types::NodeData wayNodeTuple =
          *reinterpret_cast<ngosm::hashpart::types::NodeData*>(
              static_cast<char*>(get<2>(nodeTupleFiles[hash])) +
              (wayNodePair.second) * sizeof(ngosm::hashpart::types::NodeData));
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

      ang2pix_ring(ngosm::healpix::N_SIDE, theta, phi, &ipix);

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

        edgeFileClusterWrite.add(
            sourceIpix,
            ngosm::types::Edge{edgeId, way.id, way.oneway, ipix, geomOffset,
                               static_cast<int64_t>(geomSize),
                               sourceWayNodePair.first, sourceTuple.offset,
                               wayNodePair.first, wayNodeTuple.offset, cost});

        edgeId++;
        if (!way.oneway) {
          edgeFileClusterWrite.add(
              sourceIpix,
              ngosm::types::Edge{edgeId, way.id, way.oneway, ipix, geomOffset,
                                 -static_cast<int64_t>(geomSize),
                                 wayNodePair.first, wayNodeTuple.offset,
                                 sourceWayNodePair.first, sourceTuple.offset,
                                 cost});
          edgeId++;
        }

        geomOffsetIt->second += geomSize;
      } else {
        totalBorderEdges++;
        totalBorderNodes += 2;

        borderNodesFileClusterWrite.add(
            sourceIpix, ngosm::types::Node{sourceWayNodePair.first,
                                           sourceTuple.lat, sourceTuple.lon});
        borderNodesFileClusterWrite.add(
            sourceIpix, ngosm::types::Node{wayNodePair.first, wayNodeTuple.lat,
                                           wayNodeTuple.lon});

        auto sourceOffsetsIt = borderOffsets.find(sourceIpix);

        if (sourceOffsetsIt == borderOffsets.end()) {
          std::cerr << "Offsets not found for source ipix" << std::endl;
          return 1;
        }

        sourceOffsetsIt->second += 2;

        borderNodesFileClusterWrite.add(
            ipix, ngosm::types::Node{sourceWayNodePair.first, sourceTuple.lat,
                                     sourceTuple.lon});
        borderNodesFileClusterWrite.add(
            ipix, ngosm::types::Node{wayNodePair.first, wayNodeTuple.lat,
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

        borderEdgeFileClusterWrite.add(
            sourceIpix,
            ngosm::types::Edge{
                edgeId, way.id, way.oneway, -sourceIpix,
                geomSourceOffsetIt->second, static_cast<int64_t>(geomSize),
                sourceWayNodePair.first, sourceOffsetsIt->second - 2,
                wayNodePair.first, sourceOffsetsIt->second - 1, cost});
        edgeId++;

        if (!way.oneway) {
          borderEdgeFileClusterWrite.add(
              sourceIpix,
              ngosm::types::Edge{
                  edgeId, way.id, way.oneway, -sourceIpix,
                  geomSourceOffsetIt->second, -static_cast<int64_t>(geomSize),
                  wayNodePair.first, sourceOffsetsIt->second - 1,
                  sourceWayNodePair.first, sourceOffsetsIt->second - 2, cost});
          edgeId++;
        }

        geomSourceOffsetIt->second += geomSize;

        borderEdgeFileClusterWrite.add(
            ipix,
            ngosm::types::Edge{
                edgeId - 2, way.id, way.oneway, -ipix,
                geomTargetOffsetIt->second, static_cast<int64_t>(geomSize),
                sourceWayNodePair.first, targetOffsetsIt->second - 2,
                wayNodePair.first, targetOffsetsIt->second - 1, cost});

        if (!way.oneway) {
          borderEdgeFileClusterWrite.add(
              ipix,
              ngosm::types::Edge{
                  edgeId - 1, way.id, way.oneway, -ipix,
                  geomTargetOffsetIt->second, -static_cast<int64_t>(geomSize),
                  wayNodePair.first, targetOffsetsIt->second - 1,
                  sourceWayNodePair.first, targetOffsetsIt->second - 2, cost});
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
      restrictionsFileRead.fsize() / sizeof(ngosm::types::Restriction);
  restrictionsFileRead.close_fd();

  std::vector<ngosm::types::Restriction> restrictions;

  for (uint64_t i = 0; i < restsCount; i++) {
    ngosm::types::Restriction* rest =
        reinterpret_cast<ngosm::types::Restriction*>(
            static_cast<char*>(map_rests) +
            i * sizeof(ngosm::types::Restriction));

    if (rest->type == -1) {
      continue;
    }

    restrictions.emplace_back(rest->id, rest->from, rest->via, rest->to,
                              rest->type);
  }

  restrictionsFileRead.unmap_file();

  std::cerr << "Read restrictions from file" << std::endl;

  std::unordered_multimap<google::protobuf::int64, ngosm::types::Restriction*>
      onlyRestrictionsByTo;
  std::unordered_map<
      std::tuple<google::protobuf::int64, google::protobuf::int64>,
      ngosm::types::Restriction*>
      noRestrictionsHash;

  ngosm::processing::hash_restrictions(restrictions, onlyRestrictionsByTo,
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
      ngosm::types::Restriction*>
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

    uint64_t wayCount = fileIt->second.fsize() / sizeof(ngosm::types::Edge);
    fileIt->second.close_fd();
    for (uint64_t i = 0; i < wayCount; i++) {
      ngosm::types::Edge* edge = reinterpret_cast<ngosm::types::Edge*>(
          static_cast<char*>(map) + i * sizeof(ngosm::types::Edge));
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

    uint64_t edgeCount = file.fsize() / sizeof(ngosm::types::Edge);
    file.close_fd();
    for (uint64_t i = 0; i < edgeCount; i++) {
      ngosm::types::Edge* edge = reinterpret_cast<ngosm::types::Edge*>(
          static_cast<char*>(map) + i * sizeof(ngosm::types::Edge));
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

    uint64_t edgeCount = edgesFile.fsize() / sizeof(ngosm::types::Edge);
    edgesFile.close_fd();

    std::string filename_ee =
        numFilename("./bin/geo-partitions/exp-edges/exp-edges-", ipix, "bin");
    int fd_ee =
        open(filename_ee.data(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    FileWrite<ngosm::types::ExpandedEdge> expEdgesFileWrite(
        fd_ee, ngosm::buf::parse::MAX_WAY_CLUSTER_SIZE * 3 *
                   sizeof(ngosm::types::ExpandedEdge));

    ngosm::graph::Graph graph(
        reinterpret_cast<ngosm::types::Edge*>(static_cast<char*>(map_e)),
        edgeCount);

    for (uint64_t j = 0; j < edgeCount; j++) {
      auto edgePtr = reinterpret_cast<ngosm::types::Edge*>(
          static_cast<char*>(map_e) + j * sizeof(ngosm::types::Edge));
      const auto oeIt = graph.graph().find(edgePtr->targetNodeId);

      if (oeIt == graph.graph().end()) {
        continue;
      }

      std::vector<std::tuple<ngosm::types::Edge*, long, uint64_t>>
          outgoingEdges;

      for (auto ePtr : oeIt->second) {
        outgoingEdges.emplace_back(ePtr, ipix,
                                   ePtr - reinterpret_cast<ngosm::types::Edge*>(
                                              static_cast<char*>(map_e)));
      }

      ngosm::graph::invert::applyRestrictions(
          edgePtr, ipix, j, outgoingEdges,
          reinterpret_cast<ngosm::types::Node*>(static_cast<char*>(map_n)),
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

      ngosm::types::Edge* borderEdges =
          reinterpret_cast<ngosm::types::Edge*>(static_cast<char*>(map_be));

      uint64_t borderEdgeCount =
          beEdgesFileRead.fsize() / sizeof(ngosm::types::Edge);

      std::string filename_bn =
          numFilename("./bin/geo-partitions/border-nodes/nodes-", ipix, "bin");
      int fd_bn = open(filename_bn.data(), O_RDONLY);
      FileRead borderNodesFileRead(fd_bn);

      void* map_bn = borderNodesFileRead.mmap_file();
      borderNodesFileRead.close_fd();

      ngosm::types::Node* borderNodes =
          reinterpret_cast<ngosm::types::Node*>(static_cast<char*>(map_bn));

      std::unordered_map<google::protobuf::int64,
                         std::vector<ngosm::types::Edge*>>
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

        std::vector<std::tuple<ngosm::types::Edge*, long, uint64_t>>
            outgoingEdges;

        for (auto ePtr : oeIt->second) {
          outgoingEdges.emplace_back(
              ePtr, ipix,
              ePtr - reinterpret_cast<ngosm::types::Edge*>(
                         static_cast<char*>(map_e)));
        }

        ngosm::graph::invert::applyRestrictionsSourceBorder(
            borderEdges + i, -ipix, i, outgoingEdges,
            reinterpret_cast<ngosm::types::Node*>(static_cast<char*>(map_n)),
            borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            expEdgesFileWrite);
      }

      for (uint64_t i = 0; i < edgeCount; i++) {
        auto edgePtr = reinterpret_cast<ngosm::types::Edge*>(
            static_cast<char*>(map_e) + i * sizeof(ngosm::types::Edge));

        const auto oeIt = borderGraph.find((edgePtr)->targetNodeId);

        if (oeIt == borderGraph.end()) {
          continue;
        }

        std::vector<std::tuple<ngosm::types::Edge*, long, uint64_t>>
            outgoingEdges;

        for (auto ePtr : oeIt->second) {
          outgoingEdges.emplace_back(ePtr, -ipix, ePtr - borderEdges);
        }

        ngosm::graph::invert::applyRestrictionsSourcePartition(
            edgePtr, ipix, i, outgoingEdges,
            reinterpret_cast<ngosm::types::Node*>(static_cast<char*>(map_n)),
            borderNodes, onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            expEdgesFileWrite);
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
      sum += expEdgesFileWrite.fsize() / sizeof(ngosm::types::ExpandedEdge);
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
  FileWrite<ngosm::types::ExpandedEdge> borderExpEdgeFileWrite(
      bee_fd, ngosm::healpix::MAX_BE_CLUSTER_SIZE);

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

    ngosm::types::Node* nodes =
        reinterpret_cast<ngosm::types::Node*>(static_cast<char*>(map_n));

    auto edges =
        reinterpret_cast<ngosm::types::Edge*>(static_cast<char*>(map_e));

    uint64_t edgeCount =
        edgeFileIt->second.fsize() / sizeof(ngosm::types::Edge);

    std::unordered_map<long, std::vector<ngosm::types::Edge*>>
        edgeHashByTargetIpix;

    for (uint64_t i = 0; i < edgeCount; i++) {
      auto edge = reinterpret_cast<ngosm::types::Edge*>(
          static_cast<char*>(map_e) + i * sizeof(ngosm::types::Edge));
      auto sourceNode = nodes + edge->sourceNodeOffset;

      auto sourceTheta = (90 - sourceNode->lat) * M_PI / 180;
      auto sourcePhi = sourceNode->lon * M_PI / 180;

      long sourceIpix;

      ang2pix_ring(ngosm::healpix::N_SIDE, sourceTheta, sourcePhi, &sourceIpix);

      if (sourceIpix != ipix) {
        continue;
      }

      auto targetNode = nodes + edge->targetNodeOffset;

      auto targetTheta = (90 - targetNode->lat) * M_PI / 180;
      auto targetPhi = targetNode->lon * M_PI / 180;

      long targetIpix;

      ang2pix_ring(ngosm::healpix::N_SIDE, targetTheta, targetPhi, &targetIpix);

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

      ngosm::types::Node* targetNodes =
          reinterpret_cast<ngosm::types::Node*>(static_cast<char*>(map_n));

      if (targetEdgesIt->second.fsize() == 0) {
        continue;
      }

      void* map_e = targetEdgesIt->second.mmap_file();

      ngosm::types::Edge* targetEdges =
          reinterpret_cast<ngosm::types::Edge*>(static_cast<char*>(map_e));

      uint64_t targetEdgeCount =
          targetEdgesIt->second.fsize() / sizeof(ngosm::types::Edge);

      std::unordered_map<google::protobuf::int64,
                         std::vector<ngosm::types::Edge*>>
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

        std::vector<std::tuple<ngosm::types::Edge*, long, uint64_t>>
            outgoingEdges;

        for (auto ePtr : oeIt->second) {
          outgoingEdges.emplace_back(ePtr, -targetIpix, ePtr - targetEdges);
        }

        ngosm::graph::invert::applyRestrictionsSourcePartition(
            edge, -ipix, edge - edges, outgoingEdges, nodes, targetNodes,
            onlyRestrictionsMap, noRestrictionsHash, expEdgeId,
            borderExpEdgeFileWrite);
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
            << sum + borderExpEdgeFileWrite.fsize() /
                         sizeof(ngosm::types::ExpandedEdge)
            << std::endl;

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << "Execution time: " << duration.count() << "ms" << std::endl;

  if (argc > 2) {
    std::string outputFile = argv[2];

    ngosm::csv::generate(usedPixels, outputFile);
  }

  return 0;
}