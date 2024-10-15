#pragma once

#include <chealpix.h>
#include <osmpbf/fileformat.pb.h>

#include <benchmark/filestat.hpp>
#include <disk/file-write.hpp>
#include <hash-part/utils.hpp>
#include <healpix/utils.hpp>
#include <mt/mutexes.hpp>
#include <parsing/primitive-block-parser.hpp>
#include <tables/skalib/flat_hash_map.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>
#include <utils/libdeflate_decomp.hpp>
#include <vector>

namespace ngosm {
namespace mt {
namespace parse {

void worker(
    std::vector<FileWrite<ngosm::types::Node>>& nodeFiles,
    FileWrite<ngosm::types::Way>& waysFile,
    FileWrite<std::pair<google::protobuf::int64, uint64_t>>& wayNodesFile,
    std::vector<FileWrite<google::protobuf::int64>>& usedNodesFiles,
    FileWrite<ngosm::types::Restriction>& restrictionFile,
    ska::flat_hash_set<long>& pixels, ngosm::mt::Mutexes& mutexes,
    ngosm::bm::Filestat& filestat, uint64_t& wayNodesOffset,
    std::vector<uint64_t>& hashPartOffsets) {
  while (true) {
    std::tuple<const char*, int32_t> tuple;

    {
      std::unique_lock<std::mutex> lock(mutexes.queue_mutex);

      mutexes.cv.wait(
          lock, [&] { return !mutexes.tuple_queue.empty() || mutexes.done; });

      if (mutexes.tuple_queue.empty() && mutexes.done) {
        return;
      }

      tuple = std::move(mutexes.tuple_queue.front());
      mutexes.tuple_queue.pop();
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

        if (ngosm::types::supportedHighwayTypes.find(highwayType) ==
            ngosm::types::supportedHighwayTypes.end()) {
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
          std::unique_lock<std::mutex> lock(mutexes.way_mutex);

          waysFile.add(ngosm::types::Way{way.id(), wayNodesOffset,
                                         way.refs_size(), oneway});

          wayNodesOffset += way.refs_size();

          google::protobuf::int64 id = 0;

          for (uint64_t index = 0; index < way.refs_size(); index++) {
            id += way.refs(index);

            uint64_t hash = ngosm::hashpart::partnum(id);

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

      for (auto& node : group.nodes()) {
        auto lat =
            ngosm::pb::convertCoord(primitiveBlock.lat_offset(),
                                    primitiveBlock.granularity(), node.lat());
        auto lon =
            ngosm::pb::convertCoord(primitiveBlock.lon_offset(),
                                    primitiveBlock.granularity(), node.lon());

        auto theta = (90 - lat) * M_PI / 180;
        auto phi = lon * M_PI / 180;

        long ipix;

        ang2pix_ring(ngosm::healpix::N_SIDE, theta, phi, &ipix);

        uint64_t hash = ngosm::hashpart::partnum(node.id());

        {
          std::unique_lock<std::mutex> lock(mutexes.node_mutexes[hash]);
          pixels.insert(ipix);
          nodeFiles[hash].add(ngosm::types::Node{node.id(), lat, lon});
        }
      }

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

        auto lat = ngosm::pb::convertCoord(
            primitiveBlock.lat_offset(), primitiveBlock.granularity(), latInt);
        auto lon = ngosm::pb::convertCoord(
            primitiveBlock.lon_offset(), primitiveBlock.granularity(), lonInt);

        auto theta = (90 - lat) * M_PI / 180;
        auto phi = lon * M_PI / 180;

        long ipix;

        ang2pix_ring(ngosm::healpix::N_SIDE, theta, phi, &ipix);

        uint64_t hash = ngosm::hashpart::partnum(id);

        {
          std::unique_lock<std::mutex> lock(mutexes.node_mutexes[hash]);
          pixels.insert(ipix);
          nodeFiles[hash].add(ngosm::types::Node{id, lat, lon});
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
          std::unique_lock<std::mutex> lock(mutexes.restriction_mutex);
          restrictionFile.add(
              ngosm::types::Restriction{rel.id(), from, via, to, type});
        }
      }
    }

    {
      std::unique_lock<std::mutex> lock(mutexes.filestat_mutex);
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

}  // namespace parse
}  // namespace mt
}  // namespace ngosm