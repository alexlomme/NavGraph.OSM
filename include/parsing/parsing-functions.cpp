#include <chealpix.h>
#include <osmpbf/fileformat.pb.h>

#include <iostream>
#include <parsing/parsing-functions.hpp>
#include <types/way.hpp>
#include <utils/libdeflate_decomp.hpp>

#include "primitive-block-parser.hpp"

void parser::parsing::parseUsedNodes(
    const char* data, const char* endOfFile,
    std::unordered_map<google::protobuf::int64, parser::UsedNode>& usedNodes) {
  while (data < endOfFile) {
    uint32_t headerSize;
    std::memcpy(&headerSize, data, sizeof(headerSize));
    headerSize = ntohl(headerSize);

    data += sizeof(headerSize);

    OSMPBF::BlobHeader header;
    if (!header.ParseFromArray(data, headerSize)) {
      throw std::runtime_error("Failed to parse BlobHeader");
    }

    data += headerSize;

    if (header.type() != "OSMData") {
      data += header.datasize();
      continue;
    }

    OSMPBF::Blob blob;
    if (!blob.ParseFromArray(data, header.datasize())) {
      throw std::runtime_error("Failed to parse Blob");
    }
    data += header.datasize();

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
      throw std::runtime_error("Unable to parse primitive block");
    }

    const auto& stringtable = primitiveBlock.stringtable();

    for (const auto& group : primitiveBlock.primitivegroup()) {
      for (const auto& way : group.ways()) {
        auto& keys = way.keys();
        auto& values = way.vals();

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

        google::protobuf::int64 prevValue = 0;

        for (uint64_t index = 0; index < way.refs_size(); index++) {
          const auto delta = way.refs(index);
          prevValue += delta;

          const auto it = usedNodes.find(prevValue);

          const auto lambda = [](uint64_t i, int size) {
            return i == 0 || i == size - 1 ? 2l : 1l;
          };

          if (it == usedNodes.end()) {
            usedNodes.emplace(
                std::piecewise_construct, std::forward_as_tuple(prevValue),
                std::forward_as_tuple(lambda(index, way.refs_size()), false));
          } else {
            it->second.num += lambda(index, way.refs_size());
          }
        }
      }
    }
    delete[] uncompressedData;
  }
}

void parser::parsing::parseNodes(
    const char* data, const char* endOfFile,
    std::unordered_map<uint32_t, std::vector<parser::Node>>& nodePartitions,
    std::unordered_map<google::protobuf::int64, parser::UsedNode>& usedNodes,
    std::unordered_set<uint32_t>& pixels) {
  while (data < endOfFile) {
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
      throw std::runtime_error("Unsupported compression format");
    }

    OSMPBF::PrimitiveBlock primitiveBlock;
    if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
      //   std::cerr << "Unable to parse primitive block" << std::endl;
      //   return -1;
      throw std::runtime_error("Unable to parse primitive block");
    }

    const auto& stringtable = primitiveBlock.stringtable();
    const auto lat_o = primitiveBlock.lat_offset();
    const auto lon_o = primitiveBlock.lon_offset();
    const auto gran = primitiveBlock.granularity();

    for (const auto& group : primitiveBlock.primitivegroup()) {
      const auto& dense = group.dense();

      if (dense.id_size() != dense.lon_size() ||
          dense.id_size() != dense.lat_size()) {
        throw std::runtime_error("Lacking information for dense nodes\n");
      }

      int64_t prevId = 0;
      int64_t prevLon = 0;
      int64_t prevLat = 0;

      for (int64_t i = 0; i < dense.id_size(); i++) {
        auto deltaId = dense.id(i);
        auto deltaLat = dense.lat(i);
        auto deltaLon = dense.lon(i);

        prevId += deltaId;
        prevLat += deltaLat;
        prevLon += deltaLon;

        const auto usedNodePairIt = usedNodes.find(prevId);
        if (usedNodePairIt == usedNodes.end()) {
          continue;
        }

        auto lat = parser::primitive_block::convertCoord(lat_o, gran, prevLat);
        auto lon = parser::primitive_block::convertCoord(lon_o, gran, prevLon);

        auto theta = (90 - lat) * M_PI / 180;
        auto phi = lon * M_PI / 180;

        long ipix;

        ang2pix_ring(50, theta, phi, &ipix);

        pixels.insert(ipix);

        const auto nodeBufIt = nodePartitions.find(ipix);

        if (nodeBufIt == nodePartitions.end()) {
          const auto it =
              nodePartitions
                  .emplace(std::piecewise_construct,
                           std::forward_as_tuple(ipix), std::forward_as_tuple())
                  .first;

          it->second.emplace_back(prevId, lat, lon, usedNodePairIt->second.num);
        } else {
          nodeBufIt->second.emplace_back(prevId, lat, lon,
                                         usedNodePairIt->second.num);
        }

        usedNodePairIt->second.convert(ipix);
      }
    }

    delete[] uncompressedData;
  }
}

void parser::parsing::thirdPhaseParse(
    const char* data, const char* endOfFile,
    std::unordered_map<uint32_t, std::vector<parser::Way>>& wayPartitions,
    std::vector<parser::BorderWay>& borderWays,
    std::vector<parser::Restriction>& restrictions,
    const std::unordered_map<google::protobuf::int64, parser::UsedNode>&
        usedNodes) {
  while (data < endOfFile) {
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
      throw std::runtime_error("Unsupported compression format");
    }

    OSMPBF::PrimitiveBlock primitiveBlock;
    if (!primitiveBlock.ParseFromArray(uncompressedData, blob.raw_size())) {
      throw std::runtime_error("Unable to parse primitive block");
    }

    const auto& stringtable = primitiveBlock.stringtable();

    for (const auto& group : primitiveBlock.primitivegroup()) {
      for (const auto& way : group.ways()) {
        const auto& keys = way.keys();
        const auto& values = way.vals();

        const auto highwayIt = std::find_if(
            keys.begin(), keys.end(),
            [&](uint32_t key) { return stringtable.s(key) == "highway"; });
        if (highwayIt == keys.end()) {
          continue;
        }

        std::string highwayType =
            stringtable.s(values[std::distance(keys.begin(), highwayIt)]);

        if (parser::supportedHighwayTypes.find(highwayType) ==
            supportedHighwayTypes.end()) {
          continue;
        }

        const auto owIt = std::find_if(
            keys.begin(), keys.end(),
            [&](uint32_t key) { return stringtable.s(key) == "oneway"; });

        bool oneway = false;
        if (owIt != keys.end()) {
          uint64_t index = std::distance(keys.begin(), owIt);
          auto onewayVal = stringtable.s(values[index]);

          if (onewayVal == "yes" || onewayVal == "1") {
            oneway = true;
          }
        }

        if (way.refs_size() <= 1) {
          continue;
        }

        const auto refIt = usedNodes.find(way.refs(0));

        google::protobuf::int64 prevValue = way.refs(0);

        if (refIt == usedNodes.end() || !refIt->second.converted) {
          throw std::runtime_error("Missing Node!");
        }

        const auto refPart = refIt->second.num;

        bool partitionable = true;
        for (uint64_t i = 1; i < way.refs_size(); i++) {
          prevValue += way.refs(i);
          const auto it = usedNodes.find(prevValue);
          if (it == usedNodes.end() || !it->second.converted) {
            throw std::runtime_error("Missing Node (2)");
          }

          if (it->second.num != refPart) {
            partitionable = false;
            break;
          }
        }

        if (!partitionable) {
          auto& bw = borderWays.emplace_back(way.id(), oneway);
          bw.nodes.reserve(way.refs_size());

          int64_t prevId = 0;

          for (uint64_t i = 0; i < way.refs_size(); i++) {
            auto nodeId = way.refs(i);
            prevId += nodeId;

            const auto uit = usedNodes.find(prevId);
            if (uit == usedNodes.end() || !uit->second.converted) {
              throw std::runtime_error("Missing Node (3)");
            }

            if (bw.nodes.size() == 0 ||
                bw.nodes[bw.nodes.size() - 1].first != uit->second.num) {
              auto& [_, vec] =
                  bw.nodes.emplace_back(std::piecewise_construct,
                                        std::forward_as_tuple(uit->second.num),
                                        std::forward_as_tuple());
              vec.push_back(prevId);
              continue;
            }

            bw.nodes[bw.nodes.size() - 1].second.push_back(prevId);
          }

          continue;
        }

        const auto partIt = wayPartitions.find(refPart);

        if (partIt == wayPartitions.end()) {
          const auto& [tup, _] = wayPartitions.emplace(
              std::piecewise_construct, std::forward_as_tuple(refPart),
              std::forward_as_tuple());
          tup->second.emplace_back(way.id(), oneway, way.refs().begin(),
                                   way.refs().end());
          continue;
        }

        partIt->second.emplace_back(way.id(), oneway, way.refs().begin(),
                                    way.refs().end());
      }

      for (const auto& rel : group.relations()) {
        auto& keys = rel.keys();
        auto& values = rel.vals();

        auto restIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
          return stringtable.s(key) == "restriction";
        });

        if (restIt == keys.end()) {
          continue;
        }

        auto restrictionType =
            stringtable.s(values[std::distance(keys.begin(), restIt)]);

        auto& ids = rel.memids();
        auto& roles = rel.roles_sid();
        auto& types = rel.types();

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

        restrictions.emplace_back(rel.id(), from, via, to, restrictionType);
      }
    }

    delete[] uncompressedData;
  }
}