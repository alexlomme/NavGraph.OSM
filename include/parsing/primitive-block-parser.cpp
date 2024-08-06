
#include "primitive-block-parser.hpp"

#include <chrono>

std::unordered_set<std::string> highwayTypes{
    "motorway",      "primary",        "primary_link", "road",
    "secondary",     "secondary_link", "residential",  "tertiary",
    "tertiary_link", "unclassified",   "trunk",        "trunk_link",
    "motorway_link"};

void parser::primitive_block::parse(
    OSMPBF::PrimitiveBlock& block, std::vector<parser::Node>& nodeBuffer,
    std::vector<parser::Way>& wayBuffer,
    std::unordered_multimap<google::protobuf::int64, parser::Restriction>&
        restrictionBuffer) {
  auto stringTable = block.stringtable();

  for (auto& group : block.primitivegroup()) {
    auto start = std::chrono::high_resolution_clock::now();
    for (auto& parsedWay : group.ways()) {
      auto& keys = parsedWay.keys();
      auto& values = parsedWay.vals();

      auto highwayIt = std::find_if(
          keys.begin(), keys.end(),
          [&](uint32_t key) { return stringTable.s(key) == "highway"; });
      if (highwayIt == keys.end()) {
        continue;
      }

      std::string highwayType =
          stringTable.s(values[std::distance(keys.begin(), highwayIt)]);

      if (highwayTypes.find(highwayType) == highwayTypes.end()) {
        continue;
      }

      auto owIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
        return stringTable.s(key) == "oneway";
      });

      bool oneway = false;
      if (owIt != keys.end()) {
        uint64_t index = std::distance(keys.begin(), owIt);
        auto onewayVal = stringTable.s(values[index]);

        if (onewayVal == "yes" || onewayVal == "1") {
          oneway = true;
        }
      } else {
        // std::cerr << "oneway not found" << std::endl;
      }

      std::vector<google::protobuf::int64> nodeRefs;

      google::protobuf::int64 prevValue = 0;

      for (auto& nodeRef : parsedWay.refs()) {
        nodeRefs.push_back(nodeRef + prevValue);
        prevValue += nodeRef;
      }

      wayBuffer.push_back(parser::Way{parsedWay.id(), nodeRefs, oneway});
    }

    std::cerr << "Parsed ways in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::high_resolution_clock::now() - start)
                     .count()
              << "ms" << std::endl;

    start = std::chrono::high_resolution_clock::now();

    for (auto& parsedRelation : group.relations()) {
      auto& keys = parsedRelation.keys();
      auto& values = parsedRelation.vals();

      std::vector<uint64_t> range(keys.size());
      std::iota(range.begin(), range.end(), 0);

      auto it = std::find_if(range.begin(), range.end(), [&](uint64_t index) {
        return stringTable.s(keys[index]) == "type";
      });
      if (it == range.end()) {
        // std::cerr << "Relation doesn't have a \"type\" key" << std::endl;
      }

      // if (stringTable.s(values[*it]) != "restriction") {
      //   continue;
      // }

      auto restIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
        return stringTable.s(key) == "restriction";
      });

      if (restIt == keys.end()) {
        // std::cerr << "Restriction type no specified" << std::endl;
        continue;
      }

      auto restrictionType =
          stringTable.s(values[std::distance(keys.begin(), restIt)]);

      auto& ids = parsedRelation.memids();
      auto& roles = parsedRelation.roles_sid();
      auto& types = parsedRelation.types();

      if (parsedRelation.memids_size() != 3) {
        // std::cerr << "Too much members" << std::endl;
        continue;
      }

      google::protobuf::int64 from = -1;
      google::protobuf::int64 to = -1;
      google::protobuf::int64 via = -1;

      google::protobuf::int64 prevId = 0;

      for (uint64_t i = 0; i < parsedRelation.memids_size(); i++) {
        auto id = ids[i] + prevId;
        auto role = stringTable.s(roles[i]);
        auto type = types[i];

        if (role == "from" &&
            type == OSMPBF::Relation::MemberType::Relation_MemberType_WAY) {
          from = id;
        } else if (role == "to" &&
                   type ==
                       OSMPBF::Relation::MemberType::Relation_MemberType_WAY) {
          to = id;
        } else if (role == "via" &&
                   type ==
                       OSMPBF::Relation::MemberType::Relation_MemberType_NODE) {
          via = id;
        } else {
          // std::cerr << role << " " << type << restrictionType << std::endl;
        }

        prevId = id;
      }

      if (from == -1 || to == -1 || via == -1) {
        // std::cerr << "Invalid restriction" << std::endl;
        continue;
      }

      restrictionBuffer.insert(std::make_pair(
          via, parser::Restriction{from, via, to, restrictionType}));
    }

    std::cerr << "Parsed restrictions in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::high_resolution_clock::now() - start)
                     .count()
              << "ms" << std::endl;

    start = std::chrono::high_resolution_clock::now();

    for (auto& parsedNode : group.nodes()) {
      auto lat = convertCoord(block.lat_offset(), block.granularity(),
                              parsedNode.lat());
      auto lon = convertCoord(block.lon_offset(), block.granularity(),
                              parsedNode.lon());

      nodeBuffer.push_back(parser::Node{parsedNode.id(), lat, lon, 0});
    }

    std::cerr << "Parsed nodes in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::high_resolution_clock::now() - start)
                     .count()
              << "ms" << std::endl;

    start = std::chrono::high_resolution_clock::now();

    auto& denseNodes = group.dense();

    if (denseNodes.id_size() != denseNodes.lon_size() ||
        denseNodes.id_size() != denseNodes.lat_size()) {
      throw std::runtime_error("Lacking information for dense nodes\n");
    }

    int64_t prevId = 0;
    int64_t prevLon = 0;
    int64_t prevLat = 0;

    for (int64_t i = 0; i < denseNodes.id_size(); i++) {
      auto deltaId = denseNodes.id(i);
      auto deltaLat = denseNodes.lat(i);
      auto deltaLon = denseNodes.lon(i);

      int64_t id = deltaId + prevId;
      int64_t latCoord = deltaLat + prevLat;
      int64_t lonCoord = deltaLon + prevLon;

      auto lat =
          convertCoord(block.lat_offset(), block.granularity(), latCoord);
      auto lon =
          convertCoord(block.lon_offset(), block.granularity(), lonCoord);

      nodeBuffer.push_back(Node{id, lat, lon, 0});

      prevId = id;
      prevLat = latCoord;
      prevLon = lonCoord;
    }

    std::cerr << "Parsed dense nodes in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::high_resolution_clock::now() - start)
                     .count()
              << "ms" << std::endl;
  }
}

double parser::primitive_block::convertCoord(int64_t offset,
                                             int32_t granularity,
                                             int64_t coord) {
  return (offset + granularity * coord) / pow(10, 9);
}