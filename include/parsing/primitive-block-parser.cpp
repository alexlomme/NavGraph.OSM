
#include "primitive-block-parser.hpp"

namespace parser {

parser::PrimitiveBlockParser::PrimitiveBlockParser(
    OSMPBF::PrimitiveBlock& block)
    : block(block) {}

double parser::PrimitiveBlockParser::convertLat(google::protobuf::int64 coord) {
  return convertCoord(block.lat_offset(), coord);
}

double parser::PrimitiveBlockParser::convertLon(google::protobuf::int64 coord) {
  return convertCoord(block.lon_offset(), coord);
}

void parser::PrimitiveBlockParser::parse(parser::Graph& graph) {
  auto stringTable = block.stringtable();

  for (auto& group : block.primitivegroup()) {
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

      if (std::find(highwayTypes.begin(), highwayTypes.end(), highwayType) ==
          highwayTypes.end()) {
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

      graph.add_way(parser::Way{parsedWay.id(), nodeRefs, oneway});
    }

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

      graph.add_restriction(
          parser::Restriction{from, via, to, restrictionType});
    }

    for (auto& parsedNode : group.nodes()) {
      auto lat = convertLat(parsedNode.lat());
      auto lon = convertLon(parsedNode.lon());

      graph.add_node(parser::Node{parsedNode.id(), lat, lon, 0});
    }

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

      auto lat = convertLat(deltaLat + prevLat);
      auto lon = convertLon(deltaLon + prevLon);

      graph.add_node(parser::Node{id, lat, lon, 0});

      prevId += deltaId;
      prevLat += deltaLat;
      prevLon += deltaLon;
    }
  }
}

double parser::PrimitiveBlockParser::convertCoord(
    google::protobuf::int64 offset, google::protobuf::int64 coord) {
  return (offset + block.granularity() * coord) / pow(10, 9);
}

}  // namespace parser