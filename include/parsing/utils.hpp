#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/way.hpp>

namespace parser {

bool filterWay(OSMPBF::Way way, OSMPBF::StringTable stringtable) {
  const auto& keys = way.keys();
  const auto& values = way.vals();

  auto highwayIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
    return stringtable.s(key) == "highway";
  });
  if (highwayIt == keys.end()) {
    return false;
  }

  std::string highwayType =
      stringtable.s(values[std::distance(keys.begin(), highwayIt)]);

  if (parser::supportedHighwayTypes.find(highwayType) ==
      parser::supportedHighwayTypes.end()) {
    return false;
  }

  return true;
}

bool isOneway(OSMPBF::Way way, OSMPBF::StringTable stringtable) {
  const auto& keys = way.keys();
  const auto& values = way.vals();
  auto owIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
    return stringtable.s(key) == "oneway";
  });

  if (owIt != keys.end()) {
    uint64_t index = std::distance(keys.begin(), owIt);
    auto onewayVal = stringtable.s(values[index]);

    if (onewayVal == "yes" || onewayVal == "1") {
      return true;
    }
  }
  return false;
}

bool filterRelation(OSMPBF::Relation rel, OSMPBF::StringTable stringtable,
                    std::vector<google::protobuf::int64>& members,
                    std::vector<char>& type) {
  const auto& keys = rel.keys();
  const auto& values = rel.vals();

  auto restIt = std::find_if(keys.begin(), keys.end(), [&](uint32_t key) {
    return stringtable.s(key) == "restriction";
  });

  if (restIt == keys.end()) {
    return false;
  }

  const auto restrictionType =
      stringtable.s(values[std::distance(keys.begin(), restIt)]);

  const auto& ids = rel.memids();
  const auto& roles = rel.roles_sid();
  const auto& types = rel.types();

  if (rel.memids_size() != 3) {
    return false;
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
    } else if (role == "to" &&
               type == OSMPBF::Relation::MemberType::Relation_MemberType_WAY) {
      to = id;
    } else if (role == "via" &&
               type == OSMPBF::Relation::MemberType::Relation_MemberType_NODE) {
      via = id;
    }

    prevId = id;
  }

  if (from == -1 || to == -1 || via == -1) {
    return false;
  }

  if (restrictionType != "no_right_turn" && restrictionType != "no_left_turn" &&
      restrictionType != "no_straight_on" &&
      restrictionType != "only_right_turn" &&
      restrictionType != "only_left_turn" &&
      restrictionType != "only_straight_on") {
    return false;
  }

  members.push_back(from);
  members.push_back(via);
  members.push_back(to);

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

  type.push_back(t);
  type.push_back(w);
}

}  // namespace parser