#pragma once

#include <osmpbf/osmformat.pb.h>

namespace ngosm {
namespace types {

inline const std::unordered_set<std::string> supportedHighwayTypes{
    "motorway",      "primary",        "primary_link", "road",
    "secondary",     "secondary_link", "residential",  "tertiary",
    "tertiary_link", "unclassified",   "trunk",        "trunk_link",
    "motorway_link"};

struct Way {
  google::protobuf::int64 id;
  uint64_t offset;
  int size;
  bool oneway;
};

}  // namespace types
}  // namespace ngosm