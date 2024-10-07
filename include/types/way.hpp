#pragma once

#include <osmpbf/osmformat.pb.h>

namespace parser {

inline const std::unordered_set<std::string> supportedHighwayTypes{
    "motorway",      "primary",        "primary_link", "road",
    "secondary",     "secondary_link", "residential",  "tertiary",
    "tertiary_link", "unclassified",   "trunk",        "trunk_link",
    "motorway_link"};

// struct Way {
//   Way(google::protobuf::int64 id, bool oneway,
//       google::protobuf::internal::RepeatedIterator<const int64_t> begin,
//       google::protobuf::internal::RepeatedIterator<const int64_t> end)
//       : id(id), oneway(oneway) {
//     nodes = std::vector<google::protobuf::int64>(begin, end);
//   }

//   Way(google::protobuf::int64 id, bool oneway,
//       std::vector<google::protobuf::int64>::iterator begin,
//       std::vector<google::protobuf::int64>::iterator end)
//       : id(id), oneway(oneway) {
//     nodes = std::vector<google::protobuf::int64>(begin, end);
//   }

//   google::protobuf::int64 id;
//   std::vector<google::protobuf::int64> nodes;
//   bool oneway;
// };

struct Way {
  google::protobuf::int64 id;
  uint64_t offset;
  int size;
  bool oneway;
};

}  // namespace parser