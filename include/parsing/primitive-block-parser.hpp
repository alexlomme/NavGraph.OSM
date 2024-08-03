#pragma once

#include <osmpbf/osmformat.pb.h>

#include <types/graph.hpp>

namespace parser {

struct PrimitiveBlockParser {
 public:
  PrimitiveBlockParser(OSMPBF::PrimitiveBlock& block);

  double convertLat(google::protobuf::int64 coord);

  double convertLon(google::protobuf::int64 coord);

  void parse(parser::Graph& graph);

 private:
  OSMPBF::PrimitiveBlock& block;

  std::vector<std::string> highwayTypes{
      "motorway",      "primary",        "primary_link", "road",
      "secondary",     "secondary_link", "residential",  "tertiary",
      "tertiary_link", "unclassified",   "trunk",        "trunk_link",
      "motorway_link"};

  double convertCoord(google::protobuf::int64 offset,
                      google::protobuf::int64 coord);
};

}  // namespace parser