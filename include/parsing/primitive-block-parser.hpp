#pragma once

#include <osmpbf/osmformat.pb.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <tables/ska/flat_hash_map.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>

namespace parser {

namespace primitive_block {

void parse(OSMPBF::PrimitiveBlock& block, std::vector<parser::Node>& nodeBuffer,
           std::vector<parser::Way>& wayBuffer,
           std::unordered_multimap<google::protobuf::int64,
                                   parser::Restriction>& restrictionBuffer);

double convertCoord(google::protobuf::int64 offset,
                    google::protobuf::int32 granularity,
                    google::protobuf::int64 coord);

// struct PrimitiveBlockParser {
//  public:
//   PrimitiveBlockParser(OSMPBF::PrimitiveBlock& block);

//   double convertLat(google::protobuf::int64 coord);

//   double convertLon(google::protobuf::int64 coord);

//   void parse(parser::Graph& graph);

//  private:
//   OSMPBF::PrimitiveBlock& block;

//   std::vector<std::string> highwayTypes{
//       "motorway",      "primary",        "primary_link", "road",
//       "secondary",     "secondary_link", "residential",  "tertiary",
//       "tertiary_link", "unclassified",   "trunk",        "trunk_link",
//       "motorway_link"};

//   double convertCoord(google::protobuf::int64 offset,
//                       google::protobuf::int64 coord);
// };
}  // namespace primitive_block

}  // namespace parser