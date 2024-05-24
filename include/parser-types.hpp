#include "geomath.hpp"
#include "osmpbf/osmformat.pb.h"

namespace parser {

struct Node {
    google::protobuf::int64 id;
    double lat;
    double lon;
};

struct Edge {
    google::protobuf::int64 id;
    parser::Node sourceNode;
    parser::Node targetNode;
    double cost;
};

struct Way {
    google::protobuf::int64 id;
    std::vector<google::protobuf::int64> nodes;
};

struct PrimitiveBlockParser {
   public:
    PrimitiveBlockParser(OSMPBF::PrimitiveBlock& block) : block(block) {}

    double convertLat(google::protobuf::int64 coord) {
        return convertCoord(block.lat_offset(), coord);
    }

    double convertLon(google::protobuf::int64 coord) {
        return convertCoord(block.lon_offset(), coord);
    }

    void parse(
        std::vector<parser::Way>& ways,
        std::unordered_map<google::protobuf::int64, parser::Node>& nodes) {
        for (auto& group : block.primitivegroup()) {
            for (auto& parsedWay : group.ways()) {
                std::vector<google::protobuf::int64> nodeRefs;

                google::protobuf::int64 prevValue = 0;

                for (auto& nodeRef : parsedWay.refs()) {
                    nodeRefs.push_back(nodeRef + prevValue);
                    prevValue += nodeRef;
                }

                ways.push_back(parser::Way{parsedWay.id(), nodeRefs});
            }

            for (auto& parsedNode : group.nodes()) {
                std::cerr << "nodes" << std::endl;
                auto lat = convertLat(parsedNode.lat());
                auto lon = convertLon(parsedNode.lon());

                nodes.insert(std::make_pair(
                    parsedNode.id(), parser::Node{parsedNode.id(), lat, lon}));
            }

            auto& denseNodes = group.dense();

            if (denseNodes.id_size() != denseNodes.lon_size() ||
                denseNodes.id_size() != denseNodes.lat_size()) {
                throw std::runtime_error(
                    "Lacking information for dense nodes\n");
            }

            int64_t prevId = 0;
            int64_t prevLon = 0;
            int64_t prevLat = 0;

            for (uint64_t i = 0; i < denseNodes.id_size(); i++) {
                auto deltaId = denseNodes.id(i);
                auto deltaLat = denseNodes.lat(i);
                auto deltaLon = denseNodes.lon(i);

                int64_t id = deltaId + prevId;

                auto lat = convertLat(deltaLat + prevLat);
                auto lon = convertLon(deltaLon + prevLon);

                nodes.insert(std::make_pair(prevId + deltaId,
                                            parser::Node{id, lat, lon}));

                prevId += deltaId;
                prevLat += deltaLat;
                prevLon += deltaLon;
            }
        }
    }

   private:
    OSMPBF::PrimitiveBlock& block;

    double convertCoord(google::protobuf::int64 offset,
                        google::protobuf::int64 coord) {
        return (offset + block.granularity() * coord) / pow(10, 9);
    }
};

}  // namespace parser