#include "parser-types.hpp"

void convertToGraph(
    const std::vector<parser::Way>& ways,
    const std::unordered_map<google::protobuf::int64, parser::Node>& nodes,
    std::unordered_multimap<google::protobuf::int64, google::protobuf::int64>*
        graph,
    std::unordered_map<google::protobuf::int64, parser::Edge>* edges) {
    for (auto& way : ways) {
        auto& wayNodes = way.nodes;
        std::vector<std::pair<double, double>> locations;

        parser::Node source;
        parser::Node target;

        for (uint64_t i = 0; i < wayNodes.size(); i++) {
            auto nodeId = wayNodes[i];
            auto it = nodes.find(nodeId);

            if (it == nodes.end()) {
                throw std::runtime_error("Node is missing");
            }

            if (i == 0) {
                source = it->second;
            }

            if (i == wayNodes.size() - 1) {
                target = it->second;
            }

            auto node = it->second;

            locations.push_back(std::make_pair(node.lat, node.lon));
        }

        double cost = wayCost(locations);

        parser::Edge edge{way.id, source, target, cost};

        graph->insert({source.id, way.id});
        graph->insert({target.id, way.id});

        edges->insert({way.id, edge});
    }
}