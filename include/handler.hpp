#include <osmium/handler.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/node_ref.hpp>
#include <osmium/osm/types.hpp>
#include <osmium/osm/way.hpp>
#include <unordered_map>
#include <vector>

#include "geomath.hpp"
#include "types.hpp"

struct CustomHandler : osmium::handler::Handler {
   public:
    CustomHandler() : osmium::handler::Handler() {
        ways = new std::vector<osm_parser::Way>();
        nodes =
            new std::unordered_map<osmium::object_id_type, osm_parser::Node>();
    }

    void way(const osmium::Way& way) {
        std::vector<osmium::NodeRef> nodeRefs;

        nodeRefs.insert(nodeRefs.begin(), way.nodes().begin(),
                        way.nodes().end());

        ways->push_back(osm_parser::Way{nodeRefs, way.id()});
    }

    void node(const osmium::Node& node) {
        nodes->insert(std::make_pair(
            node.id(), osm_parser::Node{node.id(), node.location()}));
    }

    std::tuple<
        std::unordered_multimap<osmium::object_id_type, osmium::object_id_type>,
        std::unordered_map<osmium::object_id_type, osm_parser::Edge>>
    convertDataToGraph() {
        std::unordered_multimap<osmium::object_id_type, osmium::object_id_type>
            graph;
        std::unordered_map<osmium::object_id_type, osm_parser::Edge> edges;

        for (auto& way : *(ways)) {
            std::vector<osmium::NodeRef> wayNodes = way.nodes;
            std::vector<osmium::Location> locations;

            osm_parser::Node source;
            osm_parser::Node target;

            for (uint64_t i = 0; i < wayNodes.size(); i++) {
                osmium::NodeRef wayNode = wayNodes[i];
                auto it = nodes->find(wayNode.ref());

                if (it == nodes->end()) {
                    throw std::runtime_error("Node is missing");
                }

                if (i == 0) {
                    source = it->second;
                }

                if (i == wayNodes.size() - 1) {
                    target = it->second;
                }

                locations.push_back((it->second).location);
            }

            double cost = wayCost(locations);

            osm_parser::Edge edge{way.id, source, target, cost};

            graph.insert({source.id, way.id});
            graph.insert({target.id, way.id});

            edges.insert({way.id, edge});
        }

        ways->clear();
        nodes->clear();

        return {graph, edges};
    }

   private:
    std::vector<osm_parser::Way>* ways;
    std::unordered_map<osmium::object_id_type, osm_parser::Node>* nodes;
};