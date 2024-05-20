namespace osm_parser {
struct Node {
    osmium::object_id_type id;
    osmium::Location location;
};

struct Edge {
    osmium::object_id_type id;
    osm_parser::Node sourceNode;
    osm_parser::Node targetNode;
    double cost;
};

struct Way {
    std::vector<osmium::NodeRef> nodes;
    osmium::object_id_type id;
};
}  // namespace osm_parser