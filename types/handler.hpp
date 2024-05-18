#include <iostream>
#include <osmium/handler.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/way.hpp>
#include <vector>

struct CustomHandler : osmium::handler::Handler {
 public:
  void way(const osmium::Way& way) { std::cerr << "aas" << std::endl; }
};