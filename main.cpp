#include <cstdint>
#include <osmium/io/pbf_input.hpp>
#include <osmium/visitor.hpp>

#include "osm-data/osmformat.pb.h"
#include "types/handler.hpp"

using namespace OSMPBF;

int main() {
  osmium::io::File input{"./osm-data/detmold.osm.pbf"};
  osmium::io::Reader reader{input};

  std::vector<osmium::Way> ways;

  CustomHandler handler;

  osmium::apply(reader, handler);

  reader.close();

  return 0;
}