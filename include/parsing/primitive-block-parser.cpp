
#include "primitive-block-parser.hpp"

double parser::primitive_block::convertCoord(int64_t offset,
                                             int32_t granularity,
                                             int64_t coord) {
  return (offset + granularity * coord) / pow(10, 9);
}