#pragma once

#include <stdint.h>

namespace parser {

struct UsedNode {
  uint32_t num;
  bool converted;

  UsedNode(uint32_t num, bool converted) : num(num), converted(converted) {}

  void convert(uint32_t partition) {
    num = partition;
    converted = true;
  }
};
}  // namespace parser