#include "processing.hpp"

namespace ngosm {
namespace processing {

void hash_restrictions(restriction_buffer_t& buffer,
                       only_restrictions_by_to& onlyRestrictionsByTo,
                       no_restriction_map_t& noRestrictionsMap) {
  for (auto& restriction : buffer) {
    auto& type = restriction.type;
    if (type < 3) {
      noRestrictionsMap.insert(std::make_pair(
          std::make_tuple(restriction.from, restriction.to), &restriction));
      continue;
    }
    onlyRestrictionsByTo.insert(std::make_pair(restriction.to, &restriction));
  }
}

}  // namespace processing
}  // namespace ngosm