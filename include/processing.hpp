#include <osmpbf/osmformat.pb.h>

#include <tables/ska/flat_hash_map.hpp>
#include <types/node.hpp>
#include <types/relation.hpp>
#include <types/way.hpp>
#include <utils/hashing.hpp>
#include <vector>

namespace ngosm {
namespace processing {

template <typename T>
using buffer = std::vector<T>;

using node_buffer_t = buffer<ngosm::types::Node>;
using way_buffer_t = buffer<ngosm::types::Way>;
using restriction_buffer_t = buffer<ngosm::types::Restriction>;

template <typename K, typename V>
using hashtable = std::unordered_map<K, V>;

using no_restriction_map_t =
    hashtable<std::tuple<google::protobuf::int64, google::protobuf::int64>,
              ngosm::types::Restriction*>;
using node_map = hashtable<google::protobuf::int64, ngosm::types::Node*>;

template <typename K, typename V>
using ska_hashtable = ska::flat_hash_map<K, V>;

using node_count_t = ska_hashtable<google::protobuf::int64, uint64_t>;

template <typename K, typename V>
using multimap = std::unordered_multimap<K, V>;

using only_restrictions_by_to =
    multimap<google::protobuf::int64, ngosm::types::Restriction*>;
using only_restriction_mm =
    multimap<std::tuple<google::protobuf::int64, google::protobuf::int64>,
             ngosm::types::Restriction*>;

void hash_restrictions(restriction_buffer_t& buffer,
                       only_restrictions_by_to& onlyRestrictionsByTo,
                       no_restriction_map_t& noRestrictionsMap);

}  // namespace processing
}  // namespace ngosm