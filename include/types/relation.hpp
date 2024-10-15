#pragma once

#include <osmpbf/osmformat.pb.h>

namespace ngosm {
namespace types {
struct Relation {
 public:
  Relation(google::protobuf::int64 id, google::protobuf::int64 from,
           google::protobuf::int64 via, google::protobuf::int64 to)
      : id(id), from(from), via(via), to(to) {}

  google::protobuf::int64 id;
  google::protobuf::int64 from;
  google::protobuf::int64 via;
  google::protobuf::int64 to;
};

struct Restriction : ngosm::types::Relation {
  Restriction(google::protobuf::int64 id, google::protobuf::int64 from,
              google::protobuf::int64 via, google::protobuf::int64 to,
              int8_t type)
      : ngosm::types::Relation(id, from, via, to), type(type) {}

  int8_t type;
};
}  // namespace types
}  // namespace ngosm