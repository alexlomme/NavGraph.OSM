#pragma once

#include <osmpbf/osmformat.pb.h>

struct DiskWay {
  google::protobuf::int64 id;
  uint64_t offset;
  int size;
  bool oneway;
};