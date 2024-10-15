#pragma once

#include <unistd.h>

#include <utils/hashing.hpp>

namespace ngosm {
namespace hashpart {

inline const uint64_t HP_NUM = 64;
inline const uint64_t HASH_MASK = 0x3F;

inline const uint64_t MAX_RN_BUF_SIZE = 600'000;
inline const uint64_t MAX_NODEDATA_BUF_SIZE = 1'000'000;

uint64_t partnum(int64_t val) { return MurmurHash64A_1(val) & HASH_MASK; }

}  // namespace hashpart
}  // namespace ngosm