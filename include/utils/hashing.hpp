#pragma once

#include <functional>
#include <tuple>

namespace std {

template <typename... Args>
struct hash<tuple<Args...>> {
  inline size_t operator()(const tuple<Args...> &args) const {
    return fold_tuple(args, 0ul,
                      [](const size_t acc, const auto &val) -> uint64_t {
                        hash<typename decay<decltype(val)>::type> hasher;
                        return hasher(val) ^ acc;
                      });
  }

 private:
  template <typename T, typename F, unsigned I = 0, typename... Tuple>
  constexpr inline static T fold_tuple(const tuple<Tuple...> &tuple,
                                       T acc_or_init, const F &fn) {
    if constexpr (I == sizeof...(Args)) {
      return acc_or_init;
    } else {
      return fold_tuple<T, F, I + 1, Tuple...>(
          tuple, fn(acc_or_init, get<I>(tuple)), fn);
    }
  }
};
}  // namespace std

inline uint64_t MurmurHash64A_1(uint64_t k) {
  // MurmurHash64A
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
  k *= m;
  k ^= k >> r;
  k *= m;
  h ^= k;
  h *= m;
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}