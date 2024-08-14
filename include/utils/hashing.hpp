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