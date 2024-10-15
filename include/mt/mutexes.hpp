#pragma once

#include <atomic>
#include <hash-part/utils.hpp>
#include <mutex>
#include <queue>
#include <vector>

namespace ngosm {
namespace mt {

struct Mutexes {
  std::mutex queue_mutex;
  std::mutex restriction_mutex;
  std::mutex filestat_mutex;
  std::mutex way_mutex;
  std::vector<std::mutex> node_mutexes =
      std::vector<std::mutex>(ngosm::hashpart::HP_NUM);

  std::queue<std::tuple<const char*, int32_t>> tuple_queue;
  std::condition_variable cv;
  std::atomic<bool> done = std::atomic<bool>(false);
};

}  // namespace mt
}  // namespace ngosm