#include <functional>
#include "dist_map.h"

class DistRange {
 public:
  DistRange(const long long start, const long long end, const long long step = 1)
      : start(start), end(end), step(step) {}

  template <class K, class V, class H>
  DistMap<K, V, H> map(
      const std::function<void(const long long, const std::function<void(const K&, const V&)>&)>&
          mapper) {
    DistMap<K, V, H> res;

    const auto& emit = [&](const K& key, const V& value) {

    };

    for (long long i = start; i < end; i += step) {
      mapper(i, emit);
    }

    return res;
  }

 private:
  long long start;
  long long end;
  long long step;
};