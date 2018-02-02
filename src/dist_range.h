#pragma once

#include <functional>
#include "dist_map.h"

namespace hpmr {
template <class T>
class DistRange {
 public:
  DistRange(const T start, const T end, const T step = 1) : start(start), end(end), step(step) {}

  template <class K, class V, class H = std::hash<K>>
  DistMap<K, V, H> mapreduce(
      const std::function<void(const T, const std::function<void(const K&, const V&)>&)>& mapper,
      const std::function<void(V&, const V&)>& reducer,
      const bool verbose = false);

 private:
  T start;
  T end;
  T step;
};

#include "dist_range.inl"

}  // namespace hpmr
