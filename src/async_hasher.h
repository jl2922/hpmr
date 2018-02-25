#pragma once

#include <omp.h>
#include <functional>

namespace hpmr {
template <class K, class H>
class AsyncHasher {
 public:
  AsyncHasher() { n_segments = omp_get_max_threads() * N_SEGMENTS_PER_THREAD; }

  size_t operator()(const K& key) const { return hasher(key) / n_segments; }

  constexpr static size_t N_SEGMENTS_PER_THREAD = 32;

 private:
  H hasher;

  size_t n_segments;
};
}  // namespace hpmr
