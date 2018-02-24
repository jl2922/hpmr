#pragma once

#include <omp.h>
#include <functional>

namespace hpmr {
template <class K, class H>
class AsyncHasher {
 public:
  AsyncHasher() { n_threads_u = omp_get_max_threads(); }

  size_t operator()(const K& key) const { return hasher(key) / 4; }

 private:
  H hasher;

  size_t n_threads_u;
};
}  // namespace hpmr
