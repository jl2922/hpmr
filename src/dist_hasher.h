#pragma once

#include <functional>
#include "parallel.h"

namespace hpmr {
template <class K, class H>
class DistHasher {
 public:
  DistHasher() { n_procs_cache = static_cast<size_t>(Parallel::get_n_procs()); }

  size_t operator()(const K& key) const { return hasher(key) / n_procs_cache; }

 private:
  H hasher;

  size_t n_procs_cache;
};
}  // namespace hpmr
