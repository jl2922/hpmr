#pragma once

#include "bare_concurrent_map.h"
#include "dist_map.h"

namespace hpmr {

template <class K, class V, class H = std::hash<K>>
class ConcurrentMap {
 public:
  
 private:
  BareConcurrentMap<K, V, H> bare_map;
};

}  // namespace hpmr
