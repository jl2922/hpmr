#ifndef DIST_MAP_H
#define DIST_MAP_H

#include <functional>
#include "parallel.h"
#include "reducer.h"

namespace hpmr {

template <class K, class V, class H = std::hash<K>>
class DistMap {
 public:
  DistMap();

  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void get(const K& key, const std::function<void(V&)>& handler);

  size_t get_n_keys();

  void sync();

  template <class KR, class VR, class HR>
  DistMap<KR, VR, HR> mapreduce(
      const std::function<
          void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>& mapper,
      const std::function<void(VR&, const VR&)>& reducer,
      const bool verbose = false);

 private:
  size_t n_keys;
};

template <class K, class V, class H>
DistMap<K, V, H>::DistMap() {
  n_keys = 0;
}

template <class K, class V, class H>
void DistMap<K, V, H>::set(const K&, const V&, const std::function<void(V&, const V&)>&) {
#pragma omp atomic
  n_keys++;
}

template <class K, class V, class H>
size_t DistMap<K, V, H>::get_n_keys() {
  return Parallel::reduce_sum(n_keys);
}

template <class K, class V, class H>
void DistMap<K, V, H>::sync() {
  printf("Shuffling ");
}

}  // namespace hpmr

#endif
