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

  ~DistMap();

  void reserve(const size_t n_buckets_min);

  size_t get_n_buckets();

  double get_load_factor();

  double get_max_load_factor() { return max_load_factor; }

  void set_max_load_factor(const double max_load_factor) {
    this->max_load_factor = max_load_factor;
  }

  size_t get_n_keys();

  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void get(const K& key, const std::function<void(const V&)>& handler);

  V get(const K& key, const V& default_value = V());

  void unset(const K& key);

  bool has(const K& key);

  void clear();

  template <class KR, class VR, class HR = std::hash<KR>>
  DistMap<KR, VR, HR> mapreduce(
      const std::function<
          void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>& mapper,
      const std::function<void(VR&, const VR&)>& reducer,
      const bool verbose = false);

  void sync();

 private:
  size_t n_keys;

  double max_load_factor;
};

template <class K, class V, class H>
DistMap<K, V, H>::DistMap() {
  n_keys = 0;
}

template <class K, class V, class H>
DistMap<K, V, H>::~DistMap() {}

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
