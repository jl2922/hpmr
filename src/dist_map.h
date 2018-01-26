#ifndef DIST_MAP_H
#define DIST_MAP_H

#include <functional>
#include "parallel.h"
#include "reducer.h"

static const size_t N_INITIAL_BUCKETS = 5;
static const size_t N_SEGMENTS_PER_THREAD = 5;

namespace hpmr {

template <class K, class V, class H>
class DistMap {
 public:
  DistMap();

  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void get(const K& key, const std::function<void(V&)>& handler);

  V get_copy_or_default(const K& key, const V& default_value);

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

  size_t n_buckets;

  double max_load_factor;

  size_t n_segments;

  H hasher;

  std::vector<omp_lock_t> segment_locks;

  struct hash_node {
    K key;
    V value;
    std::unique_ptr<hash_node> next;
    hash_node(const K& key, const V& value) : key(key), value(value){};
  };

  std::vector<std::unique_ptr<hash_node>> buckets;
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
void DistMap<K, V, H>::sync() {}

}  // namespace hpmr

#endif
