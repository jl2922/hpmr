#pragma once

#include <functional>
#include "concurrent_map.h"
#include "parallel.h"
#include "reducer.h"

namespace hpmr {

template <class K, class V, class H = std::hash<K>>
class DistMap {
 public:
  DistMap();

  void reserve(const size_t n_buckets_min);

  size_t get_n_buckets();

  double get_load_factor();

  double get_max_load_factor() { return max_load_factor; }

  void set_max_load_factor(const double max_load_factor);

  size_t get_n_keys();

  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  V get(const K& key, const V& default_value = V());

  void clear();

  void clear_and_shrink();

  template <class KR, class VR, class HR = std::hash<KR>>
  DistMap<KR, VR, HR> mapreduce(
      const std::function<
          void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>& mapper,
      const std::function<void(VR&, const VR&)>& reducer,
      const bool verbose = false);

  void sync(const bool verbose = false);

 private:
  double max_load_factor;

  size_t n_procs_cache;

  size_t proc_id_cache;

  H hasher;

  ConcurrentMap<K, V, H> local_map;

  std::vector<ConcurrentMap<K, V, H>> remote_maps;

  constexpr static double DEFAULT_MAX_LOAD_FACTOR = 1.0;
};

template <class K, class V, class H>
DistMap<K, V, H>::DistMap() {
  set_max_load_factor(DEFAULT_MAX_LOAD_FACTOR);
  n_procs_cache = static_cast<size_t>(Parallel::get_n_procs());
  proc_id_cache = static_cast<size_t>(Parallel::get_proc_id());
  remote_maps.resize(n_procs_cache);
}

template <class K, class V, class H>
void DistMap<K, V, H>::reserve(const size_t n_buckets_min) {
  local_map.reserve(n_buckets_min / n_procs_cache);
  for (auto& remote_map : remote_maps) {
    remote_map.reserve(n_buckets_min / n_procs_cache / n_procs_cache);
  }
}

template <class K, class V, class H>
size_t DistMap<K, V, H>::get_n_buckets() {
  const size_t local_n_buckets = local_map.get_n_buckets();
  return Parallel::reduce_sum(local_n_buckets);
}

template <class K, class V, class H>
double DistMap<K, V, H>::get_load_factor() {
  return static_cast<double>(get_n_buckets() / get_n_keys());
}

template <class K, class V, class H>
void DistMap<K, V, H>::set_max_load_factor(const double max_load_factor) {
  this->max_load_factor = max_load_factor;
  local_map.set_max_load_factor(max_load_factor);
  for (auto& remote_map : remote_maps) remote_map.set_max_load_factor(max_load_factor);
}

template <class K, class V, class H>
size_t DistMap<K, V, H>::get_n_keys() {
  const size_t local_n_keys = local_map.get_n_keys();
  return Parallel::reduce_sum(local_n_keys);
}

template <class K, class V, class H>
void DistMap<K, V, H>::set(
    const K& key, const V& value, const std::function<void(V&, const V&)>& reducer) {
  const size_t hash_value = hasher(key);
  const size_t target_proc_id = hash_value % n_procs_cache;
  const size_t map_hash_value = hash_value / n_procs_cache;
  if (target_proc_id == proc_id_cache) {
    local_map.set_with_hash(key, map_hash_value, value, reducer);
  } else {
    remote_maps[target_proc_id].set_with_hash(key, map_hash_value, value, reducer);
  }
}

template <class K, class V, class H>
V DistMap<K, V, H>::get(const K& key, const V& default_value) {
  
}

template <class K, class V, class H>
void DistMap<K, V, H>::clear() {
  local_map.clear();
  for (auto& remote_map : remote_maps) remote_map.clear();
}

template <class K, class V, class H>
void DistMap<K, V, H>::clear_and_shrink() {
  local_map.clear_and_shrink();
  for (auto& remote_map : remote_maps) remote_map.clear_and_shrink();
}

template <class K, class V, class H>
template <class KR, class VR, class HR>
DistMap<KR, VR, HR> DistMap<K, V, H>::mapreduce(
    const std::function<void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>&
        mapper,
    const std::function<void(VR&, const VR&)>& reducer,
    const bool verbose) {}

template <class K, class V, class H>
void DistMap<K, V, H>::sync(const bool verbose) {
  const int proc_id = Parallel::get_proc_id();
  if (verbose && proc_id == 0) printf("Syncing: ");
}

}  // namespace hpmr
