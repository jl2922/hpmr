#pragma once

#include "parallel.h"

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
DistMap<K, V, H>::~DistMap() {}

template <class K, class V, class H>
void DistMap<K, V, H>::set_max_load_factor(const double max_load_factor) {
  this->max_load_factor = max_load_factor;
  local_map.set_max_load_factor(max_load_factor);
  for (auto& remote_map : remote_maps) remote_map.set_max_load_factor(max_load_factor);
}

template <class K, class V, class H>
void DistMap<K, V, H>::set(
    const K& key, const V& value, const std::function<void(V&, const V&)>& reducer) {
  const size_t hash_value = hasher(key);
  const size_t target_proc_id = hash_value % n_procs_cache;
  if (target_proc_id == proc_id_cache) {
    local_map.set(key, value, reducer);
  } else {
    remote_maps[target_proc_id].set(key, value, reducer);
  }
}

template <class K, class V, class H>
size_t DistMap<K, V, H>::get_n_keys() {
  const size_t local_n_keys = local_map.get_n_keys();
  return Parallel::reduce_sum(local_n_keys);
}

template <class K, class V, class H>
void DistMap<K, V, H>::sync(const bool verbose) {
  const int proc_id = Parallel::get_proc_id();
  if (verbose && proc_id == 0) printf("Syncing: ");
}
