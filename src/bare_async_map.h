#pragma once

#include <omp.h>
#include <functional>
#include "async_hasher.h"
#include "bare_map.h"

namespace hpmr {
template <class K, class V, class H = std::hash<K>>
class BareAsyncMap {
 public:
  BareAsyncMap();

  void reserve(const size_t n_buckets_min);

  size_t get_n_keys();

  size_t get_n_buckets();

  double get_load_factor();

  double get_max_load_factor() const { return max_load_factor; }

  void set_max_load_factor(const double max_load_factor);

  void async_set(
      const K& key,
      const size_t hash_value,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void sync(const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  V get(const K& key, const size_t hash_value, const V& default_value = V());

  void clear();

  void clear_and_shrink();

 private:
  std::vector<std::vector<BareMap<K, V, AsyncHasher<K, H>>>> cache_maps;

  std::vector<BareMap<K, V, AsyncHasher<K, H>>> maps;

  size_t n_threads_u;

  double max_load_factor;

  constexpr static double DEFAULT_MAX_LOAD_FACTOR = 1.0;
};

template <class K, class V, class H>
BareAsyncMap<K, V, H>::BareAsyncMap() {
  n_threads_u = omp_get_max_threads();
  maps.resize(n_threads_u);
  cache_maps.resize(n_threads_u);
  for (size_t i = 0; i < n_threads_u; i++) {
    cache_maps[i].resize(n_threads_u);
  }
  set_max_load_factor(DEFAULT_MAX_LOAD_FACTOR);
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::reserve(const size_t n_buckets_min) {
  for (size_t i = 0; i < n_threads_u; i++) {
    maps[i].reserve(n_buckets_min / n_threads_u);
    for (size_t j = 0; j < n_threads_u; j++) {
      cache_maps[i][j].reserve(n_buckets_min / n_threads_u / n_threads_u);
    }
  }
}

template <class K, class V, class H>
size_t BareAsyncMap<K, V, H>::get_n_keys() {
  size_t n_keys = 0;
  for (size_t i = 0; i < n_threads_u; i++) n_keys += maps[i].get_n_keys();
  return n_keys;
}

template <class K, class V, class H>
size_t BareAsyncMap<K, V, H>::get_n_buckets() {
  size_t n_buckets = 0;
  for (size_t i = 0; i < n_threads_u; i++) n_buckets += maps[i].get_n_buckets();
  return n_buckets;
}

template <class K, class V, class H>
double BareAsyncMap<K, V, H>::get_load_factor() {
  return static_cast<double>(get_n_keys()) / get_n_buckets();
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::set_max_load_factor(const double max_load_factor) {
  this->max_load_factor = max_load_factor;
  for (size_t i = 0; i < n_threads_u; i++) {
    maps[i].set_max_load_factor(max_load_factor);
    for (size_t j = 0; j < n_threads_u; j++) {
      cache_maps[i][j].set_max_load_factor(max_load_factor);
    }
  }
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::async_set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  const int thread_id = omp_get_thread_num();
  const size_t dest_thread_id = hash_value % n_threads_u;
  const size_t async_hash_value = hash_value / n_threads_u;
  cache_maps[thread_id][dest_thread_id].set(key, async_hash_value, value, reducer);
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::sync(const std::function<void(V&, const V&)>& reducer) {
  AsyncHasher<K, H> async_hasher;
#pragma omp parallel
  {
    const int thread_id = omp_get_thread_num();
    const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double) {
      // maps[thread_id].set(node->key, 1, node->value, reducer);
      maps[thread_id].set(node->key, node->hash_value, node->value, reducer);
    };
    size_t thread_n_keys = 0;
    for (size_t i = 0; i < n_threads_u; i++) {
      thread_n_keys += cache_maps[i][thread_id].get_n_keys();
    }
    maps[thread_id].reserve(thread_n_keys / max_load_factor);
    for (size_t i = 0; i < n_threads_u; i++) {
      cache_maps[i][thread_id].all_node_apply(node_handler);
      cache_maps[i][thread_id].clear();
    }
  }
}

template <class K, class V, class H>
V BareAsyncMap<K, V, H>::get(const K& key, const size_t hash_value, const V& default_value) {
  const size_t dest_thread_id = hash_value % n_threads_u;
  const size_t async_hash_value = hash_value / n_threads_u;
  return maps[dest_thread_id].get(key, async_hash_value, default_value);
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::clear() {
  for (size_t i = 0; i < n_threads_u; i++) {
    maps[i].clear();
    for (size_t j = 0; j < n_threads_u; j++) {
      cache_maps[i][j].clear();
    }
  }
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::clear_and_shrink() {
  for (size_t i = 0; i < n_threads_u; i++) {
    maps[i].clear_and_shrink();
    for (size_t j = 0; j < n_threads_u; j++) {
      cache_maps[i][j].clear_and_shrink();
    }
  }
}

}  // namespace hpmr