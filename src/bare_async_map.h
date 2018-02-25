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

  ~BareAsyncMap();

  void reserve(const size_t n_buckets_min);

  size_t get_n_keys();

  size_t get_n_buckets();

  float get_load_factor();

  float get_max_load_factor() const { return max_load_factor; }

  void set_max_load_factor(const float max_load_factor);

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
  std::vector<BareMap<K, V, AsyncHasher<K, H>>> thread_maps;

  std::vector<BareMap<K, V, AsyncHasher<K, H>>> segment_maps;

  std::vector<omp_lock_t> locks;

  size_t n_threads;

  size_t n_segments;

  float max_load_factor;

  constexpr static float DEFAULT_MAX_LOAD_FACTOR = 1.0;
};

template <class K, class V, class H>
BareAsyncMap<K, V, H>::BareAsyncMap() {
  n_threads = omp_get_max_threads();
  thread_maps.resize(n_threads);
  n_segments = n_threads * AsyncHasher<K, V>::N_SEGMENTS_PER_THREAD;
  segment_maps.resize(n_segments);
  locks.resize(n_segments);
  for (auto& lock : locks) omp_init_lock(&lock);
  set_max_load_factor(DEFAULT_MAX_LOAD_FACTOR);
}

template <class K, class V, class H>
BareAsyncMap<K, V, H>::~BareAsyncMap() {
  clear();
  for (auto& lock : locks) omp_destroy_lock(&lock);
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::reserve(const size_t n_buckets_min) {
  for (size_t i = 0; i < n_threads; i++) {
    thread_maps[i].reserve(n_buckets_min / n_threads);
  }
  for (size_t i = 0; i < n_segments; i++) {
    segment_maps[i].reserve(n_buckets_min / n_segments);
  }
}

template <class K, class V, class H>
size_t BareAsyncMap<K, V, H>::get_n_keys() {
  size_t n_keys = 0;
  for (size_t i = 0; i < n_segments; i++) n_keys += segment_maps[i].get_n_keys();
  return n_keys;
}

template <class K, class V, class H>
size_t BareAsyncMap<K, V, H>::get_n_buckets() {
  size_t n_buckets = 0;
  for (size_t i = 0; i < n_segments; i++) n_buckets += segment_maps[i].get_n_buckets();
  return n_buckets;
}

template <class K, class V, class H>
float BareAsyncMap<K, V, H>::get_load_factor() {
  return static_cast<float>(get_n_keys()) / get_n_buckets();
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::set_max_load_factor(const float max_load_factor) {
  this->max_load_factor = max_load_factor;
  for (size_t i = 0; i < n_threads; i++) {
    thread_maps[i].set_max_load_factor(max_load_factor);
  }
  for (size_t i = 0; i < n_segments; i++) {
    segment_maps[i].set_max_load_factor(max_load_factor);
  }
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::async_set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  const int thread_id = omp_get_thread_num();
  const size_t async_hash_value = hash_value / n_segments;
  const size_t segment_id = hash_value % n_segments;
  auto& lock = locks[segment_id];
  if (omp_test_lock(&lock)) {
    segment_maps[segment_id].set(key, async_hash_value, value, reducer);
    omp_unset_lock(&lock);
  } else {
    thread_maps[thread_id].set(key, async_hash_value, value, reducer);
  }
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::sync(const std::function<void(V&, const V&)>& reducer) {
#pragma omp parallel
  {
    const int thread_id = omp_get_thread_num();
    const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const float) {
      const size_t hash_value = node->hash_value;
      const size_t segment_id = hash_value % n_segments;
      auto& lock = locks[segment_id];
      const auto& key = node->key;
      const auto& value = node->value;
      node->next.reset();
      omp_set_lock(&lock);
      segment_maps[segment_id].set(key, hash_value, value, reducer);
      omp_unset_lock(&lock);
    };
    printf("t map %d : %zu\n", thread_id, thread_maps[thread_id].get_n_keys());
    thread_maps[thread_id].all_node_apply(node_handler);
    thread_maps[thread_id].clear();
  }
}

// template <class K, class V, class H>
// V BareAsyncMap<K, V, H>::get(const K& key, const size_t hash_value, const V& default_value) {
//   const size_t dest_thread_id = hash_value % n_threads;
//   const size_t async_hash_value = hash_value / n_threads;
//   return maps[dest_thread_id].get(key, async_hash_value, default_value);
// }

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::clear() {
  for (size_t i = 0; i < n_threads; i++) {
    thread_maps[i].clear();
  }
#pragma omp parallel for
  for (size_t i = 0; i < n_segments; i++) {
    segment_maps[i].clear();
  }
}

template <class K, class V, class H>
void BareAsyncMap<K, V, H>::clear_and_shrink() {
  for (size_t i = 0; i < n_threads; i++) {
    thread_maps[i].clear_and_shrink();
  }
#pragma omp parallel for
  for (size_t i = 0; i < n_segments; i++) {
    segment_maps[i].clear_and_shrink();
  }
}

}  // namespace hpmr