#pragma once

#include <omp.h>
#include <functional>
#include "bare_map.h"
#include "segment_hasher.h"

namespace hpmr {
// A concurrent map that requires providing hash values when use.
template <class K, class V, class H = std::hash<K>>
class BareConcurrentMap {
 public:
  BareConcurrentMap();

  BareConcurrentMap(const BareConcurrentMap& m);

  ~BareConcurrentMap();

  void reserve(const size_t n_keys_min);

  void set_max_load_factor(const float max_load_factor);

  float get_max_load_factor() { return max_load_factor; };

  size_t get_n_keys();

  size_t get_n_buckets();

  float get_load_factor();

  void set(
      const K& key,
      const size_t hash_value,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void async_set(
      const K& key,
      const size_t hash_value,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void sync(const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void unset(const K& key, const size_t hash_value);

  V get(const K& key, const size_t hash_value, const V& default_value = V());

  bool has(const K& key, const size_t hash_value);

  void clear();

  void clear_and_shrink();

  // Apply node_handler to the hash node which has the specific key.
  // If the key does not exist, apply to the unassociated node from the corresponding bucket.
  // void key_node_apply(
  //     const K& key,
  //     const size_t hash_value,
  //     const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler);

  // // Apply node_handler to all the hash nodes.
  // void all_node_apply(
  //     const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler);

  std::string to_string();

  void from_string(const std::string& str);

  void for_each(
      const std::function<void(const K& key, const size_t hash_value, const V& value)>& handler);

 private:
  float max_load_factor;

  size_t n_segments;

  std::vector<BareMap<K, V, SegmentHasher<K, H>>> segments;

  size_t n_threads;

  std::vector<BareMap<K, V, H>> thread_caches;

  std::vector<omp_lock_t> segment_locks;
};

template <class K, class V, class H>
BareConcurrentMap<K, V, H>::BareConcurrentMap() {
  max_load_factor = BareMap<K, V, H>::DEFAULT_MAX_LOAD_FACTOR;
  n_threads = omp_get_max_threads();
  thread_caches.resize(n_threads);
  n_segments = n_threads * SegmentHasher<K, H>::N_SEGMENTS_PER_THREAD;
  segments.resize(n_segments);
  segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
}

template <class K, class V, class H>
BareConcurrentMap<K, V, H>::BareConcurrentMap(const BareConcurrentMap& m) {
  max_load_factor = m.max_load_factor;
  n_threads = omp_get_max_threads();
  thread_caches.resize(n_threads);
  n_segments = m.n_segments;
  segments = m.segments;
  segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
}

template <class K, class V, class H>
BareConcurrentMap<K, V, H>::~BareConcurrentMap() {
  for (auto& lock : segment_locks) omp_destroy_lock(&lock);
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::reserve(const size_t n_keys_min) {
  const size_t n_segment_keys_min = n_keys_min / n_segments;
  for (size_t i = 0; i < n_segments; i++) segments.at(i).reserve(n_segment_keys_min);
  const size_t n_thread_keys_est = n_keys_min / 1000;
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).reserve(n_thread_keys_est);
};

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::set_max_load_factor(const float max_load_factor) {
  this->max_load_factor = max_load_factor;
  for (size_t i = 0; i < n_segments; i++) segments.at(i).max_load_factor = max_load_factor;
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).max_load_factor = max_load_factor;
}

template <class K, class V, class H>
size_t BareConcurrentMap<K, V, H>::get_n_keys() {
  size_t n_keys = 0;
  for (size_t i = 0; i < n_segments; i++) n_keys += segments.at(i).n_keys;
  return n_keys;
}

template <class K, class V, class H>
size_t BareConcurrentMap<K, V, H>::get_n_buckets() {
  size_t n_buckets = 0;
  for (size_t i = 0; i < n_segments; i++) n_buckets += segments.at(i).get_n_buckets();
  return n_buckets;
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::async_set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  const size_t segment_hash_value = hash_value / n_segments;
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  if (omp_test_lock(&lock)) {
    segments.at(segment_id).set(key, segment_hash_value, value, reducer);
    omp_unset_lock(&lock);
  } else {
    const int thread_id = omp_get_thread_num();
    thread_caches.at(thread_id).set(key, hash_value, value, reducer);
  }
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::sync(const std::function<void(V&, const V&)>& reducer) {
#pragma omp parallel
  {
    const int thread_id = omp_get_thread_num();
    const auto& handler = [&](const K& key, const size_t hash_value, const V& value) {
      const size_t segment_id = hash_value % n_segments;
      const size_t segment_hash_value = hash_value / n_segments;
      auto& lock = segment_locks[segment_id];
      omp_set_lock(&lock);
      segments.at(segment_id).set(key, segment_hash_value, value, reducer);
      omp_unset_lock(&lock);
    };
    thread_caches.at(thread_id).for_each(handler);
    thread_caches.at(thread_id).clear();
  }
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  const size_t segment_hash_value = hash_value / n_segments;
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  segments.at(segment_id).set(key, segment_hash_value, value, reducer);
  omp_unset_lock(&lock);
}

template <class K, class V, class H>
V BareConcurrentMap<K, V, H>::get(const K& key, const size_t hash_value, const V& default_value) {
  const size_t segment_hash_value = hash_value / n_segments;
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  V res = segments.at(segment_id).get(key, segment_hash_value, default_value);
  omp_unset_lock(&lock);
  return res;
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::unset(const K& key, const size_t hash_value) {
  const size_t segment_hash_value = hash_value / n_segments;
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  segments.at(segment_id).unset(key, segment_hash_value);
  omp_unset_lock(&lock);
}

template <class K, class V, class H>
bool BareConcurrentMap<K, V, H>::has(const K& key, const size_t hash_value) {
  const size_t segment_hash_value = hash_value / n_segments;
  const size_t segment_id = hash_value % n_segments;
  auto& lock = segment_locks[segment_id];
  omp_set_lock(&lock);
  bool res = segments.at(segment_id).has(key, segment_hash_value);
  omp_unset_lock(&lock);
  return res;
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::clear() {
  for (size_t i = 0; i < n_segments; i++) segments.at(i).clear();
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).clear();
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::clear_and_shrink() {
  for (size_t i = 0; i < n_segments; i++) segments.at(i).clear_and_shrink();
  for (size_t i = 0; i < n_threads; i++) thread_caches.at(i).clear_and_shrink();
}

template <class K, class V, class H>
std::string BareConcurrentMap<K, V, H>::to_string() {
  std::vector<std::string> ostrs(n_segments);
  size_t total_size = 0;
#pragma omp parallel for
  for (size_t i = 0; i < n_segments; i++) {
    auto& lock = segment_locks[i];
    omp_set_lock(&lock);
    ostrs[i] = hps::serialize_to_string(segments.at(i));
    omp_unset_lock(&lock);
#pragma omp atomic
    total_size += ostrs[i].size();
  }
  std::string str;
  str.reserve(total_size + n_segments * 8);
  hps::OutputBuffer<std::string> ob_str(str);
  hps::Serializer<float, std::string>::serialize(max_load_factor, ob_str);
  for (size_t i = 0; i < n_segments; i++) {
    hps::Serializer<std::string, std::string>::serialize(ostrs[i], ob_str);
  }
  ob_str.flush();
  return str;
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::from_string(const std::string& str) {
  std::vector<std::string> istrs(n_segments);
  hps::InputBuffer<std::string> ib_str(str);
  hps::Serializer<float, std::string>::parse(max_load_factor, ib_str);
  for (size_t i = 0; i < n_segments; i++) {
    hps::Serializer<std::string, std::string>::parse(istrs[i], ib_str);
  }
#pragma omp parallel for
  for (size_t i = 0; i < n_segments; i++) {
    auto& lock = segment_locks[i];
    omp_set_lock(&lock);
    hps::parse_from_string(segments.at(i), istrs[i]);
    omp_unset_lock(&lock);
  }
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::for_each(
    const std::function<void(const K& key, const size_t hash_value, const V& value)>& handler) {
#pragma omp paralell for
  for (size_t i = 0; i < n_segments; i++) {
    segments.at(i).for_each(handler);
  }
}

}  // namespace hpmr

// template <class K, class V, class H>
// void BareConcurrentMap<K, V, H>::key_node_apply(
//     const K& key,
//     const size_t hash_value,
//     const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler) {
//   bool applied = false;
//   while (!applied) {
//     const size_t n_buckets_snapshot = n_buckets;
//     const size_t bucket_id = hash_value % n_buckets_snapshot;
//     const size_t segment_id = bucket_id % n_segments;
//     auto& lock = segment_locks[segment_id];
//     omp_set_lock(&lock);
//     if (n_buckets_snapshot != n_buckets) {
//       omp_unset_lock(&lock);
//       continue;
//     }
//     key_node_apply_recursive(buckets[bucket_id], key, node_handler);
//     omp_unset_lock(&lock);
//     applied = true;
//   }
// }

// // template <class K, class V, class H>
// // void BareConcurrentMap<K, V, H>::all_node_apply(
// //     const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler) {
// //   lock_all_segments();
// //   const double progress_factor = 100.0 / n_buckets;
// // #pragma omp parallel for schedule(static, 1)
// //   for (size_t i = 0; i < n_buckets; i++) {
// //     all_node_apply_recursive(buckets[i], node_handler, i * progress_factor);
// //   }
// //   unlock_all_segments();
// // }

// template <class K, class V, class H>
// void BareConcurrentMap<K, V, H>::key_node_apply_recursive(
//     std::unique_ptr<HashNode<K, V>>& node,
//     const K& key,
//     const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler) {
//   if (node) {
//     if (node->key == key) {
//       node_handler(node);
//     } else {
//       key_node_apply_recursive(node->next, key, node_handler);
//     }
//   } else {
//     node_handler(node);
//   }
// }

// template <class K, class V, class H>
// void BareConcurrentMap<K, V, H>::all_node_apply_recursive(
//     std::unique_ptr<HashNode<K, V>>& node,
//     const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler,
//     const double progress) {
//   if (node) {
//     // Post-order traversal for rehash.
//     all_node_apply_recursive(node->next, node_handler, progress);
//     node_handler(node, progress);
//   }
// }

// template <class K, class V, class H>
// void BareConcurrentMap<K, V, H>::rehash() {
//   const size_t n_buckets_min = static_cast<size_t>(n_keys / max_load_factor);
//   reserve(n_buckets_min * 2);
// }

// template <class K, class V, class H>
// void BareConcurrentMap<K, V, H>::rehash(const size_t n_rehash_buckets) {
//   omp_set_lock(&rehash_entry_lock);
//   if (n_buckets >= n_rehash_buckets) {
//     omp_unset_lock(&rehash_entry_lock);
//     return;
//   }
//   lock_all_segments();
//   // if (n_buckets >= n_rehash_buckets) {
//   //   omp_unset_lock(&rehash_entry_lock);
//   //   unlock_all_segments();
//   //   return;
//   // }

//   // Rehash.
//   std::vector<std::unique_ptr<HashNode<K, V>>> rehash_buckets(n_rehash_buckets);
//   const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double) {
//     const auto& rehash_node_handler = [&](std::unique_ptr<HashNode<K, V>>& rehash_node) {
//       rehash_node = std::move(node);
//       rehash_node->next.reset();
//     };
//     const K& key = node->key;
//     const size_t hash_value = hasher(key);
//     const size_t rehash_bucket_id = hash_value % n_rehash_buckets;
//     const size_t rehash_segment_id = rehash_bucket_id % n_segments;
//     auto& rehash_lock = rehash_segment_locks[rehash_segment_id];
//     omp_set_lock(&rehash_lock);
//     key_node_apply_recursive(rehash_buckets[rehash_bucket_id], key, rehash_node_handler);
//     omp_unset_lock(&rehash_lock);
//   };
// #pragma omp parallel for
//   for (size_t i = 0; i < n_buckets; i++) {
//     all_node_apply_recursive(buckets[i], node_handler);
//   }

//   buckets = std::move(rehash_buckets);
//   n_buckets = n_rehash_buckets;

//   omp_unset_lock(&rehash_entry_lock);
//   unlock_all_segments();
// }

// template <class K, class V, class H>
// size_t BareConcurrentMap<K, V, H>::get_n_rehash_buckets(const size_t n_buckets_min) const {
//   // Returns a number that is greater than or roughly equals to n_buckets_min.
//   // That number is either a prime number or the product of several prime numbers.
//   constexpr size_t PRIMES[] = {
//       11, 17, 29, 47, 79, 127, 211, 337, 547, 887, 1433, 2311, 3739, 6053, 9791, 15859};
//   constexpr size_t N_PRIMES = sizeof(PRIMES) / sizeof(size_t);
//   constexpr size_t LAST_PRIME = PRIMES[N_PRIMES - 1];
//   constexpr size_t BIG_PRIME = PRIMES[N_PRIMES - 5];
//   size_t remaining_factor = n_buckets_min;
//   remaining_factor += n_buckets_min / 4;
//   size_t n_rehash_buckets = 1;
//   while (remaining_factor > LAST_PRIME) {
//     remaining_factor /= BIG_PRIME;
//     n_rehash_buckets *= BIG_PRIME;
//   }

//   // Find a prime larger than or equal to the remaining factor with binary search.
//   size_t left = 0, right = N_PRIMES - 1;
//   while (left < right) {
//     size_t mid = (left + right) / 2;
//     if (PRIMES[mid] < remaining_factor) {
//       left = mid + 1;
//     } else {
//       right = mid;
//     }
//   }
//   n_rehash_buckets *= PRIMES[left];
//   return n_rehash_buckets;
// }

// }  // namespace hpmr
