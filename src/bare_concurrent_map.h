#pragma once

#include <omp.h>
#include <functional>
#include <memory>
#include <vector>
#include "../hps/src/hps.h"
#include "hash_node.h"
#include "hash_pending_node.h"
#include "parallel.h"
#include "reducer.h"

namespace hpmr {

// A concurrent map that requires providing hash values when use.
template <class K, class V, class H = std::hash<K>>
class BareConcurrentMap {
 public:
  BareConcurrentMap();

  BareConcurrentMap(const BareConcurrentMap& m);

  ~BareConcurrentMap();

  void reserve(const size_t n_buckets_min);

  size_t get_n_buckets() const { return n_buckets; };

  double get_load_factor() const { return static_cast<double>(n_keys) / n_buckets; }

  double get_max_load_factor() const { return max_load_factor; }

  void set_max_load_factor(const double max_load_factor) {
    this->max_load_factor = max_load_factor;
  }

  size_t get_n_keys() const { return n_keys; }

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

  void get(const K& key, const size_t hash_value, const std::function<void(const V&)>& handler);

  V get(const K& key, const size_t hash_value, const V& default_value = V());

  bool has(const K& key, const size_t hash_value);

  void clear();

  void clear_and_shrink();

  size_t get_hash_value(const K& key);

  // Apply node_handler to the hash node which has the specific key.
  // If the key does not exist, apply to the unassociated node from the corresponding bucket.
  // The hash value shall be consistent with the specified key.
  void key_node_apply(
      const K& key,
      const size_t hash_value,
      const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler);

  // Apply node_handler to all the hash nodes.
  void all_node_apply(
      const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler);

  std::string to_string();

  void from_string(const std::string& str);

 private:
  size_t n_keys;

  size_t n_buckets;

  double max_load_factor;

  size_t n_segments;

  H hasher;

  std::vector<omp_lock_t> segment_locks;

  // For parallel rehashing (Require omp_set_nested(1)).
  std::vector<omp_lock_t> rehashing_segment_locks;

  std::vector<std::unique_ptr<HashNode<K, V>>> buckets;

  std::vector<std::unique_ptr<HashPendingNode<K, V>>> pending_nodes;

  constexpr static size_t N_INITIAL_BUCKETS = 11;

  constexpr static size_t N_SEGMENTS_PER_THREAD = 7;

  constexpr static double DEFAULT_MAX_LOAD_FACTOR = 1.0;

  void rehash();

  void rehash(const size_t n_rehashing_buckets);

  // Get the number of hash buckets to use.
  // This number shall be larger than or equal to the specified number.
  size_t get_n_rehashing_buckets(const size_t n_buckets_min) const;

  void key_node_apply_recursive(
      std::unique_ptr<HashNode<K, V>>& node,
      const K& key,
      const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler);

  void pending_node_apply_recursive(
      std::unique_ptr<HashPendingNode<K, V>>& node,
      const K& key,
      const std::function<void(std::unique_ptr<HashPendingNode<K, V>>&)>& node_handler);

  // Recursively apply the handler to each node on the list from the node specified (post-order).
  void all_node_apply_recursive(
      std::unique_ptr<HashNode<K, V>>& node,
      const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler,
      const double progress = 0.0);

  void lock_all_segments();

  void unlock_all_segments();
};

template <class K, class V, class H>
BareConcurrentMap<K, V, H>::BareConcurrentMap() {
  n_keys = 0;
  n_buckets = N_INITIAL_BUCKETS;
  buckets.resize(n_buckets);
  set_max_load_factor(DEFAULT_MAX_LOAD_FACTOR);
  n_segments = Parallel::get_n_threads() * N_SEGMENTS_PER_THREAD;
  segment_locks.resize(n_segments);
  rehashing_segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
  for (auto& lock : rehashing_segment_locks) omp_init_lock(&lock);
  omp_set_nested(1);  // For parallel rehashing.
}

template <class K, class V, class H>
BareConcurrentMap<K, V, H>::BareConcurrentMap(const BareConcurrentMap<K, V, H>& m) {
  n_keys = m.n_keys;
  n_buckets = m.n_buckets;
  buckets.resize(n_buckets);
  max_load_factor = m.max_load_factor;
  n_segments = m.n_segments;
#pragma omp parallel for
  for (size_t i = 0; i < n_buckets; i++) {
    HashNode<K, V>* ptr_src = m.buckets[i].get();
    HashNode<K, V>* ptr_dest = buckets[i].get();
    while (ptr_src != nullptr) {
      ptr_dest = new HashNode<K, V>(ptr_src->key, ptr_src->value);
      ptr_src = ptr_src->next.get();
      ptr_dest = ptr_dest->next.get();
    }
  }

  segment_locks.resize(n_segments);
  rehashing_segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
  for (auto& lock : rehashing_segment_locks) omp_init_lock(&lock);
}

template <class K, class V, class H>
BareConcurrentMap<K, V, H>::~BareConcurrentMap() {
  clear();
  for (auto& lock : segment_locks) omp_destroy_lock(&lock);
  for (auto& lock : rehashing_segment_locks) omp_destroy_lock(&lock);
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::reserve(const size_t n_buckets_min) {
  if (n_buckets >= n_buckets_min) return;
  const size_t n_rehashing_buckets = get_n_rehashing_buckets(n_buckets_min);
  rehash(n_rehashing_buckets);
};

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node) {
    if (!node) {
      node.reset(new HashNode<K, V>(key, value));
#pragma omp atomic
      n_keys++;
    } else {
      reducer(node->value, value);
    }
  };
  key_node_apply(key, hash_value, node_handler);
  if (n_keys >= n_buckets * max_load_factor) rehash();
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::async_set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
//   const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node) {
//     if (!node) {
//       node.reset(new HashNode<K, V>(key, value));
// #pragma omp atomic
//       n_keys++;
//     } else {
//       reducer(node->value, value);
//     }
//   };

//   const size_t n_buckets_snapshot = n_buckets;
//   const size_t bucket_id = hash_value % n_buckets_snapshot;
//   const size_t segment_id = bucket_id % n_segments;
//   auto& lock = segment_locks[segment_id];
//   bool applied = false;
//   if (omp_test_lock(&lock)) {
//     if (n_buckets_snapshot != n_buckets) {
//       omp_unset_lock(&lock);
//     }
//     key_node_apply_recursive(buckets[bucket_id], key, node_handler);
//     omp_unset_lock(&lock);
//     applied = true;
//     if (n_keys >= n_buckets * max_load_factor) rehash();
//   }

//   const auto& pending_node_handler = [&](std::unique_ptr<HashPendingNode<K, V>>& node) {
//     if (!node) {
//       if (!applied) {
//         node.reset(new HashPendingNode<K, V>(key, value));
//       }
//     } else {
//       if (node->key == key) {
//         reducer(node->value, value);
//         applied = true;
//       }
//       if (node->tries > MAX_TRIES) return;
//       node->backoff--;
//       if (node->backoff == 0) {
//         if (async_key_node_apply(key, hash_value, value, reducer)) {
//           node = std::move(node->next);
//         } else {
//           node->tries++;
//           node->backoff = get_backoff(node->tries);
//         }
//       }
//     }
//   };

//   const int thread_id = Parallel::get_thread_id();
//   pending_node_apply_recursive(pending_nodes[thread_id], pending_node_handler);
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::unset(const K& key, const size_t hash_value) {
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node) {
    if (node) {
      node = std::move(node->next);
#pragma omp atomic
      n_keys--;
    }
  };
  key_node_apply(key, hash_value, node_handler);
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::get(
    const K& key, const size_t hash_value, const std::function<void(const V&)>& handler) {
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node) {
    if (node) handler(node->value);
  };
  key_node_apply(key, hash_value, node_handler);
}

template <class K, class V, class H>
V BareConcurrentMap<K, V, H>::get(const K& key, const size_t hash_value, const V& default_value) {
  V value(default_value);
  const auto& node_handler = [&](const std::unique_ptr<HashNode<K, V>>& node) {
    if (node) value = node->value;
  };
  key_node_apply(key, hash_value, node_handler);
  return value;
}

template <class K, class V, class H>
bool BareConcurrentMap<K, V, H>::has(const K& key, const size_t hash_value) {
  bool has_key = false;
  const auto& node_handler = [&](const std::unique_ptr<HashNode<K, V>>& node) {
    if (node) has_key = true;
  };
  key_node_apply(key, hash_value, node_handler);
  return has_key;
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::clear() {
  lock_all_segments();
#pragma omp parallel for
  for (size_t i = 0; i < n_buckets; i++) {
    buckets[i].reset();
  }
  n_keys = 0;
  unlock_all_segments();
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::clear_and_shrink() {
  lock_all_segments();
#pragma omp parallel for
  for (size_t i = 0; i < n_buckets; i++) {
    buckets[i].reset();
  }
  n_keys = 0;
  n_buckets = N_INITIAL_BUCKETS;
  buckets.resize(n_buckets);
  unlock_all_segments();
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::key_node_apply(
    const K& key,
    const size_t hash_value,
    const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler) {
  bool applied = false;
  while (!applied) {
    const size_t n_buckets_snapshot = n_buckets;
    const size_t bucket_id = hash_value % n_buckets_snapshot;
    const size_t segment_id = bucket_id % n_segments;
    auto& lock = segment_locks[segment_id];
    omp_set_lock(&lock);
    if (n_buckets_snapshot != n_buckets) {
      omp_unset_lock(&lock);
      continue;
    }
    key_node_apply_recursive(buckets[bucket_id], key, node_handler);
    omp_unset_lock(&lock);
    applied = true;
  }
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::all_node_apply(
    const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler) {
  lock_all_segments();
  const double progress_factor = 100.0 / n_buckets;
#pragma omp parallel for schedule(static, 1)
  for (size_t i = 0; i < n_buckets; i++) {
    all_node_apply_recursive(buckets[i], node_handler, i * progress_factor);
  }
  unlock_all_segments();
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::key_node_apply_recursive(
    std::unique_ptr<HashNode<K, V>>& node,
    const K& key,
    const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler) {
  if (node) {
    if (node->key == key) {
      node_handler(node);
    } else {
      key_node_apply_recursive(node->next, key, node_handler);
    }
  } else {
    node_handler(node);
  }
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::all_node_apply_recursive(
    std::unique_ptr<HashNode<K, V>>& node,
    const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler,
    const double progress) {
  if (node) {
    // Post-order traversal for rehashing.
    all_node_apply_recursive(node->next, node_handler, progress);
    node_handler(node, progress);
  }
}

template <class K, class V, class H>
std::string BareConcurrentMap<K, V, H>::to_string() {
  // Parallel serialization.
  const int n_threads = Parallel::get_n_threads();
  std::vector<std::string> ostrs(n_threads);
  std::vector<hps::OutputBuffer<std::string>> obs;
  std::vector<size_t> counts(n_threads, 0);
  obs.reserve(n_threads);
  for (int i = 0; i < n_threads; i++) obs.push_back(hps::OutputBuffer<std::string>(ostrs[i]));
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double) {
    const int thread_id = Parallel::get_thread_id();
    hps::Serializer<K, std::string>::serialize(node->key, obs[thread_id]);
    hps::Serializer<V, std::string>::serialize(node->value, obs[thread_id]);
    counts[thread_id]++;
  };
  all_node_apply(node_handler);

  // Combine results from each thread.
  size_t total_size = 0;
  for (int i = 0; i < n_threads; i++) {
    obs[i].flush();
    total_size += ostrs[i].size();
  }
  std::string str;
  str.reserve(total_size + n_threads * 8);
  hps::OutputBuffer<std::string> ob_str(str);
  for (int i = 0; i < n_threads; i++) {
    hps::Serializer<size_t, std::string>::serialize(counts[i], ob_str);
    hps::Serializer<std::string, std::string>::serialize(ostrs[i], ob_str);
  }
  ob_str.flush();
  return str;
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::from_string(const std::string& str) {
  // Load strings.
  const int n_threads = Parallel::get_n_threads();
  std::vector<std::string> istrs(n_threads);
  std::vector<size_t> counts(n_threads);
  hps::InputBuffer<std::string> ib_str(str);
  size_t total_count = 0;
  for (int i = 0; i < n_threads; i++) {
    hps::Serializer<size_t, std::string>::parse(counts[i], ib_str);
    hps::Serializer<std::string, std::string>::parse(istrs[i], ib_str);
    total_count += counts[i];
  }
  clear();
  reserve(total_count);

  // Parallel parsing and insert.
#pragma omp parallel
  {
    const int thread_id = Parallel::get_thread_id();
    K key;
    V value;
    hps::InputBuffer<std::string> thread_ib(istrs[thread_id]);
    for (size_t j = 0; j < counts[thread_id]; j++) {
      hps::Serializer<K, std::string>::parse(key, thread_ib);
      hps::Serializer<V, std::string>::parse(value, thread_ib);
      set(key, get_hash_value(key), value);
    }
  }
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::lock_all_segments() {
  for (auto& lock : segment_locks) omp_set_lock(&lock);
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::unlock_all_segments() {
  for (auto& lock : segment_locks) omp_unset_lock(&lock);
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::rehash() {
  const size_t n_buckets_min = static_cast<size_t>(n_keys / max_load_factor);
  reserve(n_buckets_min);
}

template <class K, class V, class H>
void BareConcurrentMap<K, V, H>::rehash(const size_t n_rehashing_buckets) {
  auto& first_lock = segment_locks[0];
  omp_set_lock(&first_lock);
  if (n_buckets >= n_rehashing_buckets) {
    omp_unset_lock(&first_lock);
    return;
  }
  omp_unset_lock(&first_lock);
  lock_all_segments();
  if (n_buckets >= n_rehashing_buckets) {
    unlock_all_segments();
    return;
  }

  // Rehash.
  std::vector<std::unique_ptr<HashNode<K, V>>> rehashing_buckets(n_rehashing_buckets);
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double) {
    const auto& rehashing_node_handler = [&](std::unique_ptr<HashNode<K, V>>& rehashing_node) {
      rehashing_node = std::move(node);
      rehashing_node->next.reset();
    };
    const K& key = node->key;
    const size_t hash_value = get_hash_value(key);
    const size_t bucket_id = hash_value % n_rehashing_buckets;
    const size_t segment_id = bucket_id % n_segments;
    auto& lock = rehashing_segment_locks[segment_id];
    omp_set_lock(&lock);
    key_node_apply_recursive(rehashing_buckets[bucket_id], key, rehashing_node_handler);
    omp_unset_lock(&lock);
  };
#pragma omp parallel for
  for (size_t i = 0; i < n_buckets; i++) {
    all_node_apply_recursive(buckets[i], node_handler);
  }

  buckets = std::move(rehashing_buckets);
  n_buckets = n_rehashing_buckets;
  unlock_all_segments();
}

template <class K, class V, class H>
size_t BareConcurrentMap<K, V, H>::get_n_rehashing_buckets(const size_t n_buckets_min) const {
  // Returns a number that is greater than or equal to n_buckets_min.
  // That number is either a prime number or the product of several prime numbers.
  constexpr size_t PRIMES[] = {
      11, 17, 29, 47, 79, 127, 211, 337, 547, 887, 1433, 2311, 3739, 6053, 9791, 15859};
  constexpr size_t N_PRIMES = sizeof(PRIMES) / sizeof(size_t);
  constexpr size_t LAST_PRIME = PRIMES[N_PRIMES - 1];
  size_t remaining_factor = n_buckets_min;
  size_t n_rehashing_buckets = 1;
  while (remaining_factor > LAST_PRIME) {
    remaining_factor /= LAST_PRIME;
    n_rehashing_buckets *= LAST_PRIME;
  }

  // Find a prime larger than or equal to the remaining factor with binary search.
  size_t left = 0, right = N_PRIMES - 1;
  while (left < right) {
    size_t mid = (left + right) / 2;
    if (PRIMES[mid] < remaining_factor) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  n_rehashing_buckets *= PRIMES[left];
  return n_rehashing_buckets;
}

template <class K, class V, class H>
size_t BareConcurrentMap<K, V, H>::get_hash_value(const K& key) {
  return hasher(key);
}

}  // namespace hpmr
