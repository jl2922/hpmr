#ifndef HPMR_CONCURRENT_MAP_H
#define HPMR_CONCURRENT_MAP_H

#include <omp.h>
#include <functional>
#include <memory>
#include <vector>
#include "parallel.h"
#include "reducer.h"

constexpr static size_t N_INITIAL_BUCKETS = 11;
constexpr static size_t N_SEGMENTS_PER_THREAD = 5;
constexpr static double DEFAULT_MAX_LOAD_FACTOR = 1.0;

namespace hpmr {

template <class K, class V, class H = std::hash<K>>
class ConcurrentMap {
 public:
  ConcurrentMap();

  ~ConcurrentMap();

  void reserve(const size_t n_buckets) {
    const size_t n_rehashing_buckets = get_n_rehashing_buckets(n_buckets);
    rehash(n_rehashing_buckets);
  };

  size_t get_n_buckets() const { return n_buckets; };

  double get_load_factor() const { return static_cast<double>(n_keys) / n_buckets; }

  double get_max_load_factor() const { return max_load_factor; }

  void set_max_load_factor(const double max_load_factor) {
    this->max_load_factor = max_load_factor;
  }

  size_t get_n_keys() const { return n_keys; }

  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void get(const K& key, const std::function<void(const V&)>& handler);

  V get(const K& key, const V& default_value = V());

  // Iteratively get each key value pair.
  // Blocks all the other operations.
  void get_each(const std::function<void(const K&, const V&)>& handler);

  void unset(const K& key);

  bool has(const K& key);

  void clear();

 private:
  size_t n_keys;

  size_t n_buckets;

  double max_load_factor;

  size_t n_threads;

  size_t n_segments;

  H hasher;

  std::vector<omp_lock_t> segment_locks;

  // For parallel rehashing (Require omp_set_nested(1)).
  std::vector<omp_lock_t> rehashing_segment_locks;

  struct hash_node {
    K key;
    V value;
    std::unique_ptr<hash_node> next;
    hash_node(const K& key, const V& value) : key(key), value(value){};
  };

  std::vector<std::unique_ptr<hash_node>> buckets;

  void rehash() { reserve(n_keys / max_load_factor); }

  void rehash(const size_t n_rehashing_buckets);

  // Get the number of hash buckets to use.
  // This number shall be larger than or equal to the specified number.
  size_t get_n_rehashing_buckets(const size_t n_buckets) const;

  // Apply node_handler to the hash node which has the specific key.
  // If the key does not exist, apply to the unassociated node from the corresponding bucket.
  void hash_node_apply(
      const K& key, const std::function<void(std::unique_ptr<hash_node>&)>& node_handler);

  // Apply node_handler to all the hash nodes.
  void hash_node_all_apply(const std::function<void(std::unique_ptr<hash_node>&)>& node_handler);

  // Recursively find the node with the specified key on the list starting from the node specified.
  // Then apply the specified handler to that node.
  // If the key does not exist, apply the handler to the unassociated node at the end of the list.
  void hash_node_apply_recursive(
      std::unique_ptr<hash_node>& node,
      const K& key,
      const std::function<void(std::unique_ptr<hash_node>&)>& node_handler);

  // Recursively apply the handler to each node on the list from the node specified (post-order).
  void hash_node_all_apply_recursive(
      std::unique_ptr<hash_node>& node,
      const std::function<void(std::unique_ptr<hash_node>&)>& node_handler);

  void lock_all_segments();

  void unlock_all_segments();
};

template <class K, class V, class H>
ConcurrentMap<K, V, H>::ConcurrentMap() {
  n_keys = 0;
  n_buckets = N_INITIAL_BUCKETS;
  buckets.resize(n_buckets);
  max_load_factor = DEFAULT_MAX_LOAD_FACTOR;
  n_segments = Parallel::get_n_threads() * N_SEGMENTS_PER_THREAD;
  segment_locks.resize(n_segments);
  rehashing_segment_locks.resize(n_segments);
  for (auto& lock : segment_locks) omp_init_lock(&lock);
  for (auto& lock : rehashing_segment_locks) omp_init_lock(&lock);
}

template <class K, class V, class H>
ConcurrentMap<K, V, H>::~ConcurrentMap() {
  clear();
  for (auto& lock : segment_locks) omp_destroy_lock(&lock);
  for (auto& lock : rehashing_segment_locks) omp_destroy_lock(&lock);
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::rehash(const size_t n_rehashing_buckets) {
  auto& first_lock = segment_locks[0];
  omp_set_lock(&first_lock);
  if (n_buckets >= n_rehashing_buckets) {
    omp_unset_lock(&first_lock);
    return;
  }

  omp_unset_lock(&first_lock);
  lock_all_segments();
  printf("Lock and rehash %zu\n", n_rehashing_buckets);
  // No decrease in the number of buckets.
  if (n_buckets >= n_rehashing_buckets) {
    unlock_all_segments();
    return;
  }

  // Rehash.
  std::vector<std::unique_ptr<hash_node>> rehashing_buckets(n_rehashing_buckets);
  const auto& node_handler = [&](std::unique_ptr<hash_node>& node) {
    const auto& rehashing_node_handler = [&](std::unique_ptr<hash_node>& rehashing_node) {
      rehashing_node = std::move(node);
      rehashing_node->next.reset();
    };
    const K& key = node->key;
    const size_t hash_value = hasher(key);
    const size_t bucket_id = hash_value % n_rehashing_buckets;
    const size_t segment_id = bucket_id % n_segments;
    auto& lock = rehashing_segment_locks[segment_id];
    omp_set_lock(&lock);
    hash_node_apply_recursive(rehashing_buckets[bucket_id], key, rehashing_node_handler);
    omp_unset_lock(&lock);
  };
#pragma omp parallel for
  for (size_t i = 0; i < n_buckets; i++) {
    hash_node_all_apply_recursive(buckets[i], node_handler);
  }

  buckets = std::move(rehashing_buckets);
  n_buckets = n_rehashing_buckets;
  unlock_all_segments();
}

template <class K, class V, class H>
size_t ConcurrentMap<K, V, H>::get_n_rehashing_buckets(const size_t n_buckets_in) const {
  // Returns a number that is greater than or equal to n_buckets_in.
  // That number is either a prime number itself, or a product of two prime numbers.
  constexpr size_t PRIMES[] = {
      11, 17, 29, 47, 79, 127, 211, 337, 547, 887, 1433, 2311, 3739, 6053, 9791, 15859};
  constexpr size_t N_PRIMES = sizeof(PRIMES) / sizeof(size_t);
  constexpr size_t LAST_PRIME = PRIMES[N_PRIMES - 1];
  size_t remaining_factor = n_buckets_in;
  size_t n_rehashing_buckets = 1;
  while (remaining_factor > LAST_PRIME) {
    remaining_factor /= LAST_PRIME;
    n_rehashing_buckets *= LAST_PRIME;
  }

  // Find a prime larger than or equal to the remaining factor.
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
void ConcurrentMap<K, V, H>::set(
    const K& key, const V& value, const std::function<void(V&, const V&)>& reducer) {
  const auto& node_handler = [&](std::unique_ptr<hash_node>& node) {
    if (!node) {
      node.reset(new hash_node(key, value));
#pragma omp atomic
      n_keys++;
    } else {
      reducer(node->value, value);
    }
  };
  hash_node_apply(key, node_handler);
  if (n_keys >= n_buckets * max_load_factor) rehash();
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::get(const K& key, const std::function<void(const V&)>& handler) {
  const auto& node_handler = [&](std::unique_ptr<hash_node>& node) {
    if (node) handler(node->value);
  };
  hash_node_apply(key, node_handler);
}

template <class K, class V, class H>
V ConcurrentMap<K, V, H>::get(const K& key, const V& default_value) {
  V value(default_value);
  const auto& node_handler = [&](const std::unique_ptr<hash_node>& node) {
    if (node) value = node->value;
  };
  hash_node_apply(key, node_handler);
  return value;
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::unset(const K& key) {
  const auto& node_handler = [&](std::unique_ptr<hash_node>& node) {
    if (node) {
      node = std::move(node->next);
#pragma omp atomic
      n_keys--;
    }
  };
  hash_node_apply(key, node_handler);
}

template <class K, class V, class H>
bool ConcurrentMap<K, V, H>::has(const K& key) {
  bool has_key = false;
  const auto& node_handler = [&](const std::unique_ptr<hash_node>& node) {
    if (node) has_key = true;
  };
  hash_node_apply(key, node_handler);
  return has_key;
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::clear() {
  lock_all_segments();
#pragma omp parallel for
  for (size_t i = 0; i < n_buckets; i++) {
    buckets[i].reset();
  }
  n_keys = 0;
  unlock_all_segments();
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::hash_node_apply(
    const K& key, const std::function<void(std::unique_ptr<hash_node>&)>& node_handler) {
  const size_t hash_value = hasher(key);
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
    hash_node_apply_recursive(buckets[bucket_id], key, node_handler);
    omp_unset_lock(&lock);
    applied = true;
  }
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::hash_node_all_apply(
    const std::function<void(std::unique_ptr<hash_node>&)>& node_handler) {
  lock_all_segments();
#pragma omp parallel for
  for (size_t i = 0; i < n_buckets; i++) {
    hash_node_all_apply_recursive(buckets[i], node_handler);
  }
  unlock_all_segments();
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::hash_node_apply_recursive(
    std::unique_ptr<hash_node>& node,
    const K& key,
    const std::function<void(std::unique_ptr<hash_node>&)>& node_handler) {
  if (node) {
    if (node->key == key) {
      node_handler(node);
    } else {
      hash_node_apply_recursive(node->next, key, node_handler);
    }
  } else {
    node_handler(node);
  }
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::hash_node_all_apply_recursive(
    std::unique_ptr<hash_node>& node,
    const std::function<void(std::unique_ptr<hash_node>&)>& node_handler) {
  if (node) {
    // Post-order traversal for rehashing.
    hash_node_all_apply_recursive(node->next, node_handler);
    node_handler(node);
  }
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::lock_all_segments() {
  for (auto& lock : segment_locks) omp_set_lock(&lock);
}

template <class K, class V, class H>
void ConcurrentMap<K, V, H>::unlock_all_segments() {
  for (auto& lock : segment_locks) omp_unset_lock(&lock);
}

}  // namespace hpmr

#endif
