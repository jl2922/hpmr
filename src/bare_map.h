#pragma once

#include <omp.h>
#include <functional>
#include <memory>
#include <vector>
#include "hash_node.h"
#include "reducer.h"

namespace hpmr {

// A map that requires providing hash values when use.
template <class K, class V, class H = std::hash<K>>
class BareMap {
 public:
  BareMap();

  BareMap(const BareMap& m);

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
      const std::function<void(V&, const V&)>& reducer = hpmr::Reducer<V>::overwrite);

  void unset(const K& key, const size_t hash_value);

  void get(const K& key, const size_t hash_value, const std::function<void(const V&)>& handler);

  V get(const K& key, const size_t hash_value, const V& default_value = V());

  bool has(const K& key, const size_t hash_value);

  void clear();

  void clear_and_shrink();

  // Apply node_handler to the hash node which has the specific key.
  // If the key does not exist, apply to the unassociated node from the corresponding bucket.
  void key_node_apply(
      const K& key,
      const size_t hash_value,
      const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler);

  // Apply node_handler to all the hash nodes.
  void all_node_apply(
      const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler);

 private:
  size_t n_keys;

  size_t n_buckets;

  double max_load_factor;

  H hasher;

  std::vector<std::unique_ptr<HashNode<K, V>>> buckets;

  constexpr static size_t N_INITIAL_BUCKETS = 11;

  constexpr static double DEFAULT_MAX_LOAD_FACTOR = 1.0;

  void rehash();

  void rehash(const size_t n_rehashing_buckets);

  size_t get_n_rehashing_buckets(const size_t n_buckets_min) const;

  void key_node_apply_recursive(
      std::unique_ptr<HashNode<K, V>>& node,
      const K& key,
      const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler);

  // Recursively apply the handler in post-order.
  void all_node_apply_recursive(
      std::unique_ptr<HashNode<K, V>>& node,
      const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler,
      const double progress = 0.0);
};

template <class K, class V, class H>
BareMap<K, V, H>::BareMap() {
  n_keys = 0;
  n_buckets = N_INITIAL_BUCKETS;
  buckets.resize(n_buckets);
  set_max_load_factor(DEFAULT_MAX_LOAD_FACTOR);
}

template <class K, class V, class H>
BareMap<K, V, H>::BareMap(const BareMap<K, V, H>& m) {
  n_keys = m.n_keys;
  n_buckets = m.n_buckets;
  buckets.resize(n_buckets);
  max_load_factor = m.max_load_factor;
  for (size_t i = 0; i < n_buckets; i++) {
    HashNode<K, V>* m_node_ptr = m.buckets[i].get();
    HashNode<K, V>* node_ptr = nullptr;
    while (m_node_ptr != nullptr) {
      if (node_ptr == nullptr) {  // Head node.
        buckets[i].reset(new HashNode<K, V>(m_node_ptr->key, m_node_ptr->value));
        node_ptr = buckets[i].get();
      } else {
        node_ptr->next.reset(new HashNode<K, V>(m_node_ptr->key, m_node_ptr->value));
        node_ptr = node_ptr->next.get();
      }
      m_node_ptr = m_node_ptr->next.get();
    }
  }
}

template <class K, class V, class H>
void BareMap<K, V, H>::reserve(const size_t n_buckets_min) {
  if (n_buckets >= n_buckets_min) return;
  const size_t n_rehashing_buckets = get_n_rehashing_buckets(n_buckets_min);
  rehash(n_rehashing_buckets);
};

template <class K, class V, class H>
void BareMap<K, V, H>::set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node) {
    if (!node) {
      node.reset(new HashNode<K, V>(key, value));
      n_keys++;
    } else {
      reducer(node->value, value);
    }
  };
  key_node_apply(key, hash_value, node_handler);
  if (n_keys >= n_buckets * max_load_factor) rehash();
}

template <class K, class V, class H>
void BareMap<K, V, H>::unset(const K& key, const size_t hash_value) {
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node) {
    if (node) {
      node = std::move(node->next);
      n_keys--;
    }
  };
  key_node_apply(key, hash_value, node_handler);
}

template <class K, class V, class H>
void BareMap<K, V, H>::get(
    const K& key, const size_t hash_value, const std::function<void(const V&)>& handler) {
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node) {
    if (node) handler(node->value);
  };
  key_node_apply(key, hash_value, node_handler);
}

template <class K, class V, class H>
V BareMap<K, V, H>::get(const K& key, const size_t hash_value, const V& default_value) {
  V value(default_value);
  const auto& node_handler = [&](const std::unique_ptr<HashNode<K, V>>& node) {
    if (node) value = node->value;
  };
  key_node_apply(key, hash_value, node_handler);
  return value;
}

template <class K, class V, class H>
bool BareMap<K, V, H>::has(const K& key, const size_t hash_value) {
  bool has_key = false;
  const auto& node_handler = [&](const std::unique_ptr<HashNode<K, V>>& node) {
    if (node) has_key = true;
  };
  key_node_apply(key, hash_value, node_handler);
  return has_key;
}

template <class K, class V, class H>
void BareMap<K, V, H>::clear() {
  for (size_t i = 0; i < n_buckets; i++) {
    buckets[i].reset();
  }
  n_keys = 0;
}

template <class K, class V, class H>
void BareMap<K, V, H>::clear_and_shrink() {
  buckets.resize(N_INITIAL_BUCKETS);
  for (size_t i = 0; i < N_INITIAL_BUCKETS; i++) {
    buckets[i].reset();
  }
  n_keys = 0;
  n_buckets = N_INITIAL_BUCKETS;
}

template <class K, class V, class H>
void BareMap<K, V, H>::key_node_apply(
    const K& key,
    const size_t hash_value,
    const std::function<void(std::unique_ptr<HashNode<K, V>>&)>& node_handler) {
  const size_t bucket_id = hash_value % n_buckets;
  key_node_apply_recursive(buckets[bucket_id], key, node_handler);
}

template <class K, class V, class H>
void BareMap<K, V, H>::all_node_apply(
    const std::function<void(std::unique_ptr<HashNode<K, V>>&, const double)>& node_handler) {
  const double progress_factor = 100.0 / n_buckets;
  for (size_t i = 0; i < n_buckets; i++) {
    all_node_apply_recursive(buckets[i], node_handler, i * progress_factor);
  }
}

template <class K, class V, class H>
void BareMap<K, V, H>::key_node_apply_recursive(
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
void BareMap<K, V, H>::all_node_apply_recursive(
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
void BareMap<K, V, H>::rehash() {
  const size_t n_buckets_min = static_cast<size_t>(n_keys / max_load_factor);
  reserve(n_buckets_min * 2);
}

template <class K, class V, class H>
void BareMap<K, V, H>::rehash(const size_t n_rehashing_buckets) {
  if (n_buckets >= n_rehashing_buckets) return;
  std::vector<std::unique_ptr<HashNode<K, V>>> rehashing_buckets(n_rehashing_buckets);
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double) {
    const auto& rehashing_node_handler = [&](std::unique_ptr<HashNode<K, V>>& rehashing_node) {
      rehashing_node = std::move(node);
      rehashing_node->next.reset();
    };
    const K& key = node->key;
    const size_t hash_value = hasher(key);
    const size_t bucket_id = hash_value % n_rehashing_buckets;
    key_node_apply_recursive(rehashing_buckets[bucket_id], key, rehashing_node_handler);
  };
  for (size_t i = 0; i < n_buckets; i++) {
    all_node_apply_recursive(buckets[i], node_handler);
  }
  buckets = std::move(rehashing_buckets);
  n_buckets = n_rehashing_buckets;
}

template <class K, class V, class H>
size_t BareMap<K, V, H>::get_n_rehashing_buckets(const size_t n_buckets_min) const {
  // Returns a number that is greater than or roughly equals to n_buckets_min.
  // That number is either a prime number or the product of several prime numbers.
  constexpr size_t PRIMES[] = {
      11, 17, 29, 47, 79, 127, 211, 337, 547, 887, 1433, 2311, 3739, 6053, 9791, 15859};
  constexpr size_t N_PRIMES = sizeof(PRIMES) / sizeof(size_t);
  constexpr size_t LAST_PRIME = PRIMES[N_PRIMES - 1];
  constexpr size_t BIG_PRIME = PRIMES[N_PRIMES - 5];
  size_t remaining_factor = n_buckets_min;
  remaining_factor += n_buckets_min / 4;
  size_t n_rehashing_buckets = 1;
  while (remaining_factor > LAST_PRIME) {
    remaining_factor /= BIG_PRIME;
    n_rehashing_buckets *= BIG_PRIME;
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

}  // namespace hpmr
