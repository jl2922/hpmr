#pragma once

#include <cassert>
#include <functional>
#include <vector>
#include "../hps/src/hps.h"
#include "hash_entry.h"
#include "reducer.h"

namespace hpmr {
// A linear probing hash map that requires providing hash values when use.
template <class K, class V, class H = std::hash<K>>
class BareMap {
 public:
  size_t n_keys;

  float max_load_factor;

  size_t n_buckets;

  std::vector<HashEntry<K, V>> buckets;

  constexpr static float DEFAULT_MAX_LOAD_FACTOR = 0.7;

  BareMap();

  void reserve(const size_t n_keys_min);

  void reserve_n_buckets(const size_t n_buckets_min);

  size_t get_n_buckets() { return n_buckets; }

  void set(
      const K& key,
      const size_t hash_value,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = hpmr::Reducer<V>::overwrite);

  V get(const K& key, const size_t hash_value, const V& default_value = V());

  void unset(const K& key, const size_t hash_value);

  bool has(const K& key, const size_t hash_value);

  void clear();

  void clear_and_shrink();

  void for_each(
      const std::function<void(const K& key, const size_t hash_value, const V& value)>& handler);

 private:
  constexpr static size_t N_INITIAL_BUCKETS = 11;

  constexpr static size_t MAX_N_PROBES = 64;

  size_t get_n_rehash_buckets(const size_t n_buckets_min);

  void rehash(const size_t n_rehash_buckets);
};

template <class K, class V, class H>
BareMap<K, V, H>::BareMap() {
  n_keys = 0;
  n_buckets = N_INITIAL_BUCKETS;
  buckets.resize(N_INITIAL_BUCKETS);
  max_load_factor = DEFAULT_MAX_LOAD_FACTOR;
}

template <class K, class V, class H>
void BareMap<K, V, H>::reserve_n_buckets(const size_t n_buckets_min) {
  if (n_buckets_min <= n_buckets) return;
  const size_t n_rehash_buckets = get_n_rehash_buckets(n_buckets_min);
  rehash(n_rehash_buckets);
}

template <class K, class V, class H>
void BareMap<K, V, H>::reserve(const size_t n_keys_min) {
  reserve_n_buckets(n_keys_min / max_load_factor);
}

template <class K, class V, class H>
size_t BareMap<K, V, H>::get_n_rehash_buckets(const size_t n_buckets_min) {
  constexpr size_t PRIMES[] = {
      11, 17, 29, 47, 79, 127, 211, 337, 547, 887, 1433, 2311, 3739, 6053, 9791, 15859};
  constexpr size_t N_PRIMES = sizeof(PRIMES) / sizeof(size_t);
  constexpr size_t LAST_PRIME = PRIMES[N_PRIMES - 1];
  constexpr size_t BIG_PRIME = PRIMES[N_PRIMES - 5];
  size_t remaining_factor = n_buckets_min + n_buckets_min / 4;
  size_t n_rehash_buckets = 1;
  while (remaining_factor > LAST_PRIME) {
    remaining_factor /= BIG_PRIME;
    n_rehash_buckets *= BIG_PRIME;
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
  n_rehash_buckets *= PRIMES[left];
  return n_rehash_buckets;
}

template <class K, class V, class H>
void BareMap<K, V, H>::rehash(const size_t n_rehash_buckets) {
  std::vector<HashEntry<K, V>> rehash_buckets(n_rehash_buckets);
  for (size_t i = 0; i < n_buckets; i++) {
    if (!buckets.at(i).filled) continue;
    const size_t hash_value = buckets.at(i).hash_value;
    size_t rehash_bucket_id = hash_value % n_rehash_buckets;
    size_t n_probes = 0;
    while (n_probes < n_rehash_buckets) {
      if (!rehash_buckets.at(rehash_bucket_id).filled) {
        rehash_buckets.at(rehash_bucket_id) = buckets.at(i);
        break;
      } else {
        n_probes++;
        rehash_bucket_id = (rehash_bucket_id + 1) % n_rehash_buckets;
      }
    }
    assert(n_probes < n_rehash_buckets);
  }
  buckets = std::move(rehash_buckets);
  n_buckets = n_rehash_buckets;
}

template <class K, class V, class H>
void BareMap<K, V, H>::set(
    const K& key,
    const size_t hash_value,
    const V& value,
    const std::function<void(V&, const V&)>& reducer) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      buckets.at(bucket_id).fill(key, hash_value, value);
      n_keys++;
      if (n_buckets * max_load_factor <= n_keys) reserve_n_buckets(n_buckets * 2);
      break;
    } else if (buckets.at(bucket_id).hash_value == hash_value && buckets.at(bucket_id).key == key) {
      reducer(buckets.at(bucket_id).value, value);
      break;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  assert(n_probes < n_buckets);
  if (n_probes > MAX_N_PROBES) {
    reserve_n_buckets(n_buckets * 2);
    // TODO: Check hash inbalance.
  }
}

template <class K, class V, class H>
V BareMap<K, V, H>::get(const K& key, const size_t hash_value, const V& default_value) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      return default_value;
    } else if (buckets.at(bucket_id).hash_value == hash_value && buckets.at(bucket_id).key == key) {
      return buckets.at(bucket_id).value;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  return default_value;
}

template <class K, class V, class H>
void BareMap<K, V, H>::unset(const K& key, const size_t hash_value) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      return;
    } else if (buckets.at(bucket_id).hash_value == hash_value && buckets.at(bucket_id).key == key) {
      buckets.at(bucket_id).filled = false;
      n_keys--;
      // Find a valid entry to fill the spot if exists.
      size_t swap_bucket_id = (bucket_id + 1) % n_buckets;
      while (buckets.at(swap_bucket_id).filled) {
        const size_t swap_origin_id = buckets.at(swap_bucket_id).hash_value % n_buckets;
        if ((swap_bucket_id < swap_origin_id && swap_origin_id <= bucket_id) ||
            (swap_origin_id <= bucket_id && bucket_id < swap_bucket_id) ||
            (bucket_id < swap_bucket_id && swap_bucket_id < swap_origin_id)) {
          buckets.at(bucket_id) = buckets.at(swap_bucket_id);
          buckets.at(swap_bucket_id).filled = false;
          bucket_id = swap_bucket_id;
        }
        swap_bucket_id = (swap_bucket_id + 1) % n_buckets;
      }
      return;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
}

template <class K, class V, class H>
bool BareMap<K, V, H>::has(const K& key, const size_t hash_value) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      return false;
    } else if (buckets.at(bucket_id).hash_value == hash_value && buckets.at(bucket_id).key == key) {
      return true;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  return false;
}

template <class K, class V, class H>
void BareMap<K, V, H>::clear() {
  if (n_keys == 0) return;
  for (size_t i = 0; i < n_buckets; i++) {
    buckets.at(i).filled = false;
  }
  n_keys = 0;
}

template <class K, class V, class H>
void BareMap<K, V, H>::clear_and_shrink() {
  buckets.resize(N_INITIAL_BUCKETS);
  n_buckets = N_INITIAL_BUCKETS;
  clear();
}

template <class K, class V, class H>
void BareMap<K, V, H>::for_each(
    const std::function<void(const K& key, const size_t hash_value, const V& value)>& handler) {
  if (n_keys == 0) return;
  for (size_t i = 0; i < n_buckets; i++) {
    if (buckets.at(i).filled) {
      handler(buckets.at(i).key, buckets.at(i).hash_value, buckets.at(i).value);
    }
  }
}
}  // namespace hpmr
