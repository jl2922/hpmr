#pragma once

#include <cassert>
#include <functional>
#include <vector>
#include "bare_hash_container.h"
#include "hash_entry.h"
#include "reducer.h"

namespace hpmr {

// A linear probing hash map that requires providing hash values when use.
template <class K, class V, class H = std::hash<K>>
class BareMap : public BareHashContainer<K, V, H> {
 public:
  void set(const K& key, const size_t hash_value);

  void for_each(const std::function<void(const K& key, const size_t hash_value)>& handler);

  using BareHashContainer<K, V, H>::max_load_factor;

 protected:
  using BareHashContainer<K, V, H>::n_keys;

  using BareHashContainer<K, V, H>::n_buckets;

  using BareHashContainer<K, V, H>::buckets;

  using BareHashContainer<K, V, H>::check_balance;

  using BareHashContainer<K, V, H>::reserve_n_buckets;
};

template <class K, class V, class H>
void BareMap<K, V, H>::set(const K& key, const size_t hash_value) {
  size_t bucket_id = hash_value % n_buckets;
  size_t n_probes = 0;
  while (n_probes < n_buckets) {
    if (!buckets.at(bucket_id).filled) {
      buckets.at(bucket_id).fill(key, hash_value, value);
      n_keys++;
      if (n_buckets * max_load_factor <= n_keys) reserve_n_buckets(n_buckets * 2);
      break;
    } else if (buckets.at(bucket_id).hash_value == hash_value && buckets.at(bucket_id).key == key) {
      break;
    } else {
      n_probes++;
      bucket_id = (bucket_id + 1) % n_buckets;
    }
  }
  check_balance(n_probes);
}

template <class K, class V, class H>
void BareMap<K, V, H>::for_each(
    const std::function<void(const K& key, const size_t hash_value)>& handler) {
  if (n_keys == 0) return;
  for (size_t i = 0; i < n_buckets; i++) {
    if (buckets.at(i).filled) {
      handler(buckets.at(i).key, buckets.at(i).hash_value);
    }
  }
}
}  // namespace hpmr

namespace hps {
template <class K, class V, class H, class B>
class Serializer<hpmr::BareMap<K, V, H>, B> {
 public:
  static void serialize(const hpmr::BareMap<K, V, H>& map, OutputBuffer<B>& buf) {
    map.serialize(buf);
  }
  static void parse(hpmr::BareMap<K, V, H>& map, InputBuffer<B>& buf) { map.parse(buf); }
};
}  // namespace hps
