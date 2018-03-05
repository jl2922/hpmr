#pragma once

#include <cstddef>
#include "bare_map.h"

// Linear probing hash set.
namespace hpmr {
template <class K, class H = std::hash<K>>
class HashSet {
 public:
  size_t get_n_keys() { return bare_map.n_keys; }

  size_t get_n_buckets() { return bare_map.get_n_buckets(); }

  void reserve(const size_t n_keys_min) { bare_map.reserve(n_keys_min); }

  void set(const K& key, const size_t hash_value) { bare_map.set(key, hash_value); }

  void unset(const K& key, const size_t hash_value) { bare_map.unset(key, hash_value); }

  bool has(const K& key, const size_t hash_value) { return bare_map.has(key, hash_value); }

  void clear() { bare_map.clear(); }

  void clear_and_shrink() { bare_map.clear_and_shrink(); }

  void for_each(const std::function<void(const K& key, const size_t hash_value)>& handler) {
    bare_map.for_each(handler);
  }

 private:
  BareMap<K, void, H> bare_map;
  //  public:
  //   size_t n_keys;

  //   float max_load_factor;

  //   size_t n_buckets;

  //   std::vector<HashEntry<K, V>> buckets;

  //   constexpr static float DEFAULT_MAX_LOAD_FACTOR = 0.7;

  //   HashSet();

  //   void reserve(const size_t n_keys_min);

  //   void reserve_n_buckets(const size_t n_buckets_min);

  //   size_t get_n_buckets() const { return n_buckets; }

  //   void set(
  //       const K& key,
  //       const size_t hash_value,
  //       const V& value,
  //       const std::function<void(V&, const V&)>& reducer = hpmr::Reducer<V>::overwrite);

  //   V get(const K& key, const size_t hash_value, const V& default_value = V());

  //   void unset(const K& key, const size_t hash_value);

  //   bool has(const K& key, const size_t hash_value);

  //   void clear();

  //   void clear_and_shrink();

  //   void for_each(
  //       const std::function<void(const K& key, const size_t hash_value, const V& value)>&
  //       handler);

  //  private:
  //   constexpr static size_t N_INITIAL_BUCKETS = 11;

  //   constexpr static size_t MAX_N_PROBES = 64;

  //   bool unbalanced_warned;

  //   size_t get_n_rehash_buckets(const size_t n_buckets_min);

  //   void rehash(const size_t n_rehash_buckets);

  //   void check_balance();
};
}  // namespace hpmr
