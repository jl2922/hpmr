#pragma once

#include "bare_set.h"

// Linear probing hash set for better parallel performance.
namespace hpmr {
template <class K, class H = std::hash<K>>
class HashSet : public BareSet<K, H> {
 public:
  void set(const K& key) { BareSet<K, H>::set(key, hasher(key)); }

  void unset(const K& key) { BareSet<K, H>::unset(key, hasher(key)); }

  bool has(const K& key) { return BareSet<K, H>::has(key, hasher(key)); }

  void for_each(const std::function<void(const K& key)>& handler) const;

 private:
  H hasher;
};

template <class K, class H>
void HashSet<K, H>::for_each(const std::function<void(const K& key)>& handler) const {
  if (n_keys == 0) return;
  for (size_t i = 0; i < n_buckets; i++) {
    if (buckets.at(i).filled) {
      handler(buckets.at(i).key;
    }
  }
}
}  // namespace hpmr
