#pragma once

namespace hpmr {
template <class K, class V>
class HashEntry {
 public:
  K key;

  size_t hash_value;

  V value;

  bool filled;

  HashEntry() : filled(false){};

  void fill(const K& key, const size_t hash_value, const V& value) {
    this->key = key;
    this->hash_value = hash_value;
    this->value = value;
    filled = true;
  }

  HashEntry& operator=(const HashEntry<K, V>& entry) {
    if (entry.filled) {
      key = entry.key;
      hash_value = entry.hash_value;
      value = entry.value;
      filled = true;
    } else {
      filled = false;
    }
    return *this;
  }
};
}  // namespace hpmr
