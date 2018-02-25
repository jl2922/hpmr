#pragma once

#include <memory>

template <class K, class V>
class HashNode {
 public:
  K key;

  size_t hash_value;

  V value;

  std::unique_ptr<HashNode> next;

  HashNode(const K& key, const size_t hash_value, const V& value)
      : key(key), hash_value(hash_value), value(value){};
};
