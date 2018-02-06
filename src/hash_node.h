#pragma once

#include <memory>

template <class K, class V>
class HashNode {
 public:
  K key;
  V value;
  std::unique_ptr<HashNode> next;
  HashNode(const K& key, const V& value) : key(key), value(value){};
};
