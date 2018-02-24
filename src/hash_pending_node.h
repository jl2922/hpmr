#pragma once

#include <memory>

template <class K, class V>
class HashPendingNode {
 public:
  K key;

  V value;

  size_t hash_value;

  int backoff;

  unsigned short tries;

  std::unique_ptr<HashPendingNode> next;

  HashPendingNode(const K& key, const V& value) : key(key), value(value){};

};
