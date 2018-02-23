#pragma once

#include <memory>

template <class K, class V>
class HashPendingNode {
 public:
  K key;

  V value;

  size_t hash_value;

  unsigned short backoff;

  unsigned short tries;

  std::unique_ptr<HashNode> next;

  HashPendingNode(const K& key, const V& value) : key(key), value(value){};

 private:
  constexpr unsigned short MAX_TRIES = 5;
};
