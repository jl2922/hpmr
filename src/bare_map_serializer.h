#pragma once

#include "bare_map.h"
#include "hash_entry_serializer.h"

namespace hps {
template <class K, class V, class H, class B>
class Serializer<hpmr::BareMap<K, V, H>, B> {
 public:
  static void serialize(const hpmr::BareMap<K, V, H>& map, OutputBuffer<B>& ob) {
    Serializer<size_t, B>::serialize(map.n_keys, ob);
    Serializer<float, B>::serialize(map.max_load_factor, ob);
    Serializer<std::vector<hpmr::HashEntry<K, V>>, B>::serialize(map.buckets, ob);
  }
  static void parse(hpmr::BareMap<K, V, H>& map, InputBuffer<B>& ib) {
    Serializer<size_t, B>::parse(map.n_keys, ib);
    Serializer<float, B>::parse(map.max_load_factor, ib);
    Serializer<std::vector<hpmr::HashEntry<K, V>>, B>::parse(map.buckets, ib);
    map.n_buckets = map.buckets.size();
  }
};
}  // namespace hps
