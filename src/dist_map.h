#pragma once

#include <functional>
#include "concurrent_map.h"
#include "reducer.h"

namespace hpmr {

template <class K, class V, class H = std::hash<K>>
class DistMap {
 public:
  DistMap();

  ~DistMap();

  void reserve(const size_t n_buckets_min);

  size_t get_n_buckets();

  double get_load_factor();

  double get_max_load_factor() { return max_load_factor; }

  void set_max_load_factor(const double max_load_factor);

  size_t get_n_keys();

  void set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  void get(const K& key, const std::function<void(const V&)>& handler);

  V get(const K& key, const V& default_value = V());

  bool has(const K& key);

  void clear();

  template <class KR, class VR, class HR = std::hash<KR>>
  DistMap<KR, VR, HR> mapreduce(
      const std::function<
          void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>& mapper,
      const std::function<void(VR&, const VR&)>& reducer,
      const bool verbose = false);

  void sync(const bool verbose = false);

 private:
  double max_load_factor;

  size_t n_procs_cache;

  size_t proc_id_cache;

  H hasher;

  ConcurrentMap<K, V, H> local_map;

  std::vector<ConcurrentMap<K, V, H>> remote_maps;

  constexpr static double DEFAULT_MAX_LOAD_FACTOR = 1.0;
};

#include "dist_map.inl"

}  // namespace hpmr
