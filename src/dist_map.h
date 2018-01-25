#include "parallel.h"

namespace hpmr {

template <class K, class V, class H>
class DistMap {
 public:
  DistMap();

  void set(const K& key, const V& value);

  void set(const K& key, const V& value, const std::function<void(V&, const V&)>& reducer);

  void get(const K& key, const std::function<void(V&)>& user);

  V get_copy_or_default(const K& key, const V& default_value);

  unsigned long long get_n_keys();

  template <class KR, class VR, class HR>
  DistMap<KR, VR, HR> mapreduce(
      const std::function<
          void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>& mapper,
      const std::function<void(VR&, const VR&)>& reducer,
      const bool verbose = false);

 private:
  unsigned long long n_keys;
};

template <class K, class V, class H>
DistMap<K, V, H>::DistMap() {
  n_keys = 0;
}

template <class K, class V, class H>
void DistMap<K, V, H>::set(const K&, const V&) {
#pragma omp atomic
  n_keys++;
}

template <class K, class V, class H>
unsigned long long DistMap<K, V, H>::get_n_keys() {
  return Parallel::reduce_sum(n_keys);
}

}  // namespace hpmr
