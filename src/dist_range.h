#include <functional>
#include "dist_map.h"
#include "parallel.h"

namespace hpmr {
template <class T>
class DistRange {
 public:
  DistRange(const T start, const T end, const T step = 1) : start(start), end(end), step(step) {}

  template <class K, class V, class H>
  DistMap<K, V, H> map(
      const std::function<void(const T, const std::function<void(const K&, const V&)>&)>& mapper);

 private:
  T start;
  T end;
  T step;
};

template <class T>
template <class K, class V, class H>
DistMap<K, V, H> DistRange<T>::map(
    const std::function<void(const T, const std::function<void(const K&, const V&)>&)>& mapper) {
  DistMap<K, V, H> res;
  const auto& emit = [&](const K& key, const V& value) { res.set(key, value); };
  const int n_procs = Parallel::get_n_procs();
  const int proc_id = Parallel::get_proc_id();
#pragma omp parallel for schedule(dynamic, 5)
  for (T i = start + proc_id * n_procs; i < end; i += step * n_procs) {
    mapper(i, emit);
  }
  return res;
}

}  // namespace hpmr
