#include <functional>
#include <type_traits>
#include "dist_map.h"
#include "parallel.h"

namespace hpmr {
template <class T>
class DistRange {
 public:
  DistRange(const T start, const T end, const T step = 1) : start(start), end(end), step(step) {}

  template <class K, class V, class H>
  DistMap<K, V, H> mapreduce(
      const std::function<void(const T, const std::function<void(const K&, const V&)>&)>& mapper,
      const std::function<void(T&, const T&)>& reducer,
      const bool verbose = false);

 private:
  T start;
  T end;
  T step;
};

template <class T>
template <class K, class V, class H>
DistMap<K, V, H> DistRange<T>::mapreduce(
    const std::function<void(const T, const std::function<void(const K&, const V&)>&)>& mapper,
    const std::function<void(T&, const T&)>& reducer,
    const bool verbose) {
  DistMap<K, V, H> res;
  const int proc_id = Parallel::get_proc_id();
  const int n_procs = Parallel::get_n_procs();
  double target_progress = 0.1;

  const auto& emit = [&](const K& key, const V& value) { res.set(key, value, reducer); };

#pragma omp parallel for schedule(dynamic, 5)
  for (T i = start + proc_id * step; i < end; i += step * n_procs) {
    mapper(i, emit);
    const int thread_id = Parallel::get_thread_id();
    if (verbose && thread_id == 0) {
      const double current_progress = (i - start) * 100.0 / (end - start);
      if (target_progress <= current_progress) {
        if (proc_id == 0) printf("%.1f%% ", target_progress);
        target_progress *= 2;
      }
    }
  }
  res.sync();
  if (verbose) printf("Done\n");

  return res;
}

}  // namespace hpmr
