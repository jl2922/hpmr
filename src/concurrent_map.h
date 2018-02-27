// #pragma once

// #include "bare_concurrent_map.h"
// #include "dist_map.h"
// #include "reducer.h"

// namespace hpmr {

// template <class K, class V, class H = std::hash<K>>
// class ConcurrentMap {
//  public:
//   void reserve(const size_t n_buckets_min) { bare_map.reserve(n_buckets_min); }

//   size_t get_n_buckets() const { return bare_map.get_n_buckets(); };

//   double get_load_factor() const { return bare_map.get_load_factor(); }

//   double get_max_load_factor() const { return bare_map.get_max_load_factor(); }

//   void set_max_load_factor(const double max_load_factor) {
//     bare_map.set_max_load_factor(max_load_factor);
//   }

//   size_t get_n_keys() const { return bare_map.get_n_keys(); }

//   void set(
//       const K& key,
//       const V& value,
//       const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite) {
//     bare_map.set(key, get_hash_value(key), value, reducer);
//   }

//   void get(const K& key, const std::function<void(const V&)>& handler) {
//     bare_map.get(key, get_hash_value(key), handler);
//   }

//   V get(const K& key, const V& default_value = V()) {
//     return bare_map.get(key, get_hash_value(key), default_value);
//   }

//   void unset(const K& key) { bare_map.unset(key, get_hash_value(key)); }

//   bool has(const K& key) { return bare_map.has(key, get_hash_value(key)); }

//   void clear() { bare_map.clear(); }

//   void clear_and_shrink() { bare_map.clear_and_shrink(); }

//   template <class KR, class VR, class HR = std::hash<KR>>
//   DistMap<KR, VR, HR> mapreduce(
//       const std::function<
//           void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>& mapper,
//       const std::function<void(VR&, const VR&)>& reducer,
//       const bool verbose = false);

//  private:
//   BareConcurrentMap<K, V, H> bare_map;

//   size_t get_hash_value(const K& key) { return bare_map.get_hash_value(key); }
// };

// template <class K, class V, class H>
// template <class KR, class VR, class HR>
// DistMap<KR, VR, HR> ConcurrentMap<K, V, H>::mapreduce(
//     const std::function<void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>&
//         mapper,
//     const std::function<void(VR&, const VR&)>& reducer,
//     const bool verbose) {
//   DistMap<KR, VR, HR> res;
//   const int proc_id = Parallel::get_proc_id();
//   const int n_procs = Parallel::get_n_procs();
//   const int n_threads = Parallel::get_n_threads();
//   double target_progress = 0.1;

//   const auto& emit = [&](const KR& key, const VR& value) { res.async_set(key, value, reducer); };
//   if (verbose && proc_id == 0) {
//     printf("MapReduce on %d node(s) (%d threads): ", n_procs, n_threads * n_procs);
//   }

//   const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double progress) {
//     mapper(node->key, node->value, emit);
//     const int thread_id = Parallel::get_thread_id();
//     if (verbose && proc_id == 0 && thread_id == 0) {
//       while (target_progress <= progress) {
//         printf("%.1f%% ", target_progress);
//         target_progress *= 2;
//       }
//     }
//   };
//   bare_map.all_node_apply(node_handler);

//   res.sync(reducer, verbose);

//   if (verbose && proc_id == 0) printf("Done\n");

//   return res;
// }

// }  // namespace hpmr
