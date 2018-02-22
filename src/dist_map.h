#pragma once

#include <cstdlib>
#include <ctime>
#include <functional>
#include "bare_concurrent_map.h"
#include "dist_hasher.h"
#include "parallel.h"
#include "reducer.h"

namespace hpmr {

template <class K, class V, class H = std::hash<K>>
class DistMap {
 public:
  DistMap();

  void reserve(const size_t n_buckets_min);

  size_t get_n_buckets();

  double get_load_factor();

  double get_max_load_factor() const { return max_load_factor; }

  void set_max_load_factor(const double max_load_factor);

  size_t get_n_keys();

  void async_set(
      const K& key,
      const V& value,
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::overwrite);

  V get(const K& key, const V& default_value = V());

  void clear();

  void clear_and_shrink();

  template <class KR, class VR, class HR = std::hash<KR>>
  DistMap<KR, VR, HR> mapreduce(
      const std::function<
          void(const K&, const V&, const std::function<void(const KR&, const VR&)>&)>& mapper,
      const std::function<void(VR&, const VR&)>& reducer,
      const bool verbose = false);

  void sync(
      const std::function<void(V&, const V&)>& reducer = Reducer<V>::keep,
      const bool verbose = false,
      const int trunk_size = DEFAULT_TRUNK_SIZE);

 private:
  double max_load_factor;

  size_t n_procs_cache;

  size_t proc_id_cache;

  H hasher;

  BareConcurrentMap<K, V, DistHasher<K, H>> local_map;

  std::vector<BareConcurrentMap<K, V, DistHasher<K, H>>> remote_maps;

  constexpr static double DEFAULT_MAX_LOAD_FACTOR = 1.0;

  constexpr static int DEFAULT_TRUNK_SIZE = 1 << 20;

  std::vector<int> generate_shuffled_procs();

  int get_base_id(const std::vector<int>& shuffled_procs);
};

template <class K, class V, class H>
DistMap<K, V, H>::DistMap() {
  set_max_load_factor(DEFAULT_MAX_LOAD_FACTOR);
  n_procs_cache = static_cast<size_t>(Parallel::get_n_procs());
  proc_id_cache = static_cast<size_t>(Parallel::get_proc_id());
  remote_maps.resize(n_procs_cache);
}

template <class K, class V, class H>
void DistMap<K, V, H>::reserve(const size_t n_buckets_min) {
  local_map.reserve(n_buckets_min / n_procs_cache);
  for (auto& remote_map : remote_maps) {
    remote_map.reserve(n_buckets_min / n_procs_cache / n_procs_cache);
  }
}

template <class K, class V, class H>
size_t DistMap<K, V, H>::get_n_buckets() {
  const size_t local_n_buckets = local_map.get_n_buckets();
  size_t n_buckets;
  Parallel::reduce_sum(&local_n_buckets, &n_buckets, 1);
  return n_buckets;
}

template <class K, class V, class H>
double DistMap<K, V, H>::get_load_factor() {
  return static_cast<double>(get_n_buckets()) / get_n_keys();
}

template <class K, class V, class H>
void DistMap<K, V, H>::set_max_load_factor(const double max_load_factor) {
  this->max_load_factor = max_load_factor;
  local_map.set_max_load_factor(max_load_factor);
  for (auto& remote_map : remote_maps) remote_map.set_max_load_factor(max_load_factor);
}

template <class K, class V, class H>
size_t DistMap<K, V, H>::get_n_keys() {
  const size_t local_n_keys = local_map.get_n_keys();
  size_t n_keys;
  Parallel::reduce_sum(&local_n_keys, &n_keys, 1);
  return n_keys;
}

template <class K, class V, class H>
void DistMap<K, V, H>::async_set(
    const K& key, const V& value, const std::function<void(V&, const V&)>& reducer) {
  const size_t hash_value = hasher(key);
  const size_t dest_proc_id = hash_value % n_procs_cache;
  const size_t map_hash_value = hash_value / n_procs_cache;
  if (dest_proc_id == proc_id_cache) {
    local_map.set(key, map_hash_value, value, reducer);
  } else {
    remote_maps[dest_proc_id].set(key, map_hash_value, value, reducer);
  }
}

template <class K, class V, class H>
V DistMap<K, V, H>::get(const K& key, const V& default_value) {
  const size_t hash_value = hasher(key);
  const size_t dest_proc_id = hash_value % n_procs_cache;
  const size_t map_hash_value = hash_value / n_procs_cache;
  V res;
  if (dest_proc_id == proc_id_cache) {
    res = local_map.get(key, map_hash_value, default_value);
  }
  Parallel::broadcast(&res, 1, dest_proc_id);
  return res;
}

template <class K, class V, class H>
void DistMap<K, V, H>::clear() {
  local_map.clear();
  for (auto& remote_map : remote_maps) remote_map.clear();
}

template <class K, class V, class H>
void DistMap<K, V, H>::clear_and_shrink() {
  local_map.clear_and_shrink();
  for (auto& remote_map : remote_maps) remote_map.clear_and_shrink();
}

// template <class K, class V, class H>
// template <class KR, class VR, class HR>
// DistMap<KR, VR, HR> DistMap<K, V, H>::mapreduce(
//     const std::function<void(const K&, const V&, const std::function<void(const KR&, const
//     VR&)>&)>&
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
//     printf("MapReduce on %d node(s) (%d threads):\nMapping: ", n_procs, n_threads * n_procs);
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
//   local_map.all_node_apply(node_handler);
//   if (verbose && proc_id == 0) printf("Done\n");

//   res.sync(reducer, verbose);

//   return res;
// }

template <class K, class V, class H>
void DistMap<K, V, H>::sync(
    const std::function<void(V&, const V&)>& reducer, const bool verbose, const int trunk_size) {
  assert(trunk_size > 0);  // Avoid overflow.
  const int n_procs = Parallel::get_n_procs();
  const int proc_id = Parallel::get_proc_id();
  // printf("%d: Size: %d %d\n", proc_id, local_map.get_n_keys(), local_map.get("aa", hasher("aa")));

  if (verbose && Parallel::is_master()) printf("Syncing: ");

  DistHasher<K, H> dist_hasher;
  const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double) {
    // printf("%s %f\n", node->key.c_str(), node->value);
    local_map.set(node->key, dist_hasher(node->key), node->value, reducer);
  };
  std::string send_buf;
  std::string recv_buf;
  char recv_buf_char[trunk_size];
  const auto& shuffled_procs = generate_shuffled_procs();
  const int base_id = get_base_id(shuffled_procs);
  for (int i = 1; i < n_procs; i++) {
    const int dest_proc_id = shuffled_procs[(base_id + i) % n_procs];
    const int src_proc_id = shuffled_procs[(base_id + n_procs - i) % n_procs];
    send_buf = remote_maps[dest_proc_id].to_string();
    remote_maps[dest_proc_id].clear();
    size_t send_cnt = send_buf.size();
    size_t recv_cnt;
    Parallel::irecv(&recv_cnt, 1, src_proc_id);
    Parallel::isend(&send_cnt, 1, dest_proc_id);
    Parallel::wait_all();
    Parallel::barrier();
    size_t send_pos = 0;
    size_t recv_pos = 0;
    recv_buf.reserve(recv_cnt);
    while (send_pos < send_cnt || recv_pos < recv_cnt) {
      const int recv_trunk_cnt =
          (recv_cnt - recv_pos >= trunk_size) ? trunk_size : recv_cnt - recv_pos;
      const int send_trunk_cnt =
          (send_cnt - send_pos >= trunk_size) ? trunk_size : send_cnt - send_pos;
      if (recv_pos < recv_cnt) {
        printf("%d recving %d %d %d\n", proc_id, recv_pos, recv_trunk_cnt, recv_cnt);
        Parallel::irecv(&recv_buf_char[0], recv_trunk_cnt, src_proc_id);
        recv_pos += recv_trunk_cnt;
      }
      if (send_pos < send_cnt) {
        printf("%d sending %d %d %d\n", proc_id, send_pos, send_trunk_cnt, send_cnt);
        Parallel::issend(&send_buf[send_pos], send_trunk_cnt, dest_proc_id);
        send_pos += send_trunk_cnt;
      }
      Parallel::wait_all();
      recv_buf.append(recv_buf_char, recv_trunk_cnt);
    }
    remote_maps[dest_proc_id].from_string(recv_buf);
    remote_maps[dest_proc_id].all_node_apply(node_handler);
    remote_maps[dest_proc_id].clear_and_shrink();
  }
  Parallel::barrier();
  // printf("%d: Size: %d %d\n", proc_id, local_map.get_n_keys(), local_map.get("aa",
  // hasher("aa")));
  if (verbose && Parallel::is_master()) printf("Done\n");
}

// template <class K, class V, class H>
// void DistMap<K, V, H>::sync(
//     const std::function<void(V&, const V&)>& reducer, const bool verbose, const size_t
//     trunk_size) {
//   const int proc_id = Parallel::get_proc_id();
//   const int n_procs = Parallel::get_n_procs();

//   if (verbose && proc_id == 0) printf("Syncing: ");
//   fflush(stdout);

//   const auto& node_handler = [&](std::unique_ptr<HashNode<K, V>>& node, const double) {
//     local_map.set(node->key, local_map.get_hash_value(node->key), node->value, reducer);
//   };
//   std::string send_buf;
//   std::string recv_buf;
//   char* recv_buf_char = new char[trunk_size];
//   for (int i = 1; i < n_procs; i++) {
//     const int dest_proc_id = (proc_id + i) % n_procs;
//     const int src_proc_id = (proc_id + n_procs - i) % n_procs;
//     send_buf = remote_maps[dest_proc_id].to_string();
//     remote_maps[dest_proc_id].clear();
//     size_t send_cnt = send_buf.size();
//     size_t recv_cnt;
//     Parallel::isend(&send_cnt, 1, dest_proc_id);
//     Parallel::irecv(&recv_cnt, 1, src_proc_id);
//     Parallel::wait_all();
//     Parallel::barrier();
//     size_t send_pos = 0;
//     size_t recv_pos = 0;
//     recv_buf.resize(recv_cnt);
//     size_t trunk_tag = 0;
//     while (send_pos < send_cnt || recv_pos < recv_cnt) {
//       if (send_pos < send_cnt) {
//         const int send_trunk_cnt =
//             (send_cnt - send_pos >= trunk_size) ? trunk_size : send_cnt - send_pos;
//         printf("sending %d %d %d\n", send_pos, send_trunk_cnt, send_cnt);
//         Parallel::isend(&send_buf[send_pos], send_trunk_cnt, dest_proc_id, trunk_tag);
//         send_pos += send_trunk_cnt;
//       }
//       if (recv_pos < recv_cnt) {
//         const int recv_trunk_cnt =
//             (recv_cnt - recv_pos >= trunk_size) ? trunk_size : recv_cnt - recv_pos;
//         printf("recving %d %d %d\n", recv_pos, recv_trunk_cnt, recv_cnt);
//         Parallel::irecv(&recv_buf_char[recv_pos], recv_trunk_cnt, src_proc_id, trunk_tag);
//         // printf("recv cap: %zu\n", recv_buf.capacity());

//         // Parallel::irecv(&(recv_buf.at(recv_pos)), recv_trunk_cnt, src_proc_id, trunk_tag);
//         recv_pos += recv_trunk_cnt;
//         // printf("recv cap: %zu\n", recv_buf.capacity());
//       }
//       Parallel::wait_all();
//       trunk_tag++;
//     }
//     recv_buf.assign(recv_buf_char, recv_cnt);
//     remote_maps[dest_proc_id].from_string(recv_buf);
//     remote_maps[dest_proc_id].all_node_apply(node_handler);
//     remote_maps[dest_proc_id].clear_and_shrink();
//     printf("S:%d R:%d\n", send_cnt, recv_cnt);
//     if (verbose && proc_id == 0) printf("%d/%d ", i, n_procs);
//     Parallel::barrier();
//     // break;
//   }
//   if (verbose && proc_id == 0) printf("Done\n");
// }

template <class K, class V, class H>
std::vector<int> DistMap<K, V, H>::generate_shuffled_procs() {
  const int n_procs = Parallel::get_n_procs();
  std::vector<int> res(n_procs);

  if (Parallel::is_master()) {
    for (int i = 0; i < n_procs; i++) res[i] = i;
    srand(time(0));

    // Fisher–Yates shuffle algorithm.
    for (int i = res.size() - 1; i > 0; i--) {
      const int j = rand() % (i + 1);
      if (i != j) {
        const int tmp = res[i];
        res[i] = res[j];
        res[j] = tmp;
      }
    }
  }

  Parallel::broadcast(res.data(), n_procs);

  return res;
}

template <class K, class V, class H>
int DistMap<K, V, H>::get_base_id(const std::vector<int>& shuffled_procs) {
  const int n_procs = Parallel::get_n_procs();
  const int proc_id = Parallel::get_proc_id();
  for (int i = 0; i < n_procs; i++) {
    if (shuffled_procs[i] == proc_id) return i;
  }
  throw std::runtime_error("impossible state");
}

}  // namespace hpmr
