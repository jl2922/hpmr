#pragma once

#include <mpi.h>
#include <omp.h>
#include <string>
#include "mpi_type_util.h"

namespace hpmr {
class Parallel {
 public:
  static int get_proc_id() { return get_instance().proc_id; }

  static int get_n_procs() { return get_instance().n_procs; }

  static int get_thread_id() { return omp_get_thread_num(); }

  static int get_n_threads() { return get_instance().n_threads; }

  template <class T>
  static T reduce_sum(const T& t);

  template <class T>
  static T reduce_max(const T& t);

  template <class T>
  static T reduce_min(const T& t);

  template <class T>
  static void broadcast(T& t, const int src_proc_id);

  template <class T>
  static void circular_shift(
      const T* send_buf,
      const size_t send_cnt,
      T* recv_buf,
      const size_t recv_cnt,
      const int n_shifts,
      const size_t trunk_size = DEFAULT_TRUNK_SIZE);

 private:
  int proc_id;

  int n_procs;

  int n_threads;

  constexpr static size_t DEFAULT_TRUNK_SIZE = 1 << 25;

  Parallel() {
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
    n_threads = omp_get_max_threads();
  }

  static Parallel get_instance() {
    static Parallel instance;
    return instance;
  }
};

template <class T>
T Parallel::reduce_sum(const T& t) {
  T res;
  MPI_Allreduce(&t, &res, 1, MpiTypeUtil::get_type(t), MPI_SUM, MPI_COMM_WORLD);
  return res;
}

template <class T>
T Parallel::reduce_max(const T& t) {
  T res;
  MPI_Allreduce(&t, &res, 1, MpiTypeUtil::get_type(t), MPI_MAX, MPI_COMM_WORLD);
  return res;
}

template <class T>
T Parallel::reduce_min(const T& t) {
  T res;
  MPI_Allreduce(&t, &res, 1, MpiTypeUtil::get_type(t), MPI_SUM, MPI_COMM_WORLD);
  return res;
}

template <class T>
void Parallel::broadcast(T& t, const int src_proc_id) {
  MPI_Bcast(&t, 1, MpiTypeUtil::get_type(t), src_proc_id, MPI_COMM_WORLD);
}

template <class T>
static void Parallel::circular_shift(
    const T* send_buf,
    const size_t send_cnt,
    T* recv_buf,
    const size_t recv_cnt,
    const int n_shifts,
    const size_t trunk_size = DEFAULT_TRUNK_SIZE) {
  size_t send_pos = 0;
  size_t recv_pos = 0;
  const int proc_id = get_proc_id();
  const int n_procs = get_n_procs();
  const int dest_proc_id = (proc_id + n_shifts) % n_procs;
  const int src_proc_id = (proc_id - n_shifts) % n_procs;
  MPI_Request reqs[2];
  std::array<bool, 2> pending()
  while (send_pos < send_cnt || recv_pos < recv_cnt) {
    if (send_pos < send_cnt) {
      MPI_Isend();
    }
    if (recv_pos < recv_cnt) {
      MPIIrecv
    }
  }
}

}  // namespace hpmr
