#ifndef HPMR_PARALLEL_H
#define HPMR_PARALLEL_H

#include <mpi.h>
#include <omp.h>
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

 private:
  int proc_id;

  int n_procs;

  int n_threads;

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

}  // namespace hpmr

#endif
