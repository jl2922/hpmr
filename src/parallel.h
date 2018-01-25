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

 private:
  int proc_id;

  int n_procs;

  int n_threads;

  Parallel();

  static Parallel get_instance() {
    static Parallel instance;
    return instance;
  }
};

Parallel::Parallel() {
  MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
  n_threads = omp_get_max_threads();
}

template <class T>
T Parallel::reduce_sum(const T& t) {
  T res;
  MPI_Allreduce(&t, &res, 1, MpiTypeUtil::get_type(t), MPI_SUM, MPI_COMM_WORLD);
  return res;
}

}  // namespace hpmr

#endif
