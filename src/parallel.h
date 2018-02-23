#pragma once

#include <mpi.h>
#include <omp.h>
#include <vector>
#include "mpi_type.h"

namespace hpmr {
class Parallel {
 public:
  static int get_proc_id() { return get_instance().proc_id; }

  static int get_n_procs() { return get_instance().n_procs; }

  static int get_thread_id() { return omp_get_thread_num(); }

  static int get_n_threads() { return get_instance().n_threads; }

  static bool is_master() { return get_proc_id() == 0; }

  static void barrier() { MPI_Barrier(MPI_COMM_WORLD); }

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
}  // namespace hpmr
