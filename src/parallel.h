#pragma once

#include <mpi.h>
#include <omp.h>
#include <string>
#include <vector>
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
  static void isend(
      const T* data, const int count, const int dest_proc_id, const int tag = DEFAULT_TAG);

  template <class T>
  static void irecv(T* data, const int count, const int src_proc_id, const int tag = DEFAULT_TAG);

  static void wait_all() {
    MPI_Status status;
    for (auto& req : get_instance().reqs) MPI_Wait(&req, &status);
    get_instance().reqs.clear();
  }

 private:
  int proc_id;

  int n_procs;

  int n_threads;

  std::vector<MPI_Request> reqs;

  constexpr static int DEFAULT_TAG = 0;

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
void Parallel::isend(const T* data, const int count, const int dest_proc_id, const int tag) {
  MPI_Request req;
  MPI_Isend(data, count, MpiTypeUtil::get_type(T()), dest_proc_id, tag, MPI_COMM_WORLD, &req);
  get_instance().reqs.push_back(std::move(req));
}

template <class T>
void Parallel::irecv(T* data, const int count, const int src_proc_id, const int tag) {
  MPI_Request req;
  MPI_Irecv(data, count, MpiTypeUtil::get_type(T()), src_proc_id, tag, MPI_COMM_WORLD, &req);
  get_instance().reqs.push_back(std::move(req));
}

}  // namespace hpmr
