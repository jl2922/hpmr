#pragma once

#include <mpi.h>
#include <omp.h>
#include <vector>
#include "mpi_type_util.h"

namespace hpmr {
class Parallel {
 public:
  static int get_proc_id() { return get_instance().proc_id; }

  static int get_n_procs() { return get_instance().n_procs; }

  static int get_thread_id() { return omp_get_thread_num(); }

  static int get_n_threads() { return get_instance().n_threads; }

  static bool is_master() { return get_proc_id() == 0; }

  template <class T>
  static void reduce_sum(const T* data, T* res, const int count);

  template <class T>
  static void reduce_max(const T* data, T* res, const int count);

  template <class T>
  static void reduce_min(const T* data, T* res, const int count);

  template <class T>
  static void broadcast(T* data, const int count, const int src_proc_id);

  template <class T>
  static void isend(
      const T* data, const int count, const int dest_proc_id, const int tag = DEFAULT_TAG);

  template <class T>
  static void issend(
      const T* data, const int count, const int dest_proc_id, const int tag = DEFAULT_TAG);

  template <class T>
  static void irecv(T* data, const int count, const int src_proc_id, const int tag = DEFAULT_TAG);

  static void wait_all() {
    MPI_Status status;
    for (auto& req : get_instance().reqs) MPI_Wait(&req, &status);
    get_instance().reqs.clear();
  }

  static void barrier() { MPI_Barrier(MPI_COMM_WORLD); }

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

  template <class T>
  static void reduce(const T* data, T* res, const int count, MPI_Op op);
};

template <class T>
void Parallel::reduce_sum(const T* data, T* res, const int count) {
  reduce(data, res, count, MPI_SUM);
}

template <class T>
void Parallel::reduce_max(const T* data, T* res, const int count) {
  reduce(data, res, count, MPI_MAX);
}

template <class T>
void Parallel::reduce_min(const T* data, T* res, const int count) {
  reduce(data, res, count, MPI_MIN);
}

template <class T>
void Parallel::reduce(const T* data, T* res, const int count, MPI_Op op) {
  MPI_Allreduce(data, res, count, MpiTypeUtil::get_type(T()), op, MPI_COMM_WORLD);
}

template <class T>
void Parallel::broadcast(T* data, const int count, const int src_proc_id) {
  MPI_Bcast(data, count, MpiTypeUtil::get_type(T()), src_proc_id, MPI_COMM_WORLD);
}

template <class T>
void Parallel::isend(const T* data, const int count, const int dest_proc_id, const int tag) {
  MPI_Request req;
  MPI_Isend(data, count, MpiTypeUtil::get_type(T()), dest_proc_id, tag, MPI_COMM_WORLD, &req);
  get_instance().reqs.push_back(std::move(req));
}

template <class T>
void Parallel::issend(const T* data, const int count, const int dest_proc_id, const int tag) {
  MPI_Request req;
  MPI_Issend(data, count, MpiTypeUtil::get_type(T()), dest_proc_id, tag, MPI_COMM_WORLD, &req);
  get_instance().reqs.push_back(std::move(req));
}

template <class T>
void Parallel::irecv(T* data, const int count, const int src_proc_id, const int tag) {
  MPI_Request req;
  MPI_Irecv(data, count, MpiTypeUtil::get_type(T()), src_proc_id, tag, MPI_COMM_WORLD, &req);
  get_instance().reqs.push_back(std::move(req));
}

}  // namespace hpmr
