#include "mpi.h"
#include "omp.h"

namespace hpmr {
class Parallel {
 public:
  static int get_proc_id() { return get_instance().proc_id; }

  static int get_n_procs() { return get_instance().n_procs; }

  static int get_n_threads() { return get_instance().n_threads; }

 private:
  int proc_id;

  int n_procs;

  int n_threads;

  Parallel();

  ~Parallel();

  static Parallel get_instance() {
    static Parallel instance;
    return instance;
  }
};

Parallel::Parallel() {
  int initialized;
  MPI_Initialized(&initialized);
  if (!initialized) {
    MPI_Init(nullptr, nullptr);
  }
  MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
  MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
  n_threads = omp_get_max_threads();
}

Parallel::~Parallel() {
  int finalized;
  MPI_Finalized(&finalized);
  if (!finalized) {
    MPI_Finalize();
  }
}

}  // namespace hpmr
