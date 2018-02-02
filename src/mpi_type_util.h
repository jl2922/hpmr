#pragma once

#include <mpi.h>

namespace hpmr {

class MpiTypeUtil {
 public:
  static MPI_Datatype get_type(const signed char&) { return MPI_CHAR; }
  static MPI_Datatype get_type(const signed short int&) { return MPI_SHORT; }
  static MPI_Datatype get_type(const signed int&) { return MPI_INT; }
  static MPI_Datatype get_type(const signed long int&) { return MPI_LONG; }
  static MPI_Datatype get_type(const signed long long int&) { return MPI_LONG_LONG_INT; }
  static MPI_Datatype get_type(const unsigned char&) { return MPI_UNSIGNED_CHAR; }
  static MPI_Datatype get_type(const unsigned short int&) { return MPI_UNSIGNED_SHORT; }
  static MPI_Datatype get_type(const unsigned int&) { return MPI_UNSIGNED; }
  static MPI_Datatype get_type(const unsigned long int&) { return MPI_UNSIGNED_LONG; }
  static MPI_Datatype get_type(const unsigned long long int&) { return MPI_UNSIGNED_LONG_LONG; }
  static MPI_Datatype get_type(const float&) { return MPI_FLOAT; }
  static MPI_Datatype get_type(const double&) { return MPI_DOUBLE; }
  static MPI_Datatype get_type(const long double&) { return MPI_LONG_DOUBLE; }
};

};  // namespace hpmr
