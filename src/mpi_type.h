#pragma once

#include <mpi.h>

namespace hpmr {
template <class T>
struct MpiType {};

template <>
struct MpiType<char> {
  constexpr static int value = MPI_CHAR;
};

template <>
struct MpiType<short> {
  constexpr static int value = MPI_SHORT;
};

template <>
struct MpiType<int> {
  constexpr static int value = MPI_INT;
};

template <>
struct MpiType<long> {
  constexpr static int value = MPI_LONG;
};

template <>
struct MpiType<long long> {
  constexpr static int value = MPI_LONG_LONG_INT;
};

template <>
struct MpiType<unsigned char> {
  constexpr static int value = MPI_UNSIGNED_CHAR;
};

template <>
struct MpiType<unsigned short> {
  constexpr static int value = MPI_UNSIGNED_SHORT;
};

template <>
struct MpiType<unsigned> {
  constexpr static int value = MPI_UNSIGNED;
};

template <>
struct MpiType<unsigned long> {
  constexpr static int value = MPI_UNSIGNED_LONG;
};

template <>
struct MpiType<unsigned long long> {
  constexpr static int value = MPI_UNSIGNED_LONG_LONG;
};

template <>
struct MpiType<float> {
  constexpr static int value = MPI_FLOAT;
};

template <>
struct MpiType<double> {
  constexpr static int value = MPI_DOUBLE;
};

template <>
struct MpiType<long double> {
  constexpr static int value = MPI_LONG_DOUBLE;
};
};  // namespace hpmr
