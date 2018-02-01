#include "dist_map.h"

#include <gtest/gtest.h>
#include <omp.h>
#include "reducer.h"

TEST(DistMapTest, Initialization) {
  hpmr::DistMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(DistMapTest, Reserve) {
  hpmr::DistMap<std::string, int> m;
  m.reserve(100);
  EXPECT_GE(m.get_n_buckets(), 100);
}

TEST(DistMapTest, GetAndSetLoadFactor) {
  hpmr::DistMap<int, int> m;
  constexpr int N_KEYS = 100;
  m.set_max_load_factor(0.5);
  EXPECT_EQ(m.get_max_load_factor(), 0.5);
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, i);
  }
  m.sync();
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}
