#include "dist_map.h"

#include <gtest/gtest.h>
#include <omp.h>
#include "reducer.h"

TEST(DistMapTest, Initialization) {
  hpmr::DistMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}
