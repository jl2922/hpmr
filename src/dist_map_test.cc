#include "dist_map.h"

#include <gtest/gtest.h>
#include "concurrent_map_impl.h"
#include "parallel.h"
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

TEST(DistMapTest, LargeReserve) {
  hpmr::DistMap<std::string, int> m;
  const size_t LARGE_N_BUCKETS = 1000000;
  m.reserve(LARGE_N_BUCKETS);
  const size_t n_buckets = m.get_n_buckets();
  EXPECT_GE(n_buckets, LARGE_N_BUCKETS);
}

TEST(DistMapTest, SetAndGet) {
  hpmr::DistMap<std::string, int> m;
  m.set("aa", 0);
  EXPECT_EQ(m.get("aa"), 0);
  m.set("aa", 1);
  EXPECT_EQ(m.get("aa", 0), 1);
  m.set("aa", 2, hpmr::Reducer<int>::sum);
  EXPECT_EQ(m.get("aa"), 3);

  hpmr::DistMap<std::string, int> m2;
  m2.set("cc", 3, hpmr::Reducer<int>::sum);
  EXPECT_EQ(m2.get("cc"), 3);
}

TEST(DistMapTest, CopyConstructor) {
  hpmr::DistMap<std::string, int> m;
  m.set("aa", 0);
  EXPECT_EQ(m.get("aa"), 0);
  m.set("bb", 1);
  EXPECT_EQ(m.get("bb"), 1);

  hpmr::DistMap<std::string, int> m2(m);
  EXPECT_EQ(m2.get("aa"), 0);
  EXPECT_EQ(m2.get("bb"), 0);
}

TEST(DistMapTest, Clear) {
  hpmr::DistMap<std::string, int> m;
  m.set("aa", 1);
  m.set("bbb", 2);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
}
