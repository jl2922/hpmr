#include "bare_concurrent_map.h"

#include <gtest/gtest.h>
#include "reducer.h"

TEST(BareConcurrentMapTest, Initialization) {
  hpmr::BareConcurrentMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareConcurrentMapTest, CopyConstructor) {
  hpmr::BareConcurrentMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"), 0);
  EXPECT_EQ(m.get("aa", hasher("aa")), 0);
  m.set("bb", hasher("aa"), 1);
  EXPECT_EQ(m.get("bb", hasher("aa")), 1);

  hpmr::BareConcurrentMap<std::string, int> m2(m);
  EXPECT_EQ(m2.get("aa", hasher("aa")), 0);
  EXPECT_EQ(m2.get("bb", hasher("aa")), 0);
}

TEST(BareConcurrentMapTest, Reserve) {
  hpmr::BareConcurrentMap<std::string, int> m;
  m.reserve(100);
  EXPECT_GE(m.get_n_buckets(), 100);
}

TEST(BareConcurrentMapTest, LargeReserve) {
  hpmr::BareConcurrentMap<std::string, int> m;
  const size_t LARGE_N_BUCKETS = 1000000;
  m.reserve(LARGE_N_BUCKETS);
  const size_t n_buckets = m.get_n_buckets();
  EXPECT_GE(n_buckets, LARGE_N_BUCKETS);
}

TEST(BareConcurrentMapTest, GetAndSetLoadFactor) {
  hpmr::BareConcurrentMap<int, int> m;
  constexpr int N_KEYS = 100;
  m.set_max_load_factor(0.5);
  EXPECT_EQ(m.get_max_load_factor(), 0.5);
  std::hash<int> hasher;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, hasher(i), i);
  }
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(BareConcurrentMapTest, SetAndGet) {
  hpmr::BareConcurrentMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"), 0);
  EXPECT_EQ(m.get("aa", hasher("aa")), 0);
  m.set("aa", hasher("aa"), 1);
  EXPECT_EQ(m.get("aa", hasher("aa"), 0), 1);
  m.set("aa", hasher("aa"), 2, hpmr::Reducer<int>::sum);
  EXPECT_EQ(m.get("aa", hasher("aa")), 3);
  m.set("cc", hasher("cc"), 3, hpmr::Reducer<int>::sum);
  m.get("cc", hasher("cc"), [](const int value) { EXPECT_EQ(value, 3); });
}

TEST(BareConcurrentMapTest, ParallelSetAndRehash) {
  hpmr::BareConcurrentMap<int, int> m;
  std::hash<int> hasher;
  constexpr int N_KEYS = 100;
#pragma omp parallel for
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, hasher(i), i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(BareConcurrentMapTest, UnsetAndHas) {
  hpmr::BareConcurrentMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"), 1);
  m.set("bbb", hasher("bbb"), 2);
  EXPECT_TRUE(m.has("aa", hasher("aa")));
  EXPECT_TRUE(m.has("bbb", hasher("bbb")));
  m.unset("aa", hasher("aa"));
  EXPECT_FALSE(m.has("aa", hasher("aa")));
  EXPECT_EQ(m.get_n_keys(), 1);

  m.unset("not_exist_key", hasher("not_exist_key"));
  EXPECT_EQ(m.get_n_keys(), 1);

  m.unset("bbb", hasher("bbb"));
  EXPECT_FALSE(m.has("aa", hasher("aa")));
  EXPECT_FALSE(m.has("bbb", hasher("bbb")));
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareConcurrentMapTest, Clear) {
  hpmr::BareConcurrentMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"), 1);
  m.set("bbb", hasher("bbb"), 2);
  EXPECT_EQ(m.get_n_keys(), 2);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareConcurrentMapTest, ClearAndShrink) {
  hpmr::BareConcurrentMap<int, int> m;
  std::hash<int> hasher;
  constexpr int N_KEYS = 100;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, hasher(i), i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS * m.get_max_load_factor());
  m.clear_and_shrink();
  EXPECT_EQ(m.get_n_keys(), 0);
  EXPECT_LT(m.get_n_buckets(), N_KEYS * m.get_max_load_factor());
}
