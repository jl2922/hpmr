#include "bare_async_map.h"

#include <gtest/gtest.h>
#include "reducer.h"

TEST(BareAsyncMapTest, Initialization) {
  hpmr::BareAsyncMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareAsyncMapTest, CopyConstructor) {
  hpmr::BareAsyncMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.async_set("aa", hasher("aa"), 0);
  m.async_set("bb", hasher("bb"), 1);
  m.sync();
  EXPECT_EQ(m.get("aa", hasher("aa")), 0);
  EXPECT_EQ(m.get("bb", hasher("bb")), 1);

  hpmr::BareAsyncMap<std::string, int> m2(m);
  EXPECT_EQ(m2.get("aa", hasher("aa")), 0);
  EXPECT_EQ(m2.get("bb", hasher("bb")), 1);
}

TEST(BareAsyncMapTest, Reserve) {
  hpmr::BareAsyncMap<std::string, int> m;
  m.reserve(100);
  EXPECT_GE(m.get_n_buckets(), 100);
}

TEST(BareAsyncMapTest, LargeReserve) {
  hpmr::BareAsyncMap<std::string, int> m;
  const size_t LARGE_N_BUCKETS = 1000000;
  m.reserve(LARGE_N_BUCKETS);
  const size_t n_buckets = m.get_n_buckets();
  EXPECT_GE(n_buckets, LARGE_N_BUCKETS);
}

TEST(BareAsyncMapTest, GetAndSetLoadFactor) {
  hpmr::BareAsyncMap<int, int> m;
  constexpr int N_KEYS = 100;
  m.set_max_load_factor(0.5);
  EXPECT_EQ(m.get_max_load_factor(), 0.5);
  std::hash<int> hasher;
  for (int i = 0; i < N_KEYS; i++) {
    m.async_set(i, hasher(i), i);
  }
  m.sync();
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(BareAsyncMapTest, SetAndGet) {
  hpmr::BareAsyncMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.async_set("aa", hasher("aa"), 0);
  m.async_set("aa", hasher("aa"), 1, hpmr::Reducer<int>::sum);
  m.async_set("cc", hasher("cc"), 2, hpmr::Reducer<int>::sum);
  m.sync();
  EXPECT_EQ(m.get("aa", hasher("aa")), 1);
  EXPECT_EQ(m.get("cc", hasher("cc")), 2);
}

TEST(BareAsyncMapTest, LargeSetAndGet) {
  hpmr::BareAsyncMap<int, int> m;
  constexpr int N_KEYS = 1000000;
  m.reserve(N_KEYS);
  std::hash<int> hasher;
  for (int i = 0; i < N_KEYS; i++) {
    m.async_set(i, hasher(i), i);
  }
  m.sync();
}

TEST(BareAsyncMapTest, LargeParallelSetAndGet) {
  hpmr::BareAsyncMap<int, int> m;
  constexpr int N_KEYS = 1000000;
  m.reserve(N_KEYS);
  std::hash<int> hasher;
#pragma omp parallel for
  for (int i = 0; i < N_KEYS; i++) {
    m.async_set(i, hasher(i), i);
  }
  m.sync();
}