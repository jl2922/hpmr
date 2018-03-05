#include "bare_map.h"

#include <gtest/gtest.h>
#include <unordered_map>
#include "reducer.h"

TEST(BareMapTest, Initialization) {
  hpmr::BareMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareMapTest, CopyConstructor) {
  hpmr::BareMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"), 0);
  EXPECT_EQ(m.get("aa", hasher("aa")), 0);
  m.set("bb", hasher("bb"), 1);
  EXPECT_EQ(m.get("bb", hasher("bb")), 1);

  hpmr::BareMap<std::string, int> m2(m);
  EXPECT_EQ(m2.get("aa", hasher("aa")), 0);
  EXPECT_EQ(m2.get("bb", hasher("bb")), 1);
}

TEST(BareMapTest, Reserve) {
  hpmr::BareMap<std::string, int> m;
  m.reserve(100);
  EXPECT_GE(m.get_n_buckets(), 100);
}

TEST(BareMapTest, LargeReserve) {
  hpmr::BareMap<std::string, int> m;
  const int N_KEYS = 1000000;
  m.reserve(N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(BareMapTest, MaxLoadFactorAndAutoRehash) {
  hpmr::BareMap<int, int> m;
  constexpr int N_KEYS = 100;
  m.max_load_factor = 0.5;
  std::hash<int> hasher;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, hasher(i), i);
  }
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(BareMapTest, SetAndGet) {
  hpmr::BareMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"), 0);
  EXPECT_EQ(m.get("aa", hasher("aa")), 0);
  m.set("aa", hasher("aa"), 1);
  EXPECT_EQ(m.get("aa", hasher("aa"), 0), 1);
  m.set("aa", hasher("aa"), 2, hpmr::Reducer<int>::sum);
  EXPECT_EQ(m.get("aa", hasher("aa")), 3);
  m.set("cc", hasher("cc"), 3, hpmr::Reducer<int>::sum);
  EXPECT_EQ(m.get("cc", hasher("cc")), 3);
}

TEST(BareMapTest, LargeSetAndGetSTLComparison) {
  constexpr long long N_KEYS = 1000000;
  std::unordered_map<long long, int> m;
  m.reserve(N_KEYS);
  for (long long i = 0; i < N_KEYS; i++) m[i * i] = i;
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m[i * i], i);
}

TEST(BareMapTest, LargeSetAndGet) {
  hpmr::BareMap<long long, int> m;
  constexpr long long N_KEYS = 1000000;
  m.reserve(N_KEYS);
  std::hash<long long> hasher;
  for (long long i = 0; i < N_KEYS; i++) m.set(i * i, hasher(i * i), i);
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.get(i * i, hasher(i * i)), i);
}

TEST(BareMapTest, LargeAutoRehashSetAndGetSTLComparison) {
  constexpr int N_KEYS = 1000000;
  std::unordered_map<int, int> m;
  for (int i = 0; i < N_KEYS; i++) m[i] = i;
  for (int i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m[i], i);
}

TEST(BareMapTest, LargeAutoRehashSetAndGet) {
  hpmr::BareMap<int, int> m;
  constexpr int N_KEYS = 1000000;
  std::hash<int> hasher;
  for (int i = 0; i < N_KEYS; i++) m.set(i, hasher(i), i);
  for (int i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.get(i, hasher(i)), i);
}

TEST(BareMapTest, UnsetAndHas) {
  hpmr::BareMap<std::string, int> m;
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

  hpmr::BareMap<int, int> m2;
  constexpr int N_KEYS = 100;
  m2.max_load_factor = 0.99;
  m2.reserve(N_KEYS);
  std::hash<int> hasher2;
  for (int i = 0; i < N_KEYS; i++) {
    m2.set(i * i, hasher2(i * i), i);
  }
  for (int i = 0; i < N_KEYS; i += 3) {
    m2.unset(i * i, hasher2(i * i));
  }
  for (int i = 0; i < N_KEYS; i++) {
    if (i % 3 == 0) {
      EXPECT_FALSE(m2.get(i * i, hasher2(i * i)));
    } else {
      EXPECT_TRUE(m2.get(i * i, hasher2(i * i)));
    }
  }
}

TEST(BareMapTest, Clear) {
  hpmr::BareMap<std::string, int> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"), 1);
  m.set("bbb", hasher("bbb"), 2);
  EXPECT_EQ(m.get_n_keys(), 2);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareMapTest, ClearAndShrink) {
  hpmr::BareMap<int, int> m;
  std::hash<int> hasher;
  constexpr int N_KEYS = 100;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, hasher(i), i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS * m.max_load_factor);
  m.clear_and_shrink();
  EXPECT_EQ(m.get_n_keys(), 0);
  EXPECT_LT(m.get_n_buckets(), N_KEYS * m.max_load_factor);
}

TEST(BareMapTest, ToAndFromString) {
  hpmr::BareMap<std::string, int> m1;
  std::hash<std::string> hasher;
  m1.set("aa", hasher("aa"), 1);
  m1.set("bbb", hasher("bbb"), 2);
  const std::string serialized = hps::serialize_to_string(m1);
  hpmr::BareMap<std::string, int> m2;
  hps::parse_from_string(m2, serialized);
  EXPECT_EQ(m2.get_n_keys(), 2);
  EXPECT_EQ(m2.get("aa", hasher("aa")), 1);
  EXPECT_EQ(m2.get("bbb", hasher("bbb")), 2);
}
