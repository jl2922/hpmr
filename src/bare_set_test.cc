#include "bare_set.h"

#include <gtest/gtest.h>
#include <unordered_set>
#include "reducer.h"

TEST(BareSetTest, Initialization) {
  hpmr::BareSet<std::string> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareSetTest, CopyConstructor) {
  hpmr::BareSet<std::string> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"));
  EXPECT_TRUE(m.has("aa", hasher("aa")));
  m.set("bb", hasher("bb"));
  EXPECT_TRUE(m.has("bb", hasher("bb")));

  hpmr::BareSet<std::string> m2(m);
  EXPECT_TRUE(m2.has("aa", hasher("aa")));
  EXPECT_TRUE(m2.has("bb", hasher("bb")));
}

TEST(BareSetTest, Reserve) {
  hpmr::BareSet<std::string> m;
  m.reserve(100);
  EXPECT_GE(m.get_n_buckets(), 100);
}

TEST(BareSetTest, LargeReserve) {
  hpmr::BareSet<std::string> m;
  const int N_KEYS = 1000000;
  m.reserve(N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(BareSetTest, MaxLoadFactorAndAutoRehash) {
  hpmr::BareSet<int> m;
  constexpr int N_KEYS = 100;
  m.max_load_factor = 0.5;
  std::hash<int> hasher;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, hasher(i));
  }
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(BareSetTest, SetAndHas) {
  hpmr::BareSet<std::string> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"));
  EXPECT_TRUE(m.has("aa", hasher("aa")));
  m.set("aa", hasher("aa"));
  EXPECT_TRUE(m.has("aa", hasher("aa")));
  m.set("cc", hasher("cc"));
  EXPECT_TRUE(m.has("cc", hasher("cc")));
}

TEST(BareSetTest, LargeSetAndHasSTLComparison) {
  constexpr long long N_KEYS = 1000000;
  std::unordered_set<long long> m;
  m.reserve(N_KEYS);
  for (long long i = 0; i < N_KEYS; i++) m.insert(i * i);
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.count(i * i), 1);
}

TEST(BareSetTest, LargeSetAndHas) {
  hpmr::BareSet<long long> m;
  constexpr long long N_KEYS = 1000000;
  m.reserve(N_KEYS);
  std::hash<long long> hasher;
  for (long long i = 0; i < N_KEYS; i++) m.set(i * i, hasher(i * i));
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_TRUE(m.has(i * i, hasher(i * i)));
}

TEST(BareSetTest, LargeAutoRehashSetAndHasSTLComparison) {
  constexpr int N_KEYS = 1000000;
  std::unordered_set<int> m;
  for (int i = 0; i < N_KEYS; i++) m.insert(i * i);
  for (int i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.count(i * i), 1);
}

TEST(BareSetTest, LargeAutoRehashSetAndHas) {
  hpmr::BareSet<int> m;
  constexpr int N_KEYS = 1000000;
  std::hash<int> hasher;
  for (int i = 0; i < N_KEYS; i++) m.set(i * i, hasher(i * i));
  for (int i = 0; i < N_KEYS; i += 10) EXPECT_TRUE(m.has(i * i, hasher(i * i)));
}

TEST(BareSetTest, UnsetAndHas) {
  hpmr::BareSet<std::string> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"));
  m.set("bbb", hasher("bbb"));
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

  hpmr::BareSet<int> m2;
  constexpr int N_KEYS = 100;
  m2.max_load_factor = 0.99;
  m2.reserve(N_KEYS);
  std::hash<int> hasher2;
  for (int i = 0; i < N_KEYS; i++) {
    m2.set(i * i, hasher2(i * i));
  }
  for (int i = 0; i < N_KEYS; i += 3) {
    m2.unset(i * i, hasher2(i * i));
  }
  for (int i = 0; i < N_KEYS; i++) {
    if (i % 3 == 0) {
      EXPECT_FALSE(m2.has(i * i, hasher2(i * i)));
    } else {
      EXPECT_TRUE(m2.has(i * i, hasher2(i * i)));
    }
  }
}

TEST(BareSetTest, Clear) {
  hpmr::BareSet<std::string> m;
  std::hash<std::string> hasher;
  m.set("aa", hasher("aa"));
  m.set("bbb", hasher("bbb"));
  EXPECT_EQ(m.get_n_keys(), 2);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(BareSetTest, ClearAndShrink) {
  hpmr::BareSet<int> m;
  std::hash<int> hasher;
  constexpr int N_KEYS = 100;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, hasher(i));
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS * m.max_load_factor);
  m.clear_and_shrink();
  EXPECT_EQ(m.get_n_keys(), 0);
  EXPECT_LT(m.get_n_buckets(), N_KEYS * m.max_load_factor);
}

TEST(BareSetTest, ToAndFromString) {
  hpmr::BareSet<std::string> m1;
  std::hash<std::string> hasher;
  m1.set("aa", hasher("aa"));
  m1.set("bbb", hasher("bbb"));
  const std::string serialized = hps::serialize_to_string(m1);
  hpmr::BareSet<std::string> m2;
  hps::parse_from_string(m2, serialized);
  EXPECT_EQ(m2.get_n_keys(), 2);
  EXPECT_TRUE(m2.has("aa", hasher("aa")));
  EXPECT_TRUE(m2.has("bbb", hasher("bbb")));
}
