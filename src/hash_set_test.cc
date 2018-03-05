#include "hash_set.h"

#include <gtest/gtest.h>
#include <unordered_set>
#include "reducer.h"

TEST(HashSetTest, Initialization) {
  hpmr::HashSet<std::string> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(HashSetTest, CopyConstructor) {
  hpmr::HashSet<std::string> m;
  m.set("aa");
  EXPECT_TRUE(m.has("aa"));
  m.set("bb");
  EXPECT_TRUE(m.has("bb"));

  hpmr::HashSet<std::string> m2(m);
  EXPECT_TRUE(m2.has("aa"));
  EXPECT_TRUE(m2.has("bb"));
}

TEST(HashSetTest, Reserve) {
  hpmr::HashSet<std::string> m;
  m.reserve(100);
  EXPECT_GE(m.get_n_buckets(), 100);
}

TEST(HashSetTest, LargeReserve) {
  hpmr::HashSet<std::string> m;
  const int N_KEYS = 1000000;
  m.reserve(N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(HashSetTest, MaxLoadFactorAndAutoRehash) {
  hpmr::HashSet<int> m;
  constexpr int N_KEYS = 100;
  m.max_load_factor = 0.5;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i);
  }
  EXPECT_GE(m.get_n_buckets(), N_KEYS / 0.5);
}

TEST(HashSetTest, SetAndHas) {
  hpmr::HashSet<std::string> m;
  m.set("aa");
  EXPECT_TRUE(m.has("aa"));
  m.set("aa");
  EXPECT_TRUE(m.has("aa"));
  m.set("cc");
  EXPECT_TRUE(m.has("cc"));
}

TEST(HashSetTest, LargeSetAndHasSTLComparison) {
  constexpr long long N_KEYS = 1000000;
  std::unordered_set<long long> m;
  m.reserve(N_KEYS);
  for (long long i = 0; i < N_KEYS; i++) m.insert(i * i);
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.count(i * i), 1);
}

TEST(HashSetTest, LargeSetAndHas) {
  hpmr::HashSet<long long> m;
  constexpr long long N_KEYS = 1000000;
  m.reserve(N_KEYS);
  for (long long i = 0; i < N_KEYS; i++) m.set(i * i);
  for (long long i = 0; i < N_KEYS; i += 10) EXPECT_TRUE(m.has(i * i));
}

TEST(HashSetTest, LargeAutoRehashSetAndHasSTLComparison) {
  constexpr int N_KEYS = 1000000;
  std::unordered_set<int> m;
  for (int i = 0; i < N_KEYS; i++) m.insert(i * i);
  for (int i = 0; i < N_KEYS; i += 10) EXPECT_EQ(m.count(i * i), 1);
}

TEST(HashSetTest, LargeAutoRehashSetAndHas) {
  hpmr::HashSet<int> m;
  constexpr int N_KEYS = 1000000;
  for (int i = 0; i < N_KEYS; i++) m.set(i * i);
  for (int i = 0; i < N_KEYS; i += 10) EXPECT_TRUE(m.has(i * i));
}

TEST(HashSetTest, UnsetAndHas) {
  hpmr::HashSet<std::string> m;
  m.set("aa");
  m.set("bbb");
  EXPECT_TRUE(m.has("aa"));
  EXPECT_TRUE(m.has("bbb"));
  m.unset("aa");
  EXPECT_FALSE(m.has("aa"));
  EXPECT_EQ(m.get_n_keys(), 1);

  m.unset("not_exist_key");
  EXPECT_EQ(m.get_n_keys(), 1);

  m.unset("bbb");
  EXPECT_FALSE(m.has("aa"));
  EXPECT_FALSE(m.has("bbb"));
  EXPECT_EQ(m.get_n_keys(), 0);

  hpmr::HashSet<int> m2;
  constexpr int N_KEYS = 100;
  m2.max_load_factor = 0.99;
  m2.reserve(N_KEYS);
  for (int i = 0; i < N_KEYS; i++) {
    m2.set(i * i);
  }
  for (int i = 0; i < N_KEYS; i += 3) {
    m2.unset(i * i);
  }
  for (int i = 0; i < N_KEYS; i++) {
    if (i % 3 == 0) {
      EXPECT_FALSE(m2.has(i * i));
    } else {
      EXPECT_TRUE(m2.has(i * i));
    }
  }
}

TEST(HashSetTest, Clear) {
  hpmr::HashSet<std::string> m;
  m.set("aa");
  m.set("bbb");
  EXPECT_EQ(m.get_n_keys(), 2);
  m.clear();
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(HashSetTest, ClearAndShrink) {
  hpmr::HashSet<int> m;
  constexpr int N_KEYS = 100;
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS * m.max_load_factor);
  m.clear_and_shrink();
  EXPECT_EQ(m.get_n_keys(), 0);
  EXPECT_LT(m.get_n_buckets(), N_KEYS * m.max_load_factor);
}

TEST(HashSetTest, ToAndFromString) {
  hpmr::HashSet<std::string> m1;
  m1.set("aa");
  m1.set("bbb");
  const std::string serialized = hps::serialize_to_string(m1);
  hpmr::HashSet<std::string> m2;
  hps::parse_from_string(m2, serialized);
  EXPECT_EQ(m2.get_n_keys(), 2);
  EXPECT_TRUE(m2.has("aa"));
  EXPECT_TRUE(m2.has("bbb"));
}
