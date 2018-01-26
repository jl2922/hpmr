#include "concurrent_map.h"

#include <gtest/gtest.h>
#include <omp.h>
#include "reducer.h"

TEST(ConcurrentMapTest, Initialization) {
  hpmr::ConcurrentMap<std::string, int> m;
  EXPECT_EQ(m.get_n_keys(), 0);
}

TEST(ConcurrentMapTest, ReserveAndRehash) {
  // Explicit reserve tests.
  hpmr::ConcurrentMap<std::string, int> m;
  m.reserve(10);
  EXPECT_GE(m.get_n_buckets(), 10);

  // Automatic reserve tests.
  hpmr::ConcurrentMap<int, int> m2;
  for (int i = 0; i < 100; i++) {
    m2.set(i, i * i);
    EXPECT_EQ(m2.get_n_keys(), i + 1);
    EXPECT_GE(m2.get_n_buckets(), i + 1);
  }
}

TEST(ConcurrentMapTest, ParallelInsertAndRehash) {
  hpmr::ConcurrentMap<int, int> m;
  constexpr int N_KEYS = 1000;
  omp_set_nested(1);  // For parallel rehashing.
#pragma omp parallel for
  for (int i = 0; i < N_KEYS; i++) {
    m.set(i, i);
  }
  EXPECT_EQ(m.get_n_keys(), N_KEYS);
  EXPECT_GE(m.get_n_buckets(), N_KEYS);
}

TEST(ConcurrentMapTest, LargeReserve) {
  hpmr::ConcurrentMap<std::string, int> m;
  const size_t LARGE_N_BUCKETS = 1000000;
  m.reserve(LARGE_N_BUCKETS);
  const size_t n_buckets = m.get_n_buckets();
  EXPECT_GE(n_buckets, LARGE_N_BUCKETS);
}

TEST(ConcurrentMapTest, SetAndGet) {
  hpmr::ConcurrentMap<std::string, int> m;
  // Set with value.
  m.set("aa", 0);
  EXPECT_EQ(m.get("aa", 0), 0);
  m.set("aa", 1);
  EXPECT_EQ(m.get("aa", 0), 1);

  // Set with reducer function.
  m.set("aa", 2, hpmr::Reducer<int>::sum);
  EXPECT_EQ(m.get("aa"), 3);
  hpmr::ConcurrentMap<std::string, int> m2;
  m2.set("cc", 3, hpmr::Reducer<int>::sum);
  EXPECT_EQ(m2.get("cc"), 3);
}



// TEST(ConcurrentMapTest, Unset) {
//   hpmr::ConcurrentMap<std::string, int> m;
//   m.set("aa", 1);
//   m.set("bbb", 2);
//   m.unset("aa");
//   EXPECT_FALSE(m.has("aa"));
//   EXPECT_TRUE(m.has("bbb"));
//   EXPECT_EQ(m.get_n_keys(), 1);

//   m.unset("not_exist_key");
//   EXPECT_EQ(m.get_n_keys(), 1);

//   m.unset("bbb");
//   EXPECT_FALSE(m.has("aa"));
//   EXPECT_FALSE(m.has("bbb"));
//   EXPECT_EQ(m.get_n_keys(), 0);
// }

// TEST(ConcurrentMapTest, Map) {
//   hpmr::ConcurrentMap<std::string, int> m;
//   const auto& cubic = [&](const int value) { return value * value * value; };
//   m.set("aa", 5);
//   EXPECT_EQ(m.map<int>("aa", cubic, 0), 125);
//   EXPECT_EQ(m.map<int>("not_exist_key", cubic, 3), 3);
// }

// TEST(ConcurrentMapTest, Apply) {
//   hpmr::ConcurrentMap<std::string, int> m;
//   m.set("aa", 5);
//   m.set("bbb", 10);
//   int sum = 0;

//   // Apply to one key.
//   m.apply("aa", [&](const auto& value) { return sum += value; });
//   EXPECT_EQ(sum, 5);

//   // Apply to all the keys.
//   m.apply([&](const auto& key, const auto& value) {
//     if (key.front() == 'b') sum += value;
//   });
//   EXPECT_EQ(sum, 15);
// }

// TEST(ConcurrentMapTest, MapReduce) {
//   hpmr::ConcurrentMap<std::string, double> m;
//   m.set("aa", 1.1);
//   m.set("ab", 2.2);
//   m.set("ac", 3.3);
//   m.set("ad", 4.4);
//   m.set("ae", 5.5);
//   m.set("ba", 6.6);
//   m.set("bb", 7.7);
//   // Count the number of keys that start with 'a'.
//   const auto& initial_a_to_one = [&](const std::string& key, const auto value) {
//     (void)value;  // Prevent unused variable warning.
//     if (key.front() == 'a') return 1;
//     return 0;
//   };
//   const int initial_a_count = m.map_reduce<int>(initial_a_to_one, reducer::sum<int>, 0);
//   EXPECT_EQ(initial_a_count, 5);
// }

// TEST(OMPHashMapLargeTest, TenMillionsMapReduce) {
//   hpmr::ConcurrentMap<int, int> m;
//   constexpr int N_KEYS = 10000000;

//   m.reserve(N_KEYS);
// #pragma omp parallel for
//   for (int i = 0; i < N_KEYS; i++) {
//     m.set(i, i);
//   }
//   const auto& mapper = [&](const int key, const int value) {
//     (void)key;
//     return value;
//   };
//   const auto& sum = m.map_reduce<int>(mapper, reducer::max<int>, 0.0);
//   EXPECT_EQ(sum, N_KEYS - 1);
// }

// TEST(ConcurrentMapTest, Clear) {
//   hpmr::ConcurrentMap<std::string, int> m;
//   m.set("aa", 1);
//   m.set("bbb", 2);
//   m.clear();
//   EXPECT_FALSE(m.has("aa"));
//   EXPECT_FALSE(m.has("bbb"));
//   EXPECT_EQ(m.get_n_keys(), 0);
// }