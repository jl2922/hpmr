#include "range.h"

#include <gtest/gtest.h>
#include "reducer.h"

TEST(RangeTest, MapReduceTest) {
  const int N_KEYS = 100000;
  hpmr::Range<int> range(0, N_KEYS);
  const auto& mapper = [](const int id, const std::function<void(const int, const bool)>& emit) {
    EXPECT_GE(id, 0);
    EXPECT_LT(id, N_KEYS);
    emit(id, false);
  };
  auto dist_map = range.mapreduce<int, bool>(mapper, hpmr::Reducer<bool>::keep);
  EXPECT_EQ(dist_map.get_n_keys(), N_KEYS);
}
