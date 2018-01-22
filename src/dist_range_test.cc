#include "dist_range.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

TEST(DistRangeTest, MapTest) {
  DistRange range(1, 10);

  std::vector<bool> visited(10, false);
  auto mapper = [&](const long long val, const std::function<void(const int, const int)>& emit) {
    (void)emit;
    EXPECT_THAT(val, testing::Ge(1));
    EXPECT_THAT(val, testing::Le(10));
    printf("called %d", val);
    visited[val - 1] = true;
  };

  for (const auto& val : visited) {
    EXPECT_TRUE(val);
  }

  range.map<int, int, std::hash<int>>(mapper);
}
