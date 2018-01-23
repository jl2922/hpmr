#include "dist_range.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

TEST(DistRangeTest, MapTest) {
  hpmr::DistRange<int> range(0, 10);

  std::vector<int> vec(10, 0);
  auto mapper = [&](const int id, const std::function<void(const int, const int)>& emit) {
    (void)emit;
    EXPECT_THAT(id, testing::Ge(0));
    EXPECT_THAT(id, testing::Lt(10));
    vec[id] = id;
  };

  range.map<int, int, std::hash<int>>(mapper);

  for (int i = 0; i < 10; i++) EXPECT_EQ(vec[i], i);
}
