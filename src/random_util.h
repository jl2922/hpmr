#pragma once

#include <cstdlib>
#include <type_traits>
#include <vector>

class RandomUtil {
 public:
  template <class T>
  void shuffle(std::vector<T>& vec);
};

template <class T>
void RandomUtil::shuffle(std::vector<T>& vec) {
  // Fisher–Yates shuffle algorithm.
  for (size_t i = vec.size() - 1; i > 0; i--) {
    const size_t j = rand() % (i + 1);
    if (i != j) {
      const T tmp = vec[i];
      vec[i] = vec[j];
      vec[j] = tmp;
    }
  }
}
