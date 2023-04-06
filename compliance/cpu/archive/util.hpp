#ifndef BLIMP_DB_CPU_UTIL_HPP
#define BLIMP_DB_CPU_UTIL_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace util {

template <typename F> double time(F &&f) {
  auto t0 = std::chrono::high_resolution_clock::now();
  f();
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double>(t1 - t0).count();
}

template <typename F> std::vector<double> time(size_t n, F &&f) {
  std::vector<double> t(n);
  for (size_t i = 0; i < n; ++i) {
    t[i] = time(f);
  }
  return t;
}

} // namespace util

#endif // BLIMP_DB_CPU_UTIL_HPP
