#include "blimp_hash_map.hpp"

#include <cstddef>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

int main() {
  size_t num_items = 1000;

  std::mt19937 prng(0); // NOLINT(cert-msc51-cpp)
  std::uniform_int_distribution<uint32_t> dis;

  std::vector<std::pair<uint32_t, uint32_t>> items(num_items);
  for (std::pair<uint32_t, uint32_t> &item : items) {
    item = {dis(prng), dis(prng)};
  }

  BlimpHashMap m(1000);

  for (const auto &[key, value] : items) {
    m.insert(key, value);
  }

  for (const auto &[key, value] : items) {
    if (*m.get(key) != value) {
      throw std::runtime_error("wrong value");
    }
  }

  return 0;
}
