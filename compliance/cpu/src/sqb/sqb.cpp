#include "../../archive/util.hpp"

#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>

#include <oneapi/tbb.h>

template <typename K, typename V> class HashMap {
public:
  HashMap() : mask_(0) {}

  explicit HashMap(uint32_t capacity) {
    // Load factor is 0.5.
    capacity *= 2;

    // Increase the number of buckets to the next highest power of two.
    uint32_t augmented_capacity = 1;
    while (augmented_capacity < capacity) {
      augmented_capacity <<= 1;
    }

    data_ = std::vector<std::tuple<bool, K, V>>(augmented_capacity);
    mask_ = augmented_capacity - 1;
  }

  void insert(K key, V value) {
    uint32_t index = hash(key);

    size_t iteration;
    for (iteration = 0; iteration < data_.size(); ++iteration) {
      std::tuple<bool, K, V> &slot = data_[index];

      if (!std::get<0>(slot)) {
        break;
      }

      if (std::get<1>(slot) == key) {
        throw std::runtime_error("insertion failed (duplicate key)");
      }

      if (++index == data_.size()) {
        index = 0;
      }
    }

    if (iteration == data_.size()) {
      throw std::runtime_error("insertion failed (no unoccupied slots)");
    }

    std::tuple<bool, K, V> &slot = data_[index];
    std::get<0>(slot) = true;
    std::get<1>(slot) = key;
    std::get<2>(slot) = value;
  }

  V *fetch(K key) {
    uint32_t index = hash(key);

    size_t iteration;
    for (iteration = 0; iteration < data_.size(); ++iteration) {
      std::tuple<bool, K, V> &slot = data_[index];

      if (!std::get<0>(slot)) {
        break;
      }

      if (std::get<1>(slot) == key) {
        return &std::get<2>(slot);
      }

      if (++index == data_.size()) {
        index = 0;
      }
    }

    return nullptr;
  }

private:
  std::vector<std::tuple<bool, K, V>> data_;
  size_t mask_;

  uint32_t hash(K key) { return (3634946921 * key + 2096170329) & mask_; }
};

template <typename K> class HashSet {
public:
  HashSet() = default;

  explicit HashSet(uint32_t capacity) : hash_map_(capacity) {}

  void insert(K key) { hash_map_.insert(key, {}); }

  bool fetch(K key) { return hash_map_.fetch(key) != nullptr; }

private:
  struct Empty {};

  HashMap<K, Empty> hash_map_;
};

int main(int argc, char **argv) {
  std::string usage = "USAGE:\n"
                      "sqb NA NB SEL TRIALS\n"
                      "\tNA      number of rows in table A\n"
                      "\tNB      number of rows in table B\n"
                      "\tSEL     query selectivity as a percentage\n"
                      "\tTRIALS  number of trials for each query";
  if (argc != 5) {
    std::cerr << usage << std::endl;
    throw std::logic_error("expected 3 arguments.");
  }

  uint32_t n_a;
  uint32_t n_b;
  uint32_t sel;
  size_t trials;

  try {
    n_a = std::stoul(argv[1]);
    n_b = std::stoul(argv[2]);
    sel = std::stoul(argv[3]);
    trials = std::stoull(argv[4]);
  } catch (const std::invalid_argument &e) {
    std::cerr << usage << std::endl;
    throw std::logic_error("invalid argument.");
  }

  // Generate the data.
  std::minstd_rand prng(0); // NOLINT(cert-msc51-cpp)
  std::uniform_int_distribution<uint32_t> gen_b_k(0, n_b - 1);
  std::uniform_int_distribution<uint32_t> gen_10(0, 9);
  std::uniform_int_distribution<uint32_t> gen_100(0, 99);

  std::vector<uint32_t> a_k(n_a);
  std::vector<uint32_t> a_b_k(n_a);
  std::vector<uint32_t> a_10(n_a);
  std::vector<uint32_t> a_100(n_a);
  std::iota(a_k.begin(), a_k.end(), 0);
  std::shuffle(a_k.begin(), a_k.end(), prng);
  std::generate(a_b_k.begin(), a_b_k.end(), [&] { return gen_b_k(prng); });
  std::generate(a_10.begin(), a_10.end(), [&] { return gen_10(prng); });
  std::generate(a_100.begin(), a_100.end(), [&] { return gen_100(prng); });

  std::vector<uint32_t> b_k(n_b);
  std::vector<uint32_t> b_10(n_b);
  std::vector<uint32_t> b_100(n_b);
  std::iota(b_k.begin(), b_k.end(), 0);
  std::shuffle(b_k.begin(), b_k.end(), prng);
  std::generate(b_10.begin(), b_10.end(), [&] { return gen_10(prng); });
  std::generate(b_100.begin(), b_100.end(), [&] { return gen_100(prng); });

  // Run SQ3.
  HashSet<uint32_t> hash_set((uint32_t)(sel / 100.0 * n_b));
  for (uint32_t i = 0; i < n_b; ++i) {
    if (b_100[i] < sel) {
      hash_set.insert(b_k[i]);
    }
  }

  std::vector<double> t_sq3 = util::time(trials, [&] {
    uint32_t result = tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, n_a), 0,
        [&](const tbb::blocked_range<size_t> &r, uint32_t acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            if (hash_set.fetch(a_b_k[i])) {
              acc += a_100[i];
            }
          }
          return acc;
        },
        std::plus<>());

    std::cout << result << std::endl;
  });

  for (double t : t_sq3) {
    std::cout << t << ' ';
  }
  std::cout << std::endl;

  return 0;
}
