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

HashSet<uint32_t> hash_set_build(uint32_t sel, const std::vector<uint32_t> &b_k,
                                 const std::vector<uint32_t> &b_100) {
  HashSet<uint32_t> hash_set((uint32_t)(sel / 100.0 * (double)b_k.size()));
  for (uint32_t i = 0; i < b_k.size(); ++i) {
    if (b_100[i] < sel) {
      hash_set.insert(b_k[i]);
    }
  }

  return hash_set;
}

HashMap<uint32_t, uint32_t> hash_map_build(uint32_t sel,
                                           const std::vector<uint32_t> &b_k,
                                           const std::vector<uint32_t> &b_100,
                                           const std::vector<uint32_t> &b_10) {
  HashMap<uint32_t, uint32_t> hash_map(
      (uint32_t)(sel / 100.0 * (double)b_k.size()));
  for (uint32_t i = 0; i < b_k.size(); ++i) {
    if (b_100[i] < sel) {
      hash_map.insert(b_k[i], b_10[i]);
    }
  }

  return hash_map;
}

void sq3(size_t trials, uint32_t sel, const std::vector<uint32_t> &a_b_k,
         const std::vector<uint32_t> &a_100, const std::vector<uint32_t> &b_k,
         const std::vector<uint32_t> &b_100) {
  HashSet<uint32_t> hash_set = hash_set_build(sel, b_k, b_100);

  std::vector<double> t_sq3 = util::time(trials, [&] {
    tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, a_b_k.size()), 0,
        [&](const tbb::blocked_range<size_t> &r, uint32_t acc) {
          for (size_t i = r.begin(); i < r.end(); ++i) {
            if (hash_set.fetch(a_b_k[i])) {
              acc += a_100[i];
            }
          }
          return acc;
        },
        std::plus<>());
  });

  for (double t : t_sq3) {
    std::cout << t << ' ';
  }
  std::cout << std::endl;
}

std::vector<uint32_t> sq4_hash_set_probe(const std::vector<uint32_t> &a_b_k,
                                         HashSet<uint32_t> &hash_set) {
  std::vector<uint32_t> hit_map(a_b_k.size() / 32 + (a_b_k.size() % 32 != 0));
  tbb::parallel_for(size_t(0), a_b_k.size() / 32, [&](size_t i) {
    size_t j = i * 32;
    uint32_t mask = 0;
    for (size_t k = 0; k < 32; ++k) {
      uint32_t value_a_b_k = a_b_k[j + k];
      uint32_t hit = hash_set.fetch(value_a_b_k);
      mask |= hit << k;
    }
    hit_map[i] = mask;
  });

  for (size_t j = a_b_k.size() / 32 * 32; j < a_b_k.size(); ++j) {
    uint32_t value_a_b_k = a_b_k[j];
    uint32_t hit = hash_set.fetch(value_a_b_k);
    hit_map[j / 32] |= hit << (j % 32);
  }

  return hit_map;
}

std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
sq4_hash_map_probe_emit_hit_map(const std::vector<uint32_t> &a_b_k,
                                HashMap<uint32_t, uint32_t> &hash_map) {
  std::vector<uint32_t> hit_map(a_b_k.size() / 32 + (a_b_k.size() % 32 != 0));
  std::vector<uint32_t> payloads;
  for (size_t i = 0; i < a_b_k.size(); ++i) {
    uint32_t value_a_b_k = a_b_k[i];
    uint32_t *value_b_10 = hash_map.fetch(value_a_b_k);
    if (value_b_10) {
      hit_map[i / 32] |= 1 << (i % 32);
      payloads.push_back(*value_b_10);
    }
  }

  return {hit_map, payloads};
}

std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
sq4_hash_map_probe_emit_indices(const std::vector<uint32_t> &a_b_k,
                                HashMap<uint32_t, uint32_t> &hash_map) {
  std::vector<uint32_t> indices;
  std::vector<uint32_t> payloads;
  for (size_t i = 0; i < a_b_k.size(); ++i) {
    uint32_t value_a_b_k = a_b_k[i];
    uint32_t *value_b_10 = hash_map.fetch(value_a_b_k);
    if (value_b_10) {
      indices.push_back(i);
      payloads.push_back(*value_b_10);
    }
  }

  return {indices, payloads};
}

void sq4_normal(size_t trials, uint32_t sel, const std::vector<uint32_t> &a_b_k,
                const std::vector<uint32_t> &a_10,
                const std::vector<uint32_t> &b_k,
                const std::vector<uint32_t> &b_10,
                const std::vector<uint32_t> &b_100) {
  HashMap<uint32_t, uint32_t> hash_map = hash_map_build(sel, b_k, b_100, b_10);

  std::vector<double> t_sq4 = util::time(trials, [&] {
    tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, a_b_k.size()), std::vector<uint32_t>(10),
        [&](const tbb::blocked_range<size_t> &r, std::vector<uint32_t> acc) {
          for (size_t i = r.begin(); i != r.end(); ++i) {
            uint32_t value_a_b_k = a_b_k[i];
            uint32_t value_a_10 = a_10[i];
            uint32_t *value_b_10 = hash_map.fetch(value_a_b_k);
            if (value_b_10) {
              acc[*value_b_10] += value_a_10;
            }
          }

          return acc;
        },
        [](std::vector<uint32_t> acc1, const std::vector<uint32_t> &acc2) {
          for (size_t i = 0; i < 10; ++i) {
            acc1[i] += acc2[i];
          }
          return acc1;
        });
  });

  for (double t : t_sq4) {
    std::cout << t << ' ';
  }
  std::cout << std::endl;
}

void sq4_semijoin(size_t trials, uint32_t sel,
                  const std::vector<uint32_t> &a_b_k,
                  const std::vector<uint32_t> &a_10,
                  const std::vector<uint32_t> &b_k,
                  const std::vector<uint32_t> &b_10,
                  const std::vector<uint32_t> &b_100) {
  HashMap<uint32_t, uint32_t> hash_map = hash_map_build(sel, b_k, b_100, b_10);
  HashSet<uint32_t> hash_set = hash_set_build(sel, b_k, b_100);
  std::vector<uint32_t> hit_map = sq4_hash_set_probe(a_b_k, hash_set);

  std::vector<double> t_sq4 = util::time(trials, [&] {
    tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, hit_map.size()),
        std::vector<uint32_t>(10),
        [&](const tbb::blocked_range<size_t> &r, std::vector<uint32_t> acc) {
          for (size_t i = r.begin(); i != r.end(); ++i) {
            size_t j = i * 32;
            uint32_t mask = hit_map[i];
            while (mask != 0) {
              size_t k = __builtin_ctz(mask);
              uint32_t value_a_b_k = a_b_k[j + k];
              uint32_t value_a_10 = a_10[j + k];
              uint32_t value_b_10 = *hash_map.fetch(value_a_b_k);
              acc[value_b_10] += value_a_10;
              mask ^= (uint32_t(1) << k);
            }
          }

          return acc;
        },
        [](std::vector<uint32_t> acc1, const std::vector<uint32_t> &acc2) {
          for (size_t i = 0; i < 10; ++i) {
            acc1[i] += acc2[i];
          }
          return acc1;
        });
  });

  for (double t : t_sq4) {
    std::cout << t << ' ';
  }
  std::cout << std::endl;
}

void sq4_hit_map(size_t trials, uint32_t sel,
                 const std::vector<uint32_t> &a_b_k,
                 const std::vector<uint32_t> &a_10,
                 const std::vector<uint32_t> &b_k,
                 const std::vector<uint32_t> &b_10,
                 const std::vector<uint32_t> &b_100) {
  HashMap<uint32_t, uint32_t> hash_map = hash_map_build(sel, b_k, b_100, b_10);
  std::pair<std::vector<uint32_t>, std::vector<uint32_t>> probe_result =
      sq4_hash_map_probe_emit_hit_map(a_b_k, hash_map);

  std::vector<uint32_t> hit_map = probe_result.first;
  std::vector<uint32_t> payloads = probe_result.second;

  size_t payload_index = 0;

  std::vector<double> t_sq4 = util::time(trials, [&] {
    tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, hit_map.size()),
        std::vector<uint32_t>(10),
        [&](const tbb::blocked_range<size_t> &r, std::vector<uint32_t> acc) {
          for (size_t i = r.begin(); i != r.end(); ++i) {
            size_t j = i * 32;
            uint32_t mask = hit_map[i];
            while (mask != 0) {
              size_t k = __builtin_ctz(mask);
              uint32_t value_a_10 = a_10[j + k];
              acc[payloads[payload_index++]] += value_a_10;
              mask ^= (uint32_t(1) << k);
            }
          }

          return acc;
        },
        [](std::vector<uint32_t> acc1, const std::vector<uint32_t> &acc2) {
          for (size_t i = 0; i < 10; ++i) {
            acc1[i] += acc2[i];
          }
          return acc1;
        });
  });

  for (double t : t_sq4) {
    std::cout << t << ' ';
  }
  std::cout << std::endl;
}

void sq4_indices(size_t trials, uint32_t sel,
                 const std::vector<uint32_t> &a_b_k,
                 const std::vector<uint32_t> &a_10,
                 const std::vector<uint32_t> &b_k,
                 const std::vector<uint32_t> &b_10,
                 const std::vector<uint32_t> &b_100) {
  HashMap<uint32_t, uint32_t> hash_map = hash_map_build(sel, b_k, b_100, b_10);
  std::pair<std::vector<uint32_t>, std::vector<uint32_t>> probe_result =
      sq4_hash_map_probe_emit_indices(a_b_k, hash_map);

  std::vector<uint32_t> indices = probe_result.first;
  std::vector<uint32_t> payloads = probe_result.second;

  std::vector<double> t_sq4 = util::time(trials, [&] {
    tbb::parallel_reduce(
        tbb::blocked_range<size_t>(0, indices.size()),
        std::vector<uint32_t>(10),
        [&](const tbb::blocked_range<size_t> &r, std::vector<uint32_t> acc) {
          for (size_t i = r.begin(); i != r.end(); ++i) {
            acc[payloads[i]] += a_10[indices[i]];
          }

          return acc;
        },
        [](std::vector<uint32_t> acc1, const std::vector<uint32_t> &acc2) {
          for (size_t i = 0; i < 10; ++i) {
            acc1[i] += acc2[i];
          }
          return acc1;
        });
  });

  for (double t : t_sq4) {
    std::cout << t << ' ';
  }
  std::cout << std::endl;
}

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

  sq3(trials, sel, a_b_k, a_100, b_k, b_100);
  sq4_normal(trials, sel, a_b_k, a_10, b_k, b_10, b_100);
  sq4_semijoin(trials, sel, a_b_k, a_10, b_k, b_10, b_100);
  sq4_hit_map(trials, sel, a_b_k, a_10, b_k, b_10, b_100);
  sq4_indices(trials, sel, a_b_k, a_10, b_k, b_10, b_100);

  return 0;
}
