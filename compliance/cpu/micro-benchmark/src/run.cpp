#include <array>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <cxxopts.hpp>
#include <oneapi/tbb.h>

uint32_t num_a;
uint32_t num_b;

struct Database {
  std::vector<uint32_t> a_k, a_b_k, a_10, a_100, b_k, b_10, b_100;
};

Database db;

enum Format : int { BITMAP = 0, INDICES = 1, VALUES = 2 };

template <typename F> double clock(F &&f) {
  auto t0 = std::chrono::high_resolution_clock::now();
  f();
  auto t1 = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double>(t1 - t0).count();
}

void generate() {
  db.a_k.resize(num_a);
  db.a_b_k.resize(num_a);
  db.a_10.resize(num_a);
  db.a_100.resize(num_a);
  db.b_k.resize(num_b);
  db.b_10.resize(num_b);
  db.b_100.resize(num_b);

  std::random_device rd;
  std::minstd_rand rng(rd());

  std::uniform_int_distribution<uint32_t> dis_b_k(0, num_b - 1);
  std::uniform_int_distribution<uint32_t> dis_10(0, 9);
  std::uniform_int_distribution<uint32_t> dis_100(0, 99);

  for (uint32_t i = 0; i < num_a; ++i) {
    db.a_k[i] = i;
    db.a_b_k[i] = dis_b_k(rng);
    db.a_10[i] = dis_10(rng);
    db.a_100[i] = dis_100(rng);
  }

  for (uint32_t i = 0; i < num_b; ++i) {
    db.b_k[i] = i;
    db.b_10[i] = dis_10(rng);
    db.b_100[i] = dis_100(rng);
  }
}

double selection(uint32_t sel, Format format) {
  double time;

  switch (format) {
  case Format::BITMAP: {
    std::vector<uint64_t> bitmap(num_a / 64 + (num_a % 64 != 0));

    time = clock([&] {
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, num_a / 64),
                        [&](const tbb::blocked_range<uint32_t> &r) {
                          uint8_t hits[64];

                          for (uint32_t i = r.begin(); i != r.end(); ++i) {
                            uint32_t j = i * 64;
                            for (uint32_t k = 0; k < 64; ++k) {
                              hits[k] = db.a_100[j + k] < sel;
                            }

                            uint64_t m = 0;
                            for (uint32_t k = 0; k < 64; ++k) {
                              m |= (uint64_t)hits[k] << k;
                            }

                            bitmap[i] = m;
                          }
                        });

      for (uint32_t i = num_a / 64 * 64; i < num_a; ++i) {
        bitmap.back() |= (uint32_t)(db.a_100[i] < sel) << i % 64;
      }
    });

    uint64_t checksum = 0;
    for (uint32_t i = 0; i < num_a; ++i) {
      if ((bitmap[i / 64] >> i % 64) & 1) {
        checksum += db.a_10[i];
      }
    }

    std::cout << "checksum: " << checksum << std::endl;

    break;
  }

  case Format::INDICES: {
    tbb::enumerable_thread_specific<std::vector<uint32_t>> indices;
    for (std::vector<uint32_t> &local_indices : indices) {
      local_indices.reserve(num_a);
    }

    time = clock([&] {
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, num_a),
                        [&](const tbb::blocked_range<uint32_t> &r) {
                          std::vector<uint32_t> &local_indices =
                              indices.local();

                          uint32_t j = local_indices.size();
                          local_indices.resize(j + r.end() - r.begin());

                          for (uint32_t i = r.begin(); i != r.end(); ++i) {
                            local_indices[j] = i;
                            j += db.a_100[i] < sel;
                          }

                          local_indices.resize(j);
                        });
    });

    uint64_t checksum = 0;
    for (const std::vector<uint32_t> &local_indices : indices) {
      for (uint32_t i : local_indices) {
        checksum += db.a_10[i];
      }
    }

    std::cout << "checksum: " << checksum << std::endl;

    break;
  }

  case Format::VALUES: {
    tbb::enumerable_thread_specific<std::vector<uint32_t>> values;
    for (std::vector<uint32_t> &local_values : values) {
      local_values.reserve(num_a);
    }

    time = clock([&] {
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, num_a),
                        [&](const tbb::blocked_range<uint32_t> &r) {
                          std::vector<uint32_t> &local_values = values.local();

                          uint32_t j = local_values.size();
                          local_values.resize(j + r.end() - r.begin());

                          for (uint32_t i = r.begin(); i != r.end(); ++i) {
                            local_values[j] = db.a_10[i];
                            j += db.a_100[i] < sel;
                          }

                          local_values.resize(j);
                        });
    });

    uint64_t checksum = 0;
    for (const std::vector<uint32_t> &local_values : values) {
      for (uint32_t value : local_values) {
        checksum += value;
      }
    }

    std::cout << "checksum: " << checksum << std::endl;

    break;
  }
  }

  return time;
}

double semijoin(uint32_t sel, Format format) {
  double time;

  absl::flat_hash_set<uint32_t> hash_set;
  for (uint32_t i = 0; i < num_b; ++i) {
    if (db.b_100[i] < sel) {
      hash_set.emplace(db.b_k[i]);
    }
  }

  switch (format) {

  case Format::BITMAP: {
    std::vector<uint64_t> bitmap(num_a / 64 + (num_a % 64 != 0));

    time = clock([&] {
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, num_a / 64),
                        [&](const tbb::blocked_range<uint32_t> &r) {
                          uint8_t hits[64];
                          for (uint32_t i = r.begin(); i != r.end(); ++i) {
                            uint32_t j = i * 64;
                            for (uint32_t k = 0; k < 64; ++k) {
                              hits[k] = hash_set.contains(db.a_b_k[j + k]);
                            }

                            uint64_t m = 0;
                            for (uint32_t k = 0; k < 64; ++k) {
                              m |= (uint64_t)hits[k] << k;
                            }

                            bitmap[i] = m;
                          }
                        });

      for (uint32_t i = num_a / 64 * 64; i < num_a; ++i) {
        bitmap.back() |= (uint64_t)(hash_set.contains(db.a_b_k[i])) << i % 64;
      }
    });

    uint64_t checksum = 0;
    for (uint32_t i = 0; i < num_a; ++i) {
      if ((bitmap[i / 64] >> i % 64) & 1) {
        checksum += db.a_10[i];
      }
    }

    std::cout << "checksum: " << checksum << std::endl;

    break;
  }
  case Format::INDICES: {
    tbb::enumerable_thread_specific<std::vector<uint32_t>> indices;
    for (std::vector<uint32_t> &local_indices : indices) {
      local_indices.reserve(num_a);
    }

    time = clock([&] {
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, num_a),
                        [&](const tbb::blocked_range<uint32_t> &r) {
                          std::vector<uint32_t> &local_indices =
                              indices.local();

                          for (uint32_t i = r.begin(); i != r.end(); ++i) {
                            if (hash_set.contains(db.a_b_k[i])) {
                              local_indices.push_back(i);
                            }
                          }
                        });
    });

    uint64_t checksum = 0;
    for (const std::vector<uint32_t> &local_indices : indices) {
      for (uint32_t i : local_indices) {
        checksum += db.a_10[i];
      }
    }

    std::cout << "checksum: " << checksum << std::endl;

    break;
  }
  case Format::VALUES: {
    tbb::enumerable_thread_specific<std::vector<uint32_t>> values;
    for (std::vector<uint32_t> &local_values : values) {
      local_values.reserve(num_a);
    }

    time = clock([&] {
      tbb::parallel_for(tbb::blocked_range<uint32_t>(0, num_a),
                        [&](const tbb::blocked_range<uint32_t> &r) {
                          std::vector<uint32_t> &local_values = values.local();

                          for (uint32_t i = r.begin(); i != r.end(); ++i) {
                            if (hash_set.contains(db.a_b_k[i])) {
                              local_values.push_back(db.a_10[i]);
                            }
                          }
                        });
    });

    uint64_t checksum = 0;
    for (const std::vector<uint32_t> &local_values : values) {
      for (uint32_t value : local_values) {
        checksum += value;
      }
    }

    std::cout << "checksum: " << checksum << std::endl;

    break;
  }
  }

  return time;
}

double join(uint32_t sel) {
  absl::flat_hash_map<uint32_t, uint32_t> hash_map;
  for (uint32_t i = 0; i < num_b; ++i) {
    if (db.b_100[i] < sel) {
      hash_map.emplace(db.b_k[i], db.b_10[i]);
    }
  }

  tbb::enumerable_thread_specific<std::vector<std::pair<uint32_t, uint32_t>>>
      values;
  for (std::vector<std::pair<uint32_t, uint32_t>> &local_values : values) {
    local_values.reserve(num_a);
  }

  double time = clock([&] {
    tbb::parallel_for(
        tbb::blocked_range<uint32_t>(0, num_a),
        [&](const tbb::blocked_range<uint32_t> &r) {
          std::vector<std::pair<uint32_t, uint32_t>> &local_values =
              values.local();

          for (uint32_t i = r.begin(); i != r.end(); ++i) {
            auto it = hash_map.find(db.a_b_k[i]);
            if (it != hash_map.end()) {
              local_values.emplace_back(db.a_10[i], it->second);
            }
          }
        });
  });

  uint64_t checksum = 0;
  for (const std::vector<std::pair<uint32_t, uint32_t>> &local_values :
       values) {
    for (const std::pair<uint32_t, uint32_t> &value : local_values) {
      checksum += value.first + value.second;
    }
  }

  std::cout << "checksum: " << checksum << std::endl;

  return time;
}

double aggregate() {
  uint32_t result;

  double time = clock([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<uint32_t>(0, num_a),
        0,
        [&](const tbb::blocked_range<uint32_t> &r, uint32_t acc) {
          for (uint32_t i = r.begin(); i != r.end(); ++i) {
            acc += db.a_10[i];
          }
          return acc;
        },
        std::plus<>());
  });

  std::cout << "checksum: " << result << std::endl;

  return time;
}

double group_aggregate() {
  std::array<uint32_t, 100> result{};

  double time = clock([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<uint32_t>(0, num_a),
        std::array<uint32_t, 100>{},
        [&](const tbb::blocked_range<uint32_t> &r,
            std::array<uint32_t, 100> acc) {
          for (uint32_t i = r.begin(); i != r.end(); ++i) {
            acc[db.a_100[i]] += db.a_10[i];
          }
          return acc;
        },
        [](std::array<uint32_t, 100> a, const std::array<uint32_t, 100> &b) {
          for (uint32_t i = 0; i < 100; ++i) {
            a[i] += b[i];
          }
          return a;
        });
  });

  uint32_t checksum =
      std::accumulate(result.begin(), result.end(), uint32_t(0));
  std::cout << "checksum: " << checksum << std::endl;

  return time;
}

double consume_bitmap(uint32_t sel) {
  uint32_t result;

  std::vector<uint32_t> bitmap(num_a / 32 + (num_a % 32 != 0));

  for (uint32_t i = 0; i < num_a; ++i) {
    if (db.a_100[i] < sel) {
      bitmap[i / 32] |= uint32_t(1) << i % 32;
    }
  }

  double time = clock([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<uint32_t>(0, bitmap.size()),
        0,
        [&](const tbb::blocked_range<uint32_t> &r, uint32_t acc) {
          for (uint32_t i = r.begin(); i != r.end(); ++i) {
            uint32_t m = bitmap[i];
            uint32_t j = i * 32;
            while (m != 0) {
              size_t k = __builtin_ctzl(m);
              acc += db.a_10[j + k];
              m ^= uint32_t(1) << k;
            }
          }
          return acc;
        },
        std::plus<>());
  });

  std::cout << "checksum: " << result << std::endl;

  return time;
}

double consume_indices(uint32_t sel) {
  uint32_t result;

  std::vector<uint32_t> indices;
  indices.reserve(num_a);

  for (uint32_t i = 0; i < num_a; ++i) {
    if (db.a_100[i] < sel) {
      indices.push_back(i);
    }
  }

  double time = clock([&] {
    result = tbb::parallel_reduce(
        tbb::blocked_range<uint32_t>(0, indices.size()),
        0,
        [&](const tbb::blocked_range<uint32_t> &r, uint32_t acc) {
          for (uint32_t i = r.begin(); i != r.end(); ++i) {
            acc += db.a_10[indices[i]];
          }
          return acc;
        },
        std::plus<>());
  });

  std::cout << "checksum: " << result << std::endl;

  return time;
}

int main(int argc, char **argv) {
  cxxopts::Options options("run", "BLIMP-DB microbenchmarks");

  cxxopts::OptionAdder option_adder = options.add_options();
  option_adder("num_a",
               "Number of A rows",
               cxxopts::value<uint32_t>()->default_value("100000000"));
  option_adder("num_b",
               "Number of B rows",
               cxxopts::value<uint32_t>()->default_value("1000000"));
  option_adder("num_trials",
               "Number of trials",
               cxxopts::value<size_t>()->default_value("6"));
  option_adder("h,help", "Display help");

  cxxopts::ParseResult parse_result = options.parse(argc, argv);

  if (parse_result.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  num_a = parse_result["num_a"].as<uint32_t>();
  num_b = parse_result["num_b"].as<uint32_t>();
  size_t num_trials = parse_result["num_trials"].as<size_t>();

  std::ofstream file("results.csv");
  if (!file.is_open()) {
    throw std::runtime_error(std::strerror(errno));
  }

  generate();

  file << "trial,microbenchmark,selectivity,format,time" << std::endl;

  //  for (uint32_t sel : {1, 5, 25}) {
  //    for (Format format : {Format::BITMAP, Format::INDICES, Format::VALUES})
  //    {
  //      for (size_t trial = 0; trial < num_trials; ++trial) {
  //        file << trial << ",selection," << sel << ',' << format << ','
  //             << selection(sel, format) << std::endl;
  //      }
  //    }
  //  }
  //
  //  for (uint32_t sel : {1, 5, 25}) {
  //    for (Format format : {Format::BITMAP, Format::INDICES, Format::VALUES})
  //    {
  //      for (size_t trial = 0; trial < num_trials; ++trial) {
  //        file << trial << ",semijoin," << sel << ',' << format << ','
  //             << semijoin(sel, format) << std::endl;
  //      }
  //    }
  //  }
  //
  //  for (uint32_t sel : {1, 5, 25}) {
  //    for (size_t trial = 0; trial < num_trials; ++trial) {
  //      file << trial << ",join," << sel << ",3," << join(sel) << std::endl;
  //    }
  //  }
  //
  //  for (size_t trial = 0; trial < num_trials; ++trial) {
  //    file << trial << ",aggregate,100,3," << aggregate() << std::endl;
  //  }
  //
  //  for (size_t trial = 0; trial < num_trials; ++trial) {
  //    file << trial << ",group-aggregate,100,3," << group_aggregate()
  //         << std::endl;
  //  }

  for (uint32_t sel : {0, 1, 3, 5, 10, 25, 50, 100}) {
    double time;

    time = consume_bitmap(sel);
    for (size_t trial = 0; trial < num_trials; ++trial) {
      file << trial << ",consume-bitmap," << sel << ",0," << time << std::endl;
    }

    time = consume_indices(sel);
    for (size_t trial = 0; trial < num_trials; ++trial) {
      file << trial << ",consume-indices," << sel << ",0," << time << std::endl;
    }
  }

  return 0;
}
