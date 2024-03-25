#include <cxxopts.hpp>
#include <oneapi/tbb.h>

#include <array>
#include <fstream>
#include <iostream>
#include <random>

template <typename T> std::vector<T> generate_values(size_t num_rows) {
  std::random_device rd;
  std::minstd_rand rng(rd());
  std::uniform_int_distribution<T> dis(0, 99);

  std::vector<T> values(num_rows);
  for (T &value : values) {
    value = dis(rng);
  }

  return values;
}

template <typename T> uint64_t compare_scalar_tail(size_t n, const T *a, T b) {
  uint64_t mask = 0;
  for (size_t i = 0; i < n; ++i) {
    mask |= (uint64_t)(a[i] < b) << i;
  }

  return mask;
}

template <typename T> uint64_t compare_simd(const T *, T) {
  throw std::logic_error("unimplemented");
}

template <> inline uint64_t compare_simd<int8_t>(const int8_t *a, int8_t b) {
  uint64_t mask = 0;
  __m256i b_32i8 = _mm256_set1_epi8(b);
  __m256i c_32i8;
  for (int i = 0; i < 64; i += 32) {
    __m256i a_32i8 = _mm256_lddqu_si256((__m256i *)&a[i]);
    c_32i8 = _mm256_cmpgt_epi8(b_32i8, a_32i8);
    mask |= (uint64_t)(unsigned)_mm256_movemask_epi8(c_32i8) << i;
  }
  return mask;
}

template <> inline uint64_t compare_simd<int16_t>(const int16_t *a, int16_t b) {
  uint64_t mask = 0;
  __m256i b_16i16 = _mm256_set1_epi16(b);
  for (int i = 0; i < 64; i += 32) {
    __m256i a_0_16i16 = _mm256_lddqu_si256((__m256i *)&a[i]);
    __m256i a_1_16i16 = _mm256_lddqu_si256((__m256i *)&a[i + 16]);
    __m256i c_0_16i16 = _mm256_cmpgt_epi16(b_16i16, a_0_16i16);
    __m256i c_1_16i16 = _mm256_cmpgt_epi16(b_16i16, a_1_16i16);
    __m256i c_32u8_v1 = _mm256_packs_epi16(c_0_16i16, c_1_16i16);
    __m256i c_32u8_v2 = _mm256_permute4x64_epi64(c_32u8_v1, 0b11011000);
    mask |= (uint64_t)(unsigned)_mm256_movemask_epi8(c_32u8_v2) << i;
  }
  return mask;
}

template <> inline uint64_t compare_simd<int32_t>(const int32_t *a, int32_t b) {
  uint64_t mask = 0;
  __m256i b_8i32 = _mm256_set1_epi32(b);
  for (int i = 0; i < 64; i += 8) {
    __m256i a_8i32 = _mm256_lddqu_si256((__m256i *)&a[i]);
    __m256i c_8i32 = _mm256_cmpgt_epi32(b_8i32, a_8i32);
    mask |= (uint64_t)(unsigned)_mm256_movemask_ps(_mm256_castsi256_ps(c_8i32))
            << i;
  }
  return mask;
}

template <> inline uint64_t compare_simd<int64_t>(const int64_t *a, int64_t b) {
  uint64_t mask = 0;
  __m256i b_4i64 = _mm256_set1_epi64x(b);
  for (int i = 0; i < 64; i += 4) {
    __m256i a_4i64 = _mm256_lddqu_si256((__m256i *)&a[i]);
    __m256i c_4i64 = _mm256_cmpgt_epi64(b_4i64, a_4i64);
    mask |= (uint64_t)(unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(c_4i64))
            << i;
  }
  return mask;
}

template <typename T, typename C>
void selection_hitmap(const std::vector<T> &values,
                      size_t num_trials,
                      T selectivity,
                      C &&compare,
                      std::ofstream &out) {
  std::vector<uint64_t> hitmap(values.size() / 64 + (values.size() % 64 != 0));

  for (size_t trial = 0; trial < num_trials; ++trial) {
    std::fill(hitmap.begin(), hitmap.end(), 0);

    auto t0 = std::chrono::steady_clock::now();

    tbb::parallel_for(tbb::blocked_range<size_t>(0, values.size() / 64),
                      [&](const tbb::blocked_range<size_t> &r) {
                        for (size_t i = r.begin(); i < r.end(); ++i) {
                          hitmap[i] = compare(&values[i * 64], (T)selectivity);
                        }
                      });

    if (values.size() % 64 != 0) {
      hitmap[values.size() / 64] = compare_scalar_tail(
          values.size() % 64, &values[values.size() / 64 * 64], (T)selectivity);
    }

    auto t1 = std::chrono::steady_clock::now();

    size_t count = 0;
    uint64_t checksum = 0;
    for (size_t i = 0; i < hitmap.size(); ++i) {
      uint64_t m = hitmap[i];
      while (m != 0) {
        ++count;
        size_t k = __builtin_ctzll(m);
        checksum += values[i * 64 + k];
        m ^= (uint64_t)1 << k;
      }
    }

    std::cout << "checksum: " << checksum << ", count: " << count << std::endl;

    out << typeid(T).name() << ",bitmap," << (int)selectivity << ',' << trial
        << ',' << std::chrono::duration<float>(t1 - t0).count() << std::endl;
  }
}

template <typename T>
void selection_values(const std::vector<T> &values,
                      size_t num_trials,
                      T selectivity,
                      std::ofstream &out) {
  tbb::enumerable_thread_specific<std::vector<T>> result;
  for (std::vector<T> &local_result : result) {
    local_result.reserve(values.size());
  }

  for (size_t trial = 0; trial < num_trials; ++trial) {
    for (std::vector<T> &local_result : result) {
      local_result.clear();
    }

    auto t0 = std::chrono::steady_clock::now();

    tbb::parallel_for(tbb::blocked_range<size_t>(0, values.size()),
                      [&](const tbb::blocked_range<size_t> &r) {
                        std::vector<T> &local_result = result.local();

                        size_t j = local_result.size();
                        local_result.resize(j + r.size());

                        for (size_t i = r.begin(); i < r.end(); ++i) {
                          T value = values[i];
                          if (value < selectivity) {
                            local_result[j++] = value;
                          }
                        }

                        local_result.resize(j);
                      });

    auto t1 = std::chrono::steady_clock::now();

    size_t count = 0;
    uint64_t checksum = 0;
    for (const std::vector<T> &local_result : result) {
      count += local_result.size();
      for (T value : local_result) {
        checksum += value;
      }
    }

    std::cout << "checksum: " << checksum << ", count: " << count << std::endl;

    out << typeid(T).name() << ",values," << (int)selectivity << ',' << trial
        << ',' << std::chrono::duration<float>(t1 - t0).count() << std::endl;
  }
}

template <typename T>
void selection(const std::vector<T> &values,
               T selectivity,
               size_t num_trials,
               std::ofstream &out) {
  selection_hitmap<T>(values, num_trials, selectivity, compare_simd<T>, out);
  selection_values(values, num_trials, selectivity, out);
}

int main(int argc, char **argv) {
  cxxopts::Options options("run", "BLIMP-DB microbenchmarks");

  cxxopts::OptionAdder option_adder = options.add_options();
  option_adder("num_rows",
               "Number of rows",
               cxxopts::value<size_t>()->default_value("1000"));
  option_adder("num_trials",
               "Number of trials",
               cxxopts::value<size_t>()->default_value("1"));
  option_adder("h,help", "Display help");

  cxxopts::ParseResult parse_result = options.parse(argc, argv);

  if (parse_result.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  auto num_rows = parse_result["num_rows"].as<size_t>();
  auto num_trials = parse_result["num_trials"].as<size_t>();

  std::ofstream out("selection.csv");
  if (!out.is_open()) {
    throw std::runtime_error(std::strerror(errno));
  }

  std::vector<int8_t> int8_values = generate_values<int8_t>(num_rows);
  std::vector<int16_t> int16_values = generate_values<int16_t>(num_rows);
  std::vector<int32_t> int32_values = generate_values<int32_t>(num_rows);
  std::vector<int64_t> int64_values = generate_values<int64_t>(num_rows);

  for (int selectivity : {0, 1, 5, 10, 25, 50, 100}) {
    selection(int8_values, (int8_t)selectivity, num_trials, out);
    selection(int16_values, (int16_t)selectivity, num_trials, out);
    selection(int32_values, (int32_t)selectivity, num_trials, out);
    selection(int64_values, (int64_t)selectivity, num_trials, out);
  }

  return 0;
}
