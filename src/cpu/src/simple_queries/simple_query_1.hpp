#ifndef BLIMP_DB_CPU_SIMPLE_QUERY_1_HPP
#define BLIMP_DB_CPU_SIMPLE_QUERY_1_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <x86intrin.h>

#include <duckdb.hpp>
#include <duckdb/main/appender.hpp>
#include <oneapi/tbb.h>

namespace simple_query_1 {

namespace internal {

size_t end_to_end_chunk(size_t begin, size_t end, const uint16_t *a0,
                        uint16_t x) {
  size_t result = 0;
  for (size_t i = begin; i < end; ++i) {
    result += a0[i] == x;
  }
  return result;
}

void filter_chunk(size_t begin, size_t end, const uint16_t *a0, uint16_t x,
                  uint32_t *m) {
  for (size_t i = begin; i < end; ++i) {
    m[i / 32] |= (a0[i] == x) << (i % 32);
  }
}

uint32_t filter_chunk_avx(const uint16_t *a0, uint16_t x) {
  static __m256i x_16u16 = _mm256_set1_epi16((short)x);
  __m256i c0_0_16u16 = _mm256_lddqu_si256((__m256i *)a0);
  __m256i c0_1_16u16 = _mm256_lddqu_si256((__m256i *)&a0[16]);
  __m256i mask_0_16u16 = _mm256_cmpeq_epi16(x_16u16, c0_0_16u16);
  __m256i mask_1_16u16 = _mm256_cmpeq_epi16(x_16u16, c0_1_16u16);
  __m256i mask_32u8 = _mm256_packs_epi16(mask_0_16u16, mask_1_16u16);
  return _mm256_movemask_epi8(mask_32u8);
}

} // namespace internal

void generate(size_t n, uint16_t *a0, duckdb::Connection &con) {
  con.Query("DROP TABLE IF EXISTS a");
  con.Query("CREATE TABLE a (a0 SMALLINT)");

  duckdb::Appender app_a(con, "a");

  std::random_device rd;
  std::minstd_rand prng(rd());
  std::uniform_int_distribution<uint16_t> dis_a0(1990, 1999);

  for (size_t i = 0; i < n; ++i) {
    a0[i] = dis_a0(prng);
    app_a.AppendRow(a0[i]);
  }
}

size_t end_to_end(size_t n, const uint16_t *a0, uint16_t x) {
  return tbb::parallel_reduce(
      tbb::blocked_range<size_t>(0, n), 0,
      [&](const tbb::blocked_range<size_t> &r, size_t acc) {
        return acc + internal::end_to_end_chunk(r.begin(), r.end(), a0, x);
      },
      std::plus<>());
}

size_t end_to_end_avx(size_t n, const uint16_t *a0, uint16_t x) {
  size_t acc = tbb::parallel_reduce(
      tbb::blocked_range<size_t>(0, n / 32), 0,
      [&](const tbb::blocked_range<size_t> &r, size_t acc) {
        for (size_t i = r.begin(); i < r.end(); ++i) {
          uint32_t mask = internal::filter_chunk_avx(&a0[i * 32], x);
          acc += _mm_popcnt_u32(mask);
        }
        return acc;
      },
      std::plus<>());

  // Process the remaining records.
  acc += internal::end_to_end_chunk(n / 32 * 32, n, a0, x);

  return acc;
}

void filter(size_t n, const uint16_t *a0, uint16_t x, uint32_t *m) {
  tbb::parallel_for(tbb::blocked_range<size_t>(0, n / 32),
                    [&](const tbb::blocked_range<size_t> &r) {
                      internal::filter_chunk(r.begin() * 32, r.end() * 32, a0,
                                             x, m);
                    });

  // Process the remaining records.
  internal::filter_chunk(n / 32 * 32, n, a0, x, m);
}

void filter_avx(size_t n, const uint16_t *a0, uint16_t x, uint32_t *m) {
  tbb::parallel_for(tbb::blocked_range<size_t>(0, n / 32),
                    [&](const tbb::blocked_range<size_t> &r) {
                      for (size_t i = r.begin(); i < r.end(); ++i) {
                        m[i] = internal::filter_chunk_avx(&a0[i * 32], x);
                      }
                    });

  // Process the remaining records.
  internal::filter_chunk(n / 32 * 32, n, a0, x, m);
}

size_t aggregate(size_t n, const uint32_t *m) {
  return tbb::parallel_reduce(
      tbb::blocked_range<size_t>(0, n / 32 + (n % 32 != 0)), 0,
      [&](const tbb::blocked_range<size_t> &r, size_t acc) {
        for (size_t i = r.begin(); i < r.end(); ++i) {
          acc += _mm_popcnt_u32(m[i]);
        }
        return acc;
      },
      std::plus<>());
}

size_t query_duckdb(duckdb::Connection &con, uint16_t x) {
  return con.Query("SELECT COUNT(*) FROM a WHERE a0 = ?", x)
      ->Fetch()
      ->GetValue(0, 0)
      .GetValue<uint32_t>();
}

} // namespace simple_query_1

#endif // BLIMP_DB_CPU_SIMPLE_QUERY_1_HPP
