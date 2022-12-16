#ifndef BLIMP_DB_CPU_SIMPLE_QUERY_2_HPP
#define BLIMP_DB_CPU_SIMPLE_QUERY_2_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <x86intrin.h>

#include <duckdb.hpp>
#include <duckdb/main/appender.hpp>
#include <oneapi/tbb.h>

namespace simple_query_2 {

namespace internal {

size_t end_to_end_chunk(size_t begin, size_t end, const uint16_t *a0,
                        const uint16_t *b1, uint16_t x) {
  size_t result = 0;
  for (size_t i = begin; i < end; ++i) {
    result += b1[a0[i]] == x;
  }
  return result;
}

void filter_chunk(size_t begin, size_t end, const uint16_t *a0,
                  const uint16_t *b1, uint16_t x, uint32_t *m) {
  for (size_t i = begin; i < end; ++i) {
    m[i / 32] |= (b1[a0[i]] == x) << (i % 32);
  }
}

} // namespace internal

void generate(size_t n, uint16_t *a0, uint16_t *b0, uint16_t *b1,
              duckdb::Connection &con) {
  con.Query("DROP TABLE IF EXISTS a");
  con.Query("DROP TABLE IF EXISTS b");
  con.Query("CREATE TABLE a (a0 SMALLINT)");
  con.Query("CREATE TABLE b (b0 SMALLINT PRIMARY KEY, b1 SMALLINT)");

  duckdb::Appender app_a(con, "a");
  duckdb::Appender app_b(con, "b");

  std::random_device rd;
  std::minstd_rand prng(rd());
  std::uniform_int_distribution<uint16_t> dis_a0(0, 10 * 365 - 1);
  std::uniform_int_distribution<uint16_t> dis_b1(1990, 1999);

  for (size_t i = 0; i < n; ++i) {
    a0[i] = dis_a0(prng);
    app_a.AppendRow(a0[i]);
  }

  for (size_t i = 0; i < 10 * 365; ++i) {
    b0[i] = i;
    b1[i] = dis_b1(prng);
    app_b.AppendRow(b0[i], b1[i]);
  }
}

size_t end_to_end(size_t n, const uint16_t *a0, const uint16_t *b1,
                  uint16_t x) {
  return tbb::parallel_reduce(
      tbb::blocked_range<size_t>(0, n), 0,
      [&](const tbb::blocked_range<size_t> &r, size_t acc) {
        return acc + internal::end_to_end_chunk(r.begin(), r.end(), a0, b1, x);
      },
      std::plus<>());
}

void filter(size_t n, const uint16_t *a0, const uint16_t *b1, uint16_t x,
            uint32_t *m) {
  tbb::parallel_for(tbb::blocked_range<size_t>(0, n / 32),
                    [&](const tbb::blocked_range<size_t> &r) {
                      internal::filter_chunk(r.begin() * 32, r.end() * 32, a0,
                                             b1, x, m);
                    });

  // Process the remaining records.
  internal::filter_chunk(n / 32 * 32, n, a0, b1, x, m);
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
  return con.Query("SELECT COUNT(*) FROM a, b WHERE a0 = b0 AND b1 = ?", x)
      ->Fetch()
      ->GetValue(0, 0)
      .GetValue<uint32_t>();
}

} // namespace simple_query_2

#endif // BLIMP_DB_CPU_SIMPLE_QUERY_2_HPP
