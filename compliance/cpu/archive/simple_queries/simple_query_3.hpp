#ifndef BLIMP_DB_CPU_SIMPLE_QUERY_3_HPP
#define BLIMP_DB_CPU_SIMPLE_QUERY_3_HPP

#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>
#include <x86intrin.h>

#include <duckdb.hpp>
#include <duckdb/main/appender.hpp>
#include <oneapi/tbb.h>

namespace simple_query_3 {

namespace internal {

std::vector<size_t> reduce(std::vector<size_t> v0,
                           const std::vector<size_t> &v1) {
  for (size_t i = 0; i < 12; ++i) {
    v0[i] += v1[i];
  }
  return v0;
}

std::vector<size_t> end_to_end_chunk(size_t begin, size_t end,
                                     const uint16_t *a0, const uint16_t *b1,
                                     const uint16_t *b2, uint16_t x) {
  std::vector<size_t> result(12);
  for (size_t i = begin; i < end; ++i) {
    uint16_t k = a0[i];
    result[b2[k]] += b1[k] == x;
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

void generate(size_t n, uint16_t *a0, uint16_t *b0, uint16_t *b1, uint16_t *b2,
              duckdb::Connection &con) {
  con.Query("DROP TABLE IF EXISTS a");
  con.Query("DROP TABLE IF EXISTS b");
  con.Query("CREATE TABLE a (a0 SMALLINT)");
  con.Query(
      "CREATE TABLE b (b0 SMALLINT PRIMARY KEY, b1 SMALLINT, b2 SMALLINT)");

  duckdb::Appender app_a(con, "a");
  duckdb::Appender app_b(con, "b");

  std::random_device rd;
  std::minstd_rand prng(rd());
  std::uniform_int_distribution<uint16_t> dis_a0(0, 10 * 365 - 1);
  std::uniform_int_distribution<uint16_t> dis_b1(1990, 1999);
  std::uniform_int_distribution<uint16_t> dis_b2(0, 11);

  for (size_t i = 0; i < n; ++i) {
    a0[i] = dis_a0(prng);
    app_a.AppendRow(a0[i]);
  }

  for (size_t i = 0; i < 10 * 365; ++i) {
    b0[i] = i;
    b1[i] = dis_b1(prng);
    b2[i] = dis_b2(prng);
    app_b.AppendRow(b0[i], b1[i], b2[i]);
  }
}

std::vector<size_t> end_to_end(size_t n, const uint16_t *a0, const uint16_t *b1,
                               const uint16_t *b2, uint16_t x) {
  return tbb::parallel_reduce(
      tbb::blocked_range<size_t>(0, n), std::vector<size_t>(12),
      [&](const tbb::blocked_range<size_t> &r, std::vector<size_t> acc) {
        return internal::reduce(
            std::move(acc),
            internal::end_to_end_chunk(r.begin(), r.end(), a0, b1, b2, x));
      },
      internal::reduce);
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

std::vector<size_t> aggregate(size_t n, const uint32_t *m, const uint16_t *a0,
                              const uint16_t *b2) {
  return tbb::parallel_reduce(
      tbb::blocked_range<size_t>(0, n), std::vector<size_t>(12),
      [&](const tbb::blocked_range<size_t> &r, std::vector<size_t> acc) {
        for (size_t i = r.begin(); i < r.end(); ++i) {
          uint16_t k = a0[i];
          acc[b2[k]] += m[i / 32] >> (i % 32) & 1;
        }
        return acc;
      },
      internal::reduce);
}

std::vector<size_t> query_duckdb(duckdb::Connection &con, uint16_t x) {
  std::vector<size_t> result(12);

  auto chunk = con.Query("SELECT COUNT(*) FROM a, b "
                         "WHERE a0 = b0 AND b1 = ? "
                         "GROUP BY b2 "
                         "ORDER BY b2",
                         x)
                   ->Fetch();

  for (size_t i = 0; i < 12; ++i) {
    result[i] = chunk->GetValue(0, i).GetValue<uint32_t>();
  }

  return result;
}

} // namespace simple_query_3

#endif // BLIMP_DB_CPU_SIMPLE_QUERY_3_HPP
