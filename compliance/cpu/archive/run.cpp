#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <duckdb.hpp>

#include "simple_queries/simple_query_1.hpp"
#include "simple_queries/simple_query_2.hpp"
#include "simple_queries/simple_query_3.hpp"
#include "util.hpp"

template <typename F> void run(const std::string &name, size_t trials, F &&f) {
  for (size_t trial = 0; trial < trials; ++trial) {
    double t = util::time(f);
    std::cout << name << ',' << trial << ',' << t << std::endl;
  }
}

template <typename T> void assert_equal(const T &t0, const T &t1) {
  if (t0 != t1) {
    throw std::runtime_error("assertion failed");
  }
}

int main() {
  size_t trials = 10;
  size_t n = 600e6;
  uint16_t x = 1990;

  std::vector<uint16_t> a0(n);
  std::vector<uint16_t> b0(10 * 365);
  std::vector<uint16_t> b1(10 * 365);
  std::vector<uint16_t> b2(10 * 365);
  std::vector<uint32_t> m(n / 32 + (n % 32 != 0));

  duckdb::DuckDB db;
  duckdb::Connection con(db);

  simple_query_1::generate(n, a0.data(), con);

  size_t simple_query_1_result = simple_query_1::query_duckdb(con, x);
  assert_equal(simple_query_1::end_to_end(n, a0.data(), x),
               simple_query_1_result);
  simple_query_1::filter(n, a0.data(), x, m.data());
  assert_equal(simple_query_1::aggregate(n, m.data()), simple_query_1_result);
  std::fill(m.begin(), m.end(), 0);
  simple_query_1::filter_avx(n, a0.data(), x, m.data());
  assert_equal(simple_query_1::aggregate(n, m.data()), simple_query_1_result);
  std::fill(m.begin(), m.end(), 0);

  run("simple_query_1::end_to_end", trials,
      [&] { simple_query_1::end_to_end(n, a0.data(), x); });

  run("simple_query_1::end_to_end_avx", trials,
      [&] { simple_query_1::end_to_end_avx(n, a0.data(), x); });

  run("simple_query_1::filter", trials,
      [&] { simple_query_1::filter(n, a0.data(), x, m.data()); });

  run("simple_query_1::filter_avx", trials,
      [&] { simple_query_1::filter_avx(n, a0.data(), x, m.data()); });

  run("simple_query_1::aggregate", trials,
      [&] { simple_query_1::aggregate(n, m.data()); });

  run("simple_query_1::duckdb", trials,
      [&] { simple_query_1::query_duckdb(con, x); });

  simple_query_2::generate(n, a0.data(), b0.data(), b1.data(), con);
  std::fill(m.begin(), m.end(), 0);

  size_t simple_query_2_result = simple_query_2::query_duckdb(con, x);
  assert_equal(simple_query_2::end_to_end(n, a0.data(), b1.data(), x),
               simple_query_2_result);
  simple_query_2::filter(n, a0.data(), b1.data(), x, m.data());
  assert_equal(simple_query_2::aggregate(n, m.data()), simple_query_2_result);
  std::fill(m.begin(), m.end(), 0);

  run("simple_query_2::end_to_end", trials,
      [&] { simple_query_2::end_to_end(n, a0.data(), b1.data(), x); });

  run("simple_query_2::filter", trials,
      [&] { simple_query_2::filter(n, a0.data(), b1.data(), x, m.data()); });

  run("simple_query_2::aggregate", trials,
      [&] { simple_query_2::aggregate(n, m.data()); });

  run("simple_query_2::duckdb", trials,
      [&] { simple_query_2::query_duckdb(con, x); });

  simple_query_3::generate(n, a0.data(), b0.data(), b1.data(), b2.data(), con);
  std::fill(m.begin(), m.end(), 0);

  std::vector<size_t> simple_query_3_result =
      simple_query_3::query_duckdb(con, x);
  assert_equal(
      simple_query_3::end_to_end(n, a0.data(), b1.data(), b2.data(), x),
      simple_query_3_result);
  simple_query_3::filter(n, a0.data(), b1.data(), x, m.data());
  assert_equal(simple_query_3::aggregate(n, m.data(), a0.data(), b2.data()),
               simple_query_3_result);
  std::fill(m.begin(), m.end(), 0);

  run("simple_query_3::end_to_end", trials, [&] {
    simple_query_3::end_to_end(n, a0.data(), b1.data(), b2.data(), x);
  });

  run("simple_query_3::filter", trials,
      [&] { simple_query_3::filter(n, a0.data(), b1.data(), x, m.data()); });

  run("simple_query_3::aggregate", trials,
      [&] { simple_query_3::aggregate(n, m.data(), a0.data(), b2.data()); });

  run("simple_query_3::duckdb", trials,
      [&] { simple_query_3::query_duckdb(con, x); });

  return 0;
}
