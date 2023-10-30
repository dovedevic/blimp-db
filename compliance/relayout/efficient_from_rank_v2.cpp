#include <chrono>
#include <iostream>
#include <random>
#include <stdexcept>
#include <vector>

#include <x86intrin.h>

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0]
              << " [single rank bank memory size] [trials]" << std::endl;
    return -1;
  }

  // The total region byte size per bank.
  uint64_t bank_region_size = std::stoull(argv[1]);
  uint64_t total_region_size = bank_region_size * 8;

  if (total_region_size % 64 != 0) {
    throw std::logic_error("total region size must be a multiple of 64");
  }

  // The number of trials to perform the placement study.
  int trials = std::stoi(argv[2]);

  std::cout << "Bank Region Size: " << bank_region_size << "B\n";
  std::cout << "Total Region Size: " << total_region_size << "B\n";
  std::cout << "Trials: " << trials << "\n";

  // Allocate the memory region.
  std::vector<uint8_t> memory_region(total_region_size);

  std::vector<float> bench_times(trials);

  for (int trial = 0; trial < trials; ++trial) {
    // Initialize the memory region.
    for (uint64_t i = 0; i < total_region_size; ++i) {
      memory_region[i] = i % 64;
    }

    auto t0 = std::chrono::steady_clock::now();

    __m128 shuffle_mask =
        _mm_setr_epi8(0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15);

    // Transpose blocks of 8x8 bytes.
    for (uint64_t i = 0; i < total_region_size; i += 64) {
      __m128i a0 = _mm_lddqu_si128((__m128i *)&memory_region[i]);
      __m128i a1 = _mm_lddqu_si128((__m128i *)&memory_region[i + 16]);
      __m128i a2 = _mm_lddqu_si128((__m128i *)&memory_region[i + 32]);
      __m128i a3 = _mm_lddqu_si128((__m128i *)&memory_region[i + 48]);

      __m128i b0 = _mm_shuffle_epi8(a0, shuffle_mask);
      __m128i b1 = _mm_shuffle_epi8(a1, shuffle_mask);
      __m128i b2 = _mm_shuffle_epi8(a2, shuffle_mask);
      __m128i b3 = _mm_shuffle_epi8(a3, shuffle_mask);

      __m128i c0 = _mm_unpacklo_epi16(b0, b1);
      __m128i c1 = _mm_unpackhi_epi16(b0, b1);
      __m128i c2 = _mm_unpacklo_epi16(b2, b3);
      __m128i c3 = _mm_unpackhi_epi16(b2, b3);

      __m128i d0 = _mm_unpacklo_epi32(c0, c2);
      __m128i d1 = _mm_unpackhi_epi32(c0, c2);
      __m128i d2 = _mm_unpacklo_epi32(c1, c3);
      __m128i d3 = _mm_unpackhi_epi32(c1, c3);

      _mm_storeu_si128((__m128i *)&memory_region[i], d0);
      _mm_storeu_si128((__m128i *)&memory_region[i + 16], d1);
      _mm_storeu_si128((__m128i *)&memory_region[i + 32], d2);
      _mm_storeu_si128((__m128i *)&memory_region[i + 48], d3);
    }

    auto t1 = std::chrono::steady_clock::now();

    // Verify correctness.
    for (uint64_t i = 0; i < total_region_size; ++i) {
      if (memory_region[i] != (i % 64) / 8 + 8 * (i % 8)) {
        throw std::runtime_error("incorrect");
      }
    }

    bench_times[trial] = std::chrono::duration<float>(t1 - t0).count();
  }
}
