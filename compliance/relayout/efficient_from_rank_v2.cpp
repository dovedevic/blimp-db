#include <chrono>
#include <iostream>
#include <random>
#include <stdexcept>

#include <oneapi/tbb.h>
#include <x86intrin.h>

int num_trials = 10;

std::vector<size_t> bank_region_sizes = {1024,
                                         16384,
                                         32768,
                                         65536,
                                         131072,
                                         262144,
                                         524288,
                                         1048576,
                                         2097152,
                                         4194304,
                                         8388608,
                                         16777216,
                                         33554432,
                                         268435456,
                                         536870912,
                                         1073741824};

int main() {
  for (size_t bank_region_size : bank_region_sizes) {
    size_t total_region_size = bank_region_size * 8;

    // Allocate the memory region.
    auto *memory_region = (uint8_t *)std::aligned_alloc(64, total_region_size);

    for (int trial = 0; trial < num_trials; ++trial) {
      // Initialize the memory region.
      for (uint64_t i = 0; i < total_region_size; ++i) {
        memory_region[i] = i % 64;
      }

      auto t0 = std::chrono::steady_clock::now();

      __m128i shuffle_mask =
          _mm_setr_epi8(0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15);

      // Transpose blocks of 8x8 bytes.
      tbb::parallel_for(size_t(0), total_region_size / 64, [&](size_t j) {
        size_t i = j * 64;

        __m128i a0 = _mm_load_si128((__m128i *)&memory_region[i]);
        __m128i a1 = _mm_load_si128((__m128i *)&memory_region[i + 16]);
        __m128i a2 = _mm_load_si128((__m128i *)&memory_region[i + 32]);
        __m128i a3 = _mm_load_si128((__m128i *)&memory_region[i + 48]);

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

        _mm_store_si128((__m128i *)&memory_region[i], d0);
        _mm_store_si128((__m128i *)&memory_region[i + 16], d1);
        _mm_store_si128((__m128i *)&memory_region[i + 32], d2);
        _mm_store_si128((__m128i *)&memory_region[i + 48], d3);
      });

      auto t1 = std::chrono::steady_clock::now();

      // Verify correctness.
      for (uint64_t i = 0; i < total_region_size; ++i) {
        if (memory_region[i] != (i % 64) / 8 + 8 * (i % 8)) {
          throw std::runtime_error("incorrect");
        }
      }

      float time = std::chrono::duration<float>(t1 - t0).count();
      std::cout << bank_region_size << ',' << trial << ',' << time << std::endl;
    }

    free(memory_region);
  }
}
