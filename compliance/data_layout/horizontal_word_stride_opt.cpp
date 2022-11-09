#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <string>
#include <vector>
#include <limits>
#include <iostream>
#include <chrono>


uint64_t* src_memory_region;
uint64_t* dst_memory_region;


int main(int argc, char* argv[]) {
	if (argc != 3) {
		std::cerr << "Usage: " << argv[0] << " [region_size_bytes] [trials]" << std::endl;
		return -1;
	}
	uint64_t region_size = std::stoll(argv[1]);
	uint64_t trials = std::stoll(argv[2]);
	const uint64_t target_bank_index = 0;
	const uint64_t banks_per_chip = 8;
	const uint64_t bank_size = 33554432;
    const uint64_t word_size = sizeof(uint64_t);

    std::cout << "Region Size: " << region_size << "B\n";
    std::cout << "Trials: " << trials << "\n";

	src_memory_region = (uint64_t*)malloc(region_size);
	dst_memory_region = (uint64_t*)malloc(region_size * banks_per_chip + bank_size * target_bank_index);  // multiplied by word_size only to remove seg-faults

    std::vector<float> bench_times(trials);

#if defined _DEBUG || ALLOW_CACHE
    for (uint64_t i = 0; i < region_size / word_size; i++) {
        src_memory_region[i] = 0;
        dst_memory_region[i] = 0;
    }
#endif

	for (uint64_t t = 0; t < trials; t++) {  // perform [trials] benchmarks...

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Horizontal Data Layout
		for (uint64_t i = 0; i < region_size / word_size; i++) {
            uint64_t data = src_memory_region[i];
            dst_memory_region[i*banks_per_chip +  0] = (data & 0xFF00000000000000) <<  0;
            dst_memory_region[i*banks_per_chip +  8] = (data & 0x00FF000000000000) <<  8;
            dst_memory_region[i*banks_per_chip + 16] = (data & 0x0000FF0000000000) << 16;
            dst_memory_region[i*banks_per_chip + 24] = (data & 0x000000FF00000000) << 24;
            dst_memory_region[i*banks_per_chip + 32] = (data & 0x00000000FF000000) << 32;
            dst_memory_region[i*banks_per_chip + 40] = (data & 0x0000000000FF0000) << 40;
            dst_memory_region[i*banks_per_chip + 48] = (data & 0x000000000000FF00) << 48;
            dst_memory_region[i*banks_per_chip + 56] = (data & 0x00000000000000FF) << 56;
        }
		/// End Horizontal Data Layout

		// Do some book-keeping
		std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> elapsed = end - start;
        bench_times[t] = elapsed.count();
	}

	// Evaluate the results
	float min = std::numeric_limits<float>::max();
	float max = std::numeric_limits<float>::min();
	float sum = 0;
	for (uint64_t t = 0; t < trials; t++) {
	    sum += bench_times[t];
	    min = bench_times[t] < min ? bench_times[t] : min;
	    max = bench_times[t] > max ? bench_times[t] : max;
	}
	float avg = sum / trials;

	std::cout << "Evaluation Results of " << trials << " trials:\n"
	    << "\tAverage: " << avg << "ms"
	    << " [" << min << ", " << max << "]"
	    << std::endl;

	return 0;
}
