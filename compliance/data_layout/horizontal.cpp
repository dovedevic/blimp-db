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
	if (argc <= 1 || argc >= 4) {
		std::cerr << "Usage: " << argv[0] << " [region_size_bytes] [trials]" << std::endl;
		return -1;
	}
	uint64_t region_size = std::stoll(argv[1]);
	uint64_t trials = std::stoll(argv[2]);

	const uint64_t word_size = sizeof(uint64_t);
	const uint64_t bank_index = 0;
	const uint64_t target_bank = bank_index * word_size;
	const uint64_t banks_per_chip = 8;

	src_memory_region = (uint64_t*)malloc(region_size);
	dst_memory_region = (uint64_t*)malloc(region_size * word_size);  // multiplied by word_size only to remove seg-faults

    std::vector<float> bench_times(trials);

	for (uint64_t t = 0; t < trials; t++) {  // perform [trials] benchmarks...

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Horizontal Data Layout
		for (uint64_t i = 0; i < region_size / word_size; i+=word_size) {       // for each word...
			uint64_t data = src_memory_region[i];                                   // fetch the word
			for (uint64_t j = 0; j < banks_per_chip * word_size; j+=word_size) {    // for each byte in the word...
			    dst_memory_region[i+j] =                                                // send each byte to a bank-
			        ((data & (0xFF00000000000000 >> j)) <<  j) >> target_bank;          // aligned address
			}
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
