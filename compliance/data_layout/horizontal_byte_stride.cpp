#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <string>
#include <vector>
#include <limits>
#include <iostream>
#include <chrono>


uint8_t* src_memory_region;
uint8_t* dst_memory_region;


int main(int argc, char* argv[]) {
	if (argc != 3) {
		std::cerr << "Usage: " << argv[0] << " [region_size_bytes] [trials]" << std::endl;
		return -1;
	}
	uint64_t region_size = std::stoll(argv[1]);  // the total region byte size
	uint64_t trials = std::stoll(argv[2]);       // the number of trials to perform the placement study
    const uint64_t target_bank_index = 0;
	const uint64_t banks_per_chip = 8;
	const uint64_t bank_size = 33554432;

    std::cout << "Region Size: " << region_size << "B\n";
    std::cout << "Trials: " << trials << "\n";

    // Set up our memory byte regions
	src_memory_region = (uint8_t*)malloc(region_size);
	dst_memory_region = (uint8_t*)malloc(region_size * banks_per_chip + bank_size * target_bank_index);  // multiplied by banks_per_chip only to remove seg-faults

    // Book keeping for trials
    std::vector<float> bench_times(trials);

    // Zero out the memory region, not particularly needed but done for easy debugging
#if defined _DEBUG || ALLOW_CACHE
    for (uint64_t i = 0; i < region_size; i++) {
        src_memory_region[i] = 0;
        dst_memory_region[i] = 0;
    }
#endif

    // perform [trials] benchmarks...
	for (uint64_t t = 0; t < trials; t++) {

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Horizontal Data Layout
		for (uint64_t i = 0; i < region_size; i+=1) {  // for each byte...
			dst_memory_region[i * banks_per_chip + bank_size * target_bank_index] = src_memory_region[i];
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
