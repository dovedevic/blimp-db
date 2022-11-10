#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <string>
#include <vector>
#include <limits>
#include <iostream>
#include <chrono>


uint64_t* hitmap_a;
uint64_t* hitmap_b;


int main(int argc, char* argv[]) {
	if (argc != 3) {
		std::cerr << "Usage: " << argv[0] << " [hitmap_size_bytes] [trials]" << std::endl;
		return -1;
	}
	uint64_t hitmap_size = std::stoll(argv[1]);  // the total hitmap byte size
	uint64_t trials = std::stoll(argv[2]);       // the number of trials to perform the placement study

    std::cout << "Hitmap Size: " << hitmap_size << "B\n";
    std::cout << "Trials: " << trials << "\n";

    // Set up our memory byte regions
	hitmap_a = (uint64_t*)malloc(hitmap_size);
	hitmap_b = (uint64_t*)malloc(hitmap_size);

    // Book keeping for trials
    std::vector<float> bench_times(trials);

    // Zero out the memory region, not particularly needed but done for easy debugging
#if defined _DEBUG || ALLOW_CACHE
    for (uint64_t i = 0; i < region_size; i++) {
        hitmap_a[i] = 0;
        hitmap_b[i] = 0;
    }
#endif

    // perform [trials] benchmarks...
	for (uint64_t t = 0; t < trials; t++) {

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Logical AND
		for (uint64_t i = 0; i < hitmap_size / sizeof(uint64_t); i+=1) {  // for each word...
			hitmap_a[i] = hitmap_a[i] & hitmap_b[i];
		}
		/// End Logical AND

		// Do some book-keeping
		std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::nano> elapsed = end - start;
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

    std::cout.precision(0);
	std::cout << "Evaluation Results of " << trials << " trials:\n"
	    << "\tAverage: " << std::fixed << avg << "ns"
	    << " [" << min << ", " << max << "]"
	    << std::endl;

	return 0;
}
