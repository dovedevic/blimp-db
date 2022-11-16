#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <string>
#include <vector>
#include <limits>
#include <iostream>
#include <chrono>

struct hash_set_slot {
    bool is_filled;
    uint32_t value;
};

uint32_t hash(uint32_t x) {
    x ^= x >> 16;
    x *= 0x21f0aaad;
    x ^= x >> 15;
    x *= 0xd35a2d97;
    x ^= x >> 15;
    return x;
}

uint8_t* hitmap;
uint32_t* build_data;
uint32_t* probe_data;
hash_set_slot* hash_set;

int main(int argc, char* argv[]) {
	if (argc != 4) {
		std::cerr << "Usage: " << argv[0] << " [build_indices] [probe_indices] [trials]" << std::endl;
		return -1;
	}

	uint64_t build_indices = std::stoll(argv[1]);                 // the total number of hash build indices
	uint64_t probe_indices = std::stoll(argv[2]);                 // the total number of hash probe indices
	uint64_t trials = std::stoll(argv[3]);                        // the number of trials to perform the placement study

	if (ceil(log2(build_indices)) != floor(log2(build_indices))) {
	    std::cerr << "build_indices argument must be a power of 2" << std::endl;
		return -1;
	}

	uint64_t build_data_size = build_indices * sizeof(uint32_t);  // the total build data byte size
	uint64_t probe_data_size = probe_indices * sizeof(uint32_t);  // the total probe data byte size
	uint64_t num_slots = 2 * build_indices;                       // the total number of hash set slots
	uint64_t hash_set_size = num_slots * sizeof(hash_set_slot);   // the total hash set byte size
	uint64_t shift_amount = 32 - (int)log2(num_slots);
	uint64_t mask = UINT32_MAX >> shift_amount;
	uint64_t hitmap_size = probe_indices / 8 + (probe_indices % 8 != 0);

    std::cout << "Hash Build Indices: " << build_indices << " records\n";
    std::cout << "Hash Probe Indices: " << probe_indices << " records\n";
    std::cout << "Hash Build Data Size: " << build_data_size << "B\n";
    std::cout << "Hash Probe Data Size: " << probe_data_size << "B\n";
    std::cout << "Hash Set Size: " << hash_set_size << "B\n";
    std::cout << "Trials: " << trials << "\n";

    // Set up our memory byte regions
	build_data = (uint32_t*)malloc(build_data_size);
	probe_data = (uint32_t*)malloc(probe_data_size);
	hash_set = (hash_set_slot*)malloc(hash_set_size);
	hitmap = (uint8_t*)malloc(hitmap_size);

    // Book keeping for trials
    std::vector<float> bench_times(trials);

    // Populate build and probe data
    for (int i = 0; i < build_indices; i++) { build_data[i] = i; }
    for (int i = 0; i < probe_indices; i++) { probe_data[i] = i % (build_indices * 7); }  // 14% selectivity (configurable)
    for (int i = 0; i < hitmap_size; i++) { hitmap[i] = 0; }

    // perform [trials] benchmarks for hash build...
	for (uint64_t t = 0; t < trials; t++) {

	    for (int i = 0; i < num_slots; i++) { hash_set[i].is_filled = false; }

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Hash Build

        for (int i = 0; i < build_indices; i++) {
            uint32_t hash_slot = hash(build_data[i]) >> shift_amount;
            while(hash_set[hash_slot].is_filled) {
                hash_slot = (hash_slot + 1) & mask;
            }
            hash_set[hash_slot].is_filled = true;
            hash_set[hash_slot].value = build_data[i];
        }

		/// End Hash Build

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

    std::cout.precision(0);
	std::cout << "Hash Build Evaluation Results of " << trials << " trials:\n"
	    << "\tAverage: " << std::fixed << avg << "ms"
	    << " [" << min << ", " << max << "]"
	    << std::endl;

	// perform [trials] benchmarks for hash probe...
	for (uint64_t t = 0; t < trials; t++) {

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Hash Probe
        for (int i = 0; i < probe_indices; i++) {
            uint32_t hash_slot = hash(probe_data[i]) >> shift_amount;
            while(hash_set[hash_slot].is_filled && hash_set[hash_slot].value != probe_data[i]) {
                hash_slot = (hash_slot + 1) & mask;
            }
            if (hash_set[hash_slot].is_filled) {
                hitmap[i / 8] |= 1 << (i % 8);
            }
        }

		/// End Hash Probe

		// Do some book-keeping
		std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> elapsed = end - start;
        bench_times[t] = elapsed.count();
	}

	// Evaluate the results
	min = std::numeric_limits<float>::max();
	max = std::numeric_limits<float>::min();
	sum = 0;
	for (uint64_t t = 0; t < trials; t++) {
	    sum += bench_times[t];
	    min = bench_times[t] < min ? bench_times[t] : min;
	    max = bench_times[t] > max ? bench_times[t] : max;
	}
	avg = sum / trials;

    std::cout.precision(0);
	std::cout << "Hash Probe Evaluation Results of " << trials << " trials:\n"
	    << "\tAverage: " << std::fixed << avg << "ms"
	    << " [" << min << ", " << max << "]"
	    << std::endl;

	return 0;
}
