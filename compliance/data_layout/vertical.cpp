#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <string>
#include <vector>
#include <limits>
#include <iostream>
#include <chrono>
#include <iomanip>


uint8_t* src_memory_region;
uint8_t* dst_memory_region;
uint8_t* tmp_chunk_region;


int main(int argc, char* argv[]) {
	if (argc != 5) {
	    std::cerr << "Usage: " << argv[0] << " [region_size_bytes] [trials] [chunk_size] [data_size]" << std::endl;
	    return -1;
	}
	uint64_t region_size = std::stoll(argv[1]);
	uint64_t trials = std::stoll(argv[2]);
	uint64_t chunk_size = std::stoll(argv[3]);
	uint64_t data_size = std::stoll(argv[4]);

	src_memory_region = (uint8_t*)malloc(region_size);
	dst_memory_region = (uint8_t*)malloc(region_size);
	tmp_chunk_region = (uint8_t*)malloc(chunk_size);

    std::vector<float> bench_times(trials);

    for(uint64_t i = data_size; i <= region_size; i+=data_size){
        src_memory_region[i - 1] = (uint8_t)((i/data_size)-1);
        std::cout << (int)src_memory_region[i - 1] << " ";
		    if ((i+1) % data_size == 0) {
		        std::cout << std::endl;
		    }
    }

    int z = 0;

	for (uint64_t t = 0; t < trials; t++) {  // perform [trials] benchmarks...

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Vertical Data Layout
		for (uint64_t i = 0; i < region_size / chunk_size; i+=1) {       // for each chunk...
		    int chunk_page = i / (data_size * 8);
            int chunk_bit_index = i % (data_size * 8);

            int start_data_index = chunk_page * chunk_size * 8;

            int byte_offset = chunk_bit_index / 8;
            int byte_bit = chunk_bit_index % 8;

            uint8_t tmp = 0;
            for (uint64_t j = 0; j < chunk_size * 8; j+=1) {  // for each bit per chunk
                int sub_start_index = start_data_index + j;
                int src_address = sub_start_index * data_size + byte_offset;

                uint8_t data = src_memory_region[src_address];
                tmp <<= 1;
                uint8_t bit = (data & (128 >> byte_bit)) > 0 ? 1 : 0;
                tmp += bit;

                if ((j + 1) % 8 == 0) {
                    dst_memory_region[z++] = tmp;
                    tmp = 0;
                }
            }
		}
		/// End Vertical Data Layout

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
