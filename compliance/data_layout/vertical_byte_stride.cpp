#include <iostream>
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


int main(int argc, char* argv[]) {
    // Check if we have enough args
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " [region_size_bytes] [trials] [chunk_size] [data_size]" << std::endl;
        return -1;
    }
    uint64_t region_size = std::stoll(argv[1]);  // the total region byte size
    uint64_t trials = std::stoll(argv[2]);       // the number of trials to perform the placement study
    uint64_t chunk_size = std::stoll(argv[3]);   // the width of the chunk (row buffer)
    uint64_t data_size = std::stoll(argv[4]);    // the length of the data in bytes (4 for 32b integers)

    std::cout << "Region Size: " << region_size << "B\n";
    std::cout << "Trials: " << trials << "\n";
    std::cout << "Chunk Size: " << chunk_size << "B\n";
    std::cout << "Data Size: " << data_size << "B\n";

    // Set up our memory byte regions
    src_memory_region = (uint8_t*)malloc(region_size);
    dst_memory_region = (uint8_t*)malloc(region_size);

    // Book keeping for trials
    std::vector<float> bench_times(trials);

    // Zero out the memory region, not particularly needed but done for easy debugging
    for (uint64_t i = 0; i < region_size; i++) {
        src_memory_region[i] = 0;
        dst_memory_region[i] = 0;
    }

    // Set up some dummy data to ensure it is working, again not particularly needed
    for (uint64_t i = data_size; i <= region_size; i += data_size) {
        src_memory_region[i - 1] = (uint8_t)((i / data_size) - 1);
    }

#ifdef _DEBUG
    // Dump the memory region by chunk size after initial placement to confirm placement
    std::cout << std::endl;
    for (uint64_t i = 0; i < region_size; i++) {
        std::cout << std::hex << std::setfill('0') << std::setw(2) << (int)src_memory_region[i] << " ";
        if ((i + 1) % chunk_size == 0) {
            std::cout << std::endl;
        }
    }
#endif

    // perform [trials] benchmarks...
    for (uint64_t t = 0; t < trials; t++) {

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

        /// Begin Vertical Data Layout
        int num_vertical_strips = region_size / (data_size * 8);
        for (uint64_t i = 0; i < num_vertical_strips; i += 1) {  // for each data slice...
            int chunk_slice_index = i % chunk_size;
            int chunk_segment_index = i / chunk_size;

            int start_data_index = 8 * chunk_segment_index * (data_size * 8) + chunk_slice_index;

            for (uint64_t j = 0; j < data_size * 8; j += 1) {  // for each byte in the slice
                int sub_start_index = start_data_index + j * chunk_size;
                int tmp = 0;
                for (uint64_t k = 0; k < 8; k += 1) {  // for each bit in this byte
                    int record_index = i * 8 + k;
                    int record_base_address = record_index * data_size;
                    int record_byte_offset = j / 8;
                    int record_address = record_base_address + record_byte_offset;
                    int record_bit = j % 8;

                    uint8_t data = src_memory_region[record_address];
                    tmp <<= 1;
                    uint8_t bit = (data & (128 >> record_bit)) >> (7 - record_bit);
                    tmp += bit;
                }

                dst_memory_region[sub_start_index] = tmp;
            }
        }
        /// End Vertical Data Layout

        // Do some book-keeping
        std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;
        bench_times[t] = elapsed.count();

#ifdef _DEBUG
        // Dump the memory region by chunk size after final placement to confirm layout
        std::cout << std::endl;
        for (uint64_t i = 0; i < region_size; i++) {
            std::cout << std::hex << std::setfill('0') << std::setw(2) << (int)dst_memory_region[i] << " ";
            if ((i + 1) % chunk_size == 0) {
                std::cout << std::endl;
            }
        }
#endif
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
