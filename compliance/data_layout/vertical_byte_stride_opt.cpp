// compliance.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

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

    std::cout << "Region Size: " << region_size << "B\n";
    std::cout << "Trials: " << trials << "\n";
    std::cout << "Chunk Size: " << chunk_size << "B\n";
    std::cout << "Data Size: " << data_size << "B\n";


    src_memory_region = (uint8_t*)malloc(region_size);
    dst_memory_region = (uint8_t*)malloc(region_size);
    tmp_chunk_region = (uint8_t*)malloc(chunk_size);

    std::vector<float> bench_times(trials);

    for (uint64_t i = 0; i < region_size; i++) {
        src_memory_region[i] = 0;
        dst_memory_region[i] = 0;
    }
    for (uint64_t i = data_size; i <= region_size; i += data_size) {
        src_memory_region[i - 1] = (uint8_t)((i / data_size) - 1);
    }

#ifdef _DEBUG
    std::cout << std::endl;
    for (uint64_t i = 0; i < region_size; i++) {
        std::cout << std::hex << std::setfill('0') << std::setw(2) << (int)src_memory_region[i] << " ";
        if ((i + 1) % chunk_size == 0) {
            std::cout << std::endl;
        }
    }
#endif

    int data_size_by_eight = data_size * 8;
    for (uint64_t t = 0; t < trials; t++) {
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

        /// Begin Vertical Data Layout
        int num_vertical_strips = region_size / data_size_by_eight;
        for (uint64_t i = 0; i < num_vertical_strips; i += 1) {
            int chunk_slice_index = i % chunk_size;
            int chunk_segment_index = i / chunk_size;
            int i_by_eight = i << 3;  // i * 8

            int start_data_index = ((chunk_segment_index * data_size_by_eight) << 3) + chunk_slice_index;

            for (uint64_t j = 0; j < data_size_by_eight; j += 1) {
                int sub_start_index = start_data_index + j * chunk_size;
                int tmp = 0;
                int record_byte_offset = j >> 3;  // j / 8
                int record_bit = j & 7;  // j % 8

                tmp += ((src_memory_region[(i_by_eight + 0) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 7;
                tmp += ((src_memory_region[(i_by_eight + 1) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 6;
                tmp += ((src_memory_region[(i_by_eight + 2) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 5;
                tmp += ((src_memory_region[(i_by_eight + 3) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 4;
                tmp += ((src_memory_region[(i_by_eight + 4) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 3;
                tmp += ((src_memory_region[(i_by_eight + 5) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 2;
                tmp += ((src_memory_region[(i_by_eight + 6) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 1;
                tmp += ((src_memory_region[(i_by_eight + 7) * data_size + record_byte_offset] & (128 >> record_bit)) >> (7 - record_bit)) << 0;

                dst_memory_region[sub_start_index] = tmp;
            }
        }
        /// End Vertical Data Layout

        std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;
        bench_times[t] = elapsed.count();

#ifdef _DEBUG
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
