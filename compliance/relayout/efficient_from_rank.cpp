#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <string>
#include <vector>
#include <limits>
#include <iostream>
#include <chrono>
#include <cassert>


uint64_t* memory_region;


int main(int argc, char* argv[]) {
	if (argc != 3) {
		std::cerr << "Usage: " << argv[0] << " [single rank bank memory size] [trials]" << std::endl;
		return -1;
	}
	uint64_t region_size = std::stoll(argv[1]);  // the total region byte size per bank
	uint64_t trials = std::stoll(argv[2]);       // the number of trials to perform the placement study

	std::cout << "Bank Region Size: " << region_size << "B\n";
	std::cout << "Total Region Size: " << region_size * 8 << "B\n";
    std::cout << "Trials: " << trials << "\n";

    // Set up our memory byte regions
	memory_region = (uint64_t*)malloc(region_size * sizeof(uint64_t));

	// testing sentinels
	// word set #1
	// memory_region[0] = 0xFFEEDDCCBBAA9988;
	// memory_region[1] = 0xFFEEDDCCBBAA9988;
	// memory_region[2] = 0xFFEEDDCCBBAA9988;
	// memory_region[3] = 0xFFEEDDCCBBAA9988;
	// memory_region[4] = 0xFFEEDDCCBBAA9988;
	// memory_region[5] = 0xFFEEDDCCBBAA9988;
	// memory_region[6] = 0xFFEEDDCCBBAA9988;
	// memory_region[7] = 0xFFEEDDCCBBAA9988;
	// word set #2
	// memory_region[8] = 0x7766554433221100;
	// memory_region[9] = 0x7766554433221100;
	// memory_region[10] = 0x7766554433221100;
	// memory_region[11] = 0x7766554433221100;
	// memory_region[12] = 0x7766554433221100;
	// memory_region[13] = 0x7766554433221100;
	// memory_region[14] = 0x7766554433221100;
	// memory_region[15] = 0x7766554433221100;
	// relayed word set #1
	// bank word 0 = 18446744073709551615   0xFFFFFFFFFFFFFFFF
    // bank word 1 = 17216961135462248174   0xEEEEEEEEEEEEEEEE
    // bank word 2 = 15987178197214944733   0xDDDDDDDDDDDDDDDD
    // bank word 3 = 14757395258967641292   0xCCCCCCCCCCCCCCCC
    // bank word 4 = 13527612320720337851   0xBBBBBBBBBBBBBBBB
    // bank word 5 = 12297829382473034410   0xAAAAAAAAAAAAAAAA
    // bank word 6 = 11068046444225730969   0x9999999999999999
    // bank word 7 = 9838263505978427528    0x8888888888888888
    // relayed word set #2
    // bank word 0 = 8608480567731124087    0x7777777777777777
    // bank word 1 = 7378697629483820646    0x6666666666666666
    // bank word 2 = 6148914691236517205    0x5555555555555555
    // bank word 3 = 4919131752989213764    0x4444444444444444
    // bank word 4 = 3689348814741910323    0x3333333333333333
    // bank word 5 = 2459565876494606882    0x2222222222222222
    // bank word 6 = 1229782938247303441    0x1111111111111111
    // bank word 7 = 0                      0x0000000000000000

    // Book keeping for trials
    std::vector<float> bench_times(trials);

    // words that are extracted at the rank level, chip wide, bank serially
    uint64_t relay_words[8] = {0, 0, 0, 0, 0, 0, 0, 0};

    // perform [trials] benchmarks...
	for (uint64_t t = 0; t < trials; t++) {

        // Do some perf-timing
        std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

	    /// Begin Rank-level Data Relayout from the banks
	    // Bytes of a 64b word are striped across the chips of a rank, then serially in the banks
	    // Assuming a 8 chip-per-rank, 8 bank-per-chip system
	    // fetching one word fetches a byte from 8 different chips, same bank index in all chips
	    // this word has a byte from each bank word
	    // fetching 8 contiguous words fetches all bytes from 8 different banks, allowing for 1 word per bank, or 8
		for (uint64_t i = 0; i < region_size / 8; i+=1) {

			// most significant byte belongs to word0, least significant byte belongs to word7
			// relay_word_* = (byte 7) (byte 6) ... (byte 0)

			// chip_byte_word_0 = (byte 7 word 0) (byte 7 word 1) ... (byte 7 word 7)
			// chip_byte_word_1 = (byte 6 word 0) (byte 6 word 1) ... (byte 6 word 7)
			// ...
			// chip_byte_word_7 = (byte 0 word 0) (byte 0 word 1) ... (byte 0 word 7)

			// fetched striped words
			uint64_t chip_byte_word_0 = memory_region[i*8 + 0];
			uint64_t chip_byte_word_1 = memory_region[i*8 + 1];
			uint64_t chip_byte_word_2 = memory_region[i*8 + 2];
			uint64_t chip_byte_word_3 = memory_region[i*8 + 3];
			uint64_t chip_byte_word_4 = memory_region[i*8 + 4];
			uint64_t chip_byte_word_5 = memory_region[i*8 + 5];
			uint64_t chip_byte_word_6 = memory_region[i*8 + 6];
			uint64_t chip_byte_word_7 = memory_region[i*8 + 7];

            // initialize our relayout words
            // relay_words = bank_0_word_i, bank_1_word_i, ..., bank_7_word_i

			for (uint8_t j = 0; j < 8; j++) {

			    relay_words[7-j] = ((uint64_t)((uint8_t)chip_byte_word_0) << 56) +
			                       ((uint64_t)((uint8_t)chip_byte_word_1) << 48) +
			                       ((uint64_t)((uint8_t)chip_byte_word_2) << 40) +
			                       ((uint64_t)((uint8_t)chip_byte_word_3) << 32) +
			                       ((uint64_t)((uint8_t)chip_byte_word_4) << 24) +
			                       ((uint64_t)((uint8_t)chip_byte_word_5) << 16) +
			                       ((uint64_t)((uint8_t)chip_byte_word_6) << 8) +
			                       ((uint64_t)((uint8_t)chip_byte_word_7) << 0);

			    chip_byte_word_0 >>= 8;
                chip_byte_word_1 >>= 8;
                chip_byte_word_2 >>= 8;
                chip_byte_word_3 >>= 8;
                chip_byte_word_4 >>= 8;
                chip_byte_word_5 >>= 8;
                chip_byte_word_6 >>= 8;
                chip_byte_word_7 >>= 8;
			}

			// assert(chip_byte_word_0 == 0);
            // assert(chip_byte_word_1 == 0);
            // assert(chip_byte_word_2 == 0);
            // assert(chip_byte_word_3 == 0);
            // assert(chip_byte_word_4 == 0);
            // assert(chip_byte_word_5 == 0);
            // assert(chip_byte_word_6 == 0);
            // assert(chip_byte_word_7 == 0);
            // std::cout << "\tbank word 0 = " << relay_words[0] << "\n";
			// std::cout << "\tbank word 1 = " << relay_words[1] << "\n";
			// std::cout << "\tbank word 2 = " << relay_words[2] << "\n";
			// std::cout << "\tbank word 3 = " << relay_words[3] << "\n";
			// std::cout << "\tbank word 4 = " << relay_words[4] << "\n";
			// std::cout << "\tbank word 5 = " << relay_words[5] << "\n";
			// std::cout << "\tbank word 6 = " << relay_words[6] << "\n";
			// std::cout << "\tbank word 7 = " << relay_words[7] << "\n";

			// at this point relay_words contains 8 words, word i from bank 0 to bank 7
			// do something meaningful with that word of data
            // for the test we'll do nothing

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
