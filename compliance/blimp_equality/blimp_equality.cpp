#include <iostream>
#include <stdio.h>
#include <random>
#include <string.h>
#include <fstream>
#include <iomanip>

// Meta Directives
//#define DEBUG

// Hardware Specifics
#define BANK_SIZE_BYTES 33554432
#define ROW_BUFFER_BYTES 1024
#define BANK_ROWS 32768  // bank size / row buffer

// Database Specifics
#define HITMAP_COUNT 3
#define INDEX_SIZE_BYTES 8
#define RECORD_SIZE_BYTES 512
#define DATA_SIZE_BYTES 504  // record size - index size

// Layout Specifics
#define ROWS_FOR_RECORDS 32220
#define ROWS_FOR_HITMAPS 24
#define RECORDS_PROCESSABLE 64440
#define HITMAP_BASE_ROW 32734
#define RECORD_BASE_ROW 514

// Query Specifics
#define PI_SUBINDEX_OFFSET_BYTES 0
#define PI_ELEMENT_SIZE_BYTES 8
#define _VALUE {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
#define NEGATE 0
#define HITMAP_INDEX 1

#define min(a,b) ((a)<(b)?(a):(b))
#define max(a,b) ((a)>(b)?(a):(b))


uint8_t memory[BANK_ROWS][ROW_BUFFER_BYTES];
uint8_t rowbuffer[ROW_BUFFER_BYTES];
int current_row;
uint8_t VALUE[] = _VALUE;

uint8_t v0[ROW_BUFFER_BYTES];

void load_row(int row_index) {
    for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
        rowbuffer[byte] = memory[row_index][byte];
    }
    current_row = row_index;
}

void store_v0(int row_index) {
    for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
        memory[row_index][byte] = v0[byte];
    }
}

void create_memory() {
    for (int row = 0; row < BANK_ROWS; row++) {
        // Utility rows
        if (row < RECORD_BASE_ROW) {
            for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
                memory[row][byte] = 0;  // don't care
            }
        }
        // Data row generation
        else if (row < HITMAP_BASE_ROW) {
            for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
                memory[row][byte] = (uint8_t)(rand() % 256);  // random data
            }
        }
        // Hitmap row generation and other utilities
        else if (row < HITMAP_BASE_ROW + ROWS_FOR_HITMAPS) {
            for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
                memory[row][byte] = 0xFF;  // initialize all hitmaps to true
            }
        }
        else {
            for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
                memory[row][byte] = 0;  // everything else is null
            }
        }
    }

    // Place a sentinel
    memory[HITMAP_BASE_ROW - 10][0] = 0;
    memory[HITMAP_BASE_ROW - 10][1] = 0;
    memory[HITMAP_BASE_ROW - 10][2] = 0;
    memory[HITMAP_BASE_ROW - 10][3] = 0;
    memory[HITMAP_BASE_ROW - 10][4] = 0;
    memory[HITMAP_BASE_ROW - 10][5] = 0;
    memory[HITMAP_BASE_ROW - 10][6] = 0;
    memory[HITMAP_BASE_ROW - 10][7] = 0;

    // Initialize v0 to zero
    for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
        v0[byte] = 0x00;
    }
    // Set the row buffer initially to row zero
    load_row(0);
}

void dump_memory(std::string path) {
    std::ofstream dump_file;
    dump_file.open(path);
    for (int row = 0; row < BANK_ROWS; row++) {
        dump_file << std::hex << std::setfill('0') << std::setw(8) << (uint8_t) row * ROW_BUFFER_BYTES << ":  ";
        for (int byte = 0; byte < ROW_BUFFER_BYTES; byte++) {
            dump_file << std::hex << std::setfill('0') << std::setw(2) << unsigned(memory[row][byte]) << " ";
        }
        dump_file << "\n";
    }
    dump_file.close();
}


int main()
{
    printf("Creating memory...\n");
    create_memory();

    printf("Starting compliance...\n");
    int rows_per_hitmap = ROWS_FOR_HITMAPS / HITMAP_COUNT;
    int targeted_hitmap_base = HITMAP_BASE_ROW + rows_per_hitmap * HITMAP_INDEX;
    
    int records_per_row = ROW_BUFFER_BYTES / RECORD_SIZE_BYTES;
    int rows_per_record = RECORD_SIZE_BYTES / ROW_BUFFER_BYTES;

    uint8_t bitmap = 0x0;
    int bitdex = 0;
    int hitdex = 0;


    // Iterate over all records
    for (int record_index = 0; record_index < RECORDS_PROCESSABLE; record_index += 1) {
        int row, offset = 0;

        // Calculate the row this record starts in
        // Calculate the offset the record starts in the row
        
        if (records_per_row <= 0) { // Are we dealing with multi-rows per record
            row = RECORD_BASE_ROW + record_index * rows_per_record;
            offset = 0;
        }
        else { // Are we dealing with multi-records per row
            row = RECORD_BASE_ROW + record_index / records_per_row;
            offset = record_index % records_per_row * RECORD_SIZE_BYTES;
        }
        
        // Fetch the record
        if (current_row != row) {
            load_row(row);
        }

        // Point to the index
        int index_sub_offset = offset + PI_SUBINDEX_OFFSET_BYTES;

        // Perform the operation
        int equal = memcmp(rowbuffer + index_sub_offset, VALUE, PI_ELEMENT_SIZE_BYTES);

#ifdef DEBUG
        for (int z = 0; z < PI_ELEMENT_SIZE_BYTES; z++) {
            std::cout << std::hex << std::setfill('0') << std::setw(2) << unsigned(rowbuffer[index_sub_offset + z]) << " ";
        }
        std::cout << " = ";
        for (int z = 0; z < PI_ELEMENT_SIZE_BYTES; z++) {
            std::cout << std::hex << std::setfill('0') << std::setw(2) << unsigned(VALUE[z]) << " ";
        }
#endif // DEBUG

        // Update bookeeping
        bitmap <<= 1;
        bitmap += equal == 0 ? 1 : 0;
        bitdex += 1;

#ifdef DEBUG
        std::cout << "? " << (equal == 0 ? "yes" : "no") << "\nbitdex=" << bitdex << " bitmap=" << std::hex << std::setfill('0') << std::setw(2) << bitmap << "\n";
#endif // DEBUG

        // Manage bookeeping
        if (bitdex % 8 == 0) {
            v0[hitdex % ROW_BUFFER_BYTES] = bitmap;
            hitdex += 1;
            // Filled v0 hitmap? Save it
            if (hitdex % ROW_BUFFER_BYTES == 0) {
                store_v0(targeted_hitmap_base + ((hitdex - 1) / ROW_BUFFER_BYTES));
            }
        }
    }

    // All records finished processing, save last row
    while (hitdex % ROW_BUFFER_BYTES != 0) {
        bitmap <<= 1;
        bitmap += 1;
        bitdex += 1;

        // Manage bookeeping
        if (bitdex % 8 == 0) {
            v0[hitdex % ROW_BUFFER_BYTES] = bitmap;
            hitdex += 1;
        }
    }
    store_v0(targeted_hitmap_base + ((hitdex - 1) / ROW_BUFFER_BYTES));

    printf("Dumping data...\n");
    dump_memory("test.memdump");

    return 0;
}
