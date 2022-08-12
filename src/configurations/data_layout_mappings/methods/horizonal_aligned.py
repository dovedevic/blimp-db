from generators import DatabaseRecordGenerator
from hardware import Bank


def perform_aligned_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        total_records_processable: int
):
    """
    Places records in row-buffer-aligned chunks within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [     record     ][     record     ]          -
    - [     record     ]                            -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ... [     record     ][     record     ]     -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """
    # See if we are in a multi-record-per-row configuration or multi-row-per-record
    records_per_row = bank.hardware_configuration.row_buffer_size_bytes \
        // record_generator.record_size_bytes
    records_placed = 0

    if records_per_row > 0:
        # Multiple (or one) records per row; record size <= row buffer
        for row_index in range(row_count):
            # Construct a temporary location for the row / records
            records_in_row = []

            # Fetch all records in this row, if we are at the end, null pad with zeros
            for sub_record_index in range(records_per_row):
                if records_placed < total_records_processable:
                    records_in_row.append(
                        record_generator.get_raw_record(row_index * records_per_row + sub_record_index)
                    )
                    records_placed += 1
                else:
                    records_in_row.append(record_generator.get_null_record())

            # Construct the raw value byte array for the hardware
            raw_value = 0
            for raw in records_in_row:
                raw_value <<= (record_generator.record_size_bytes * 8)
                raw_value |= raw

            # Store this row with all the records placed
            bank.set_raw_row(
                base_row + row_index,
                raw_value
            )
    else:
        # Multiple rows per record; row buffer < record size
        rows_per_record = record_generator.record_size_bytes \
                          // bank.hardware_configuration.row_buffer_size_bytes

        # For all placeable records, extract row-buffer sized chunks and store them
        for record_index in range(total_records_processable):
            # Fetch/Generate the records
            record = record_generator.get_raw_record(record_index)

            # Chunk the record
            for sub_row_index in range(rows_per_record):
                # Construct the mask for each chunk
                row_buffer_mask = (2 ** (bank.hardware_configuration.row_buffer_size_bytes * 8)) - 1
                # Move the mask to the appropriate chunk
                record_mask = row_buffer_mask << \
                    (rows_per_record - 1 - sub_row_index) * \
                    (bank.hardware_configuration.row_buffer_size_bytes * 8)
                # Mask the record to extract the chunk
                masked_record = record & record_mask
                # Realign the chunk
                record_chunk = masked_record >> \
                    (rows_per_record - 1 - sub_row_index) * \
                    (bank.hardware_configuration.row_buffer_size_bytes * 8)
                # Save the chunk
                bank.set_raw_row(
                    base_row + (record_index * rows_per_record) + sub_row_index,
                    record_chunk
                )
