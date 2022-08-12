from generators import DatabaseRecordGenerator
from hardware import Bank


def perform_packed_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
):
    """
    Places records in packed fashion within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [     record     ][     record     ][     reco-
    - rd     ]                                      -
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
    temporary_row = 0
    bytes_in_temporary = 0
    generating_at_row = base_row

    for record in record_generator.get_raw_records():
        if generating_at_row >= base_row + row_count:
            break  # done placing

        temporary_row <<= record_generator.record_size_bytes * 8
        temporary_row += record
        bytes_in_temporary += record_generator.record_size_bytes

        if bytes_in_temporary == bank.hardware_configuration.row_buffer_size_bytes:
            # Place and reset
            bank.set_raw_row(generating_at_row, temporary_row)
            temporary_row = 0
            bytes_in_temporary = 0
            generating_at_row += 1
        elif bytes_in_temporary > bank.hardware_configuration.row_buffer_size_bytes:
            # Overshot, chunk
            bytes_overshot = bytes_in_temporary - bank.hardware_configuration.row_buffer_size_bytes
            overshot_mask = (2 ** (bytes_overshot * 8)) - 1
            overshot_row = temporary_row & overshot_mask

            temporary_row >>= bytes_overshot * 8
            bank.set_raw_row(generating_at_row, temporary_row)

            temporary_row = overshot_row
            bytes_in_temporary = bytes_overshot
            generating_at_row += 1
        else:
            # Undershot, continue loading data
            pass

    # Either no more records to process or we ran out of rows
    if generating_at_row < base_row + row_count:
        # Place what we have left
        bytes_left_in_row = bank.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary
        temporary_row <<= bytes_left_in_row * 8
        bank.set_raw_row(generating_at_row, temporary_row)
