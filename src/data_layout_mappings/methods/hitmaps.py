from hardware import Bank


def place_hitmap(
        base_row: int,
        row_count: int,
        bank: Bank,
        value: bool,
        limit: int
):
    """
    Places a hitmap within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [      bits per record in limit hitmap      ] -
    - [     bits     ]                              -
    -                                               -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """

    total_mapped = 0
    for hitmap_sub_row in range(row_count):
        # Did we reach our limit
        if total_mapped >= limit:
            break
        # Is this a full hitmap row?
        elif total_mapped + bank.hardware_configuration.row_buffer_size_bytes * 8 <= limit:
            total_mapped += bank.hardware_configuration.row_buffer_size_bytes * 8
            bank.set_raw_row(
                base_row + hitmap_sub_row,
                ((2 ** (bank.hardware_configuration.row_buffer_size_bytes * 8)) - 1) if value else 0
            )
        # Is this a row that needs padding
        elif limit - total_mapped < bank.hardware_configuration.row_buffer_size_bytes * 8:
            remainder = limit - total_mapped
            null_remainder = bank.hardware_configuration.row_buffer_size_bytes * 8 - remainder

            segmented_row = 0
            segmented_row += ((2 ** remainder) - 1) if value else 0
            segmented_row <<= null_remainder
            segmented_row += ((2 ** null_remainder) - 1) if not value else 0

            bank.set_raw_row(
                base_row + hitmap_sub_row,
                segmented_row
            )

            total_mapped += remainder
