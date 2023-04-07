from src.generators import DatabaseRecordGenerator
from src.hardware import Bank


def perform_record_packed_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
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

    for record_index in range(limit):
        if generating_at_row >= base_row + row_count:
            break  # done placing

        temporary_row <<= record_generator.record_size_bytes * 8
        temporary_row += record_generator.get_raw_record(record_index)
        bytes_in_temporary += record_generator.record_size_bytes

        if bytes_in_temporary == bank.hardware_configuration.row_buffer_size_bytes:
            # Place and reset
            bank.set_raw_row(generating_at_row, temporary_row)
            temporary_row = 0
            bytes_in_temporary = 0
            generating_at_row += 1
        elif bytes_in_temporary >= bank.hardware_configuration.row_buffer_size_bytes:
            # Overshot, chunk
            while bytes_in_temporary >= bank.hardware_configuration.row_buffer_size_bytes and \
                    generating_at_row < base_row + row_count:
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
    if generating_at_row < base_row + row_count and bytes_in_temporary > 0:
        # Place what we have left
        bytes_left_in_row = bank.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary
        for _ in range(bytes_left_in_row):
            temporary_row <<= 8
            temporary_row += bank.default_byte_value
        bank.set_raw_row(generating_at_row, temporary_row)


def perform_index_packed_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
):
    """
    Places record indices in packed fashion within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [  index  ][  index  ][  index  ][  index  ][ -
    - index  ]                                      -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ... [  index  ][  index  ]                   -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """
    temporary_row = 0
    bytes_in_temporary = 0
    generating_at_row = base_row

    for record_index in range(limit):
        if generating_at_row >= base_row + row_count:
            break  # done placing

        temporary_row <<= record_generator.index_size_bytes * 8
        temporary_row += record_generator.get_raw_index_field(record_index)
        bytes_in_temporary += record_generator.index_size_bytes

        if bytes_in_temporary == bank.hardware_configuration.row_buffer_size_bytes:
            # Place and reset
            bank.set_raw_row(generating_at_row, temporary_row)
            temporary_row = 0
            bytes_in_temporary = 0
            generating_at_row += 1
        elif bytes_in_temporary >= bank.hardware_configuration.row_buffer_size_bytes:
            # Overshot, chunk
            while bytes_in_temporary >= bank.hardware_configuration.row_buffer_size_bytes and \
                    generating_at_row < base_row + row_count:
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

    # Either no more indies to process or we ran out of rows
    if generating_at_row < base_row + row_count and bytes_in_temporary > 0:
        # Place what we have left
        bytes_left_in_row = bank.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary
        for _ in range(bytes_left_in_row):
            temporary_row <<= 8
            temporary_row += bank.default_byte_value
        bank.set_raw_row(generating_at_row, temporary_row)


def perform_data_packed_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
):
    """
    Places record data/values in packed fashion within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [  data  ][  data  ][  data  ][  data  ][  da -
    - ta  ]                                         -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ... [  data  ][  data  ]                     -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """
    temporary_row = 0
    bytes_in_temporary = 0
    generating_at_row = base_row

    for record_index in range(limit):
        if generating_at_row >= base_row + row_count:
            break  # done placing

        temporary_row <<= record_generator.data_size_bytes * 8
        temporary_row += record_generator.get_raw_data_field(record_index)
        bytes_in_temporary += record_generator.data_size_bytes

        if bytes_in_temporary == bank.hardware_configuration.row_buffer_size_bytes:
            # Place and reset
            bank.set_raw_row(generating_at_row, temporary_row)
            temporary_row = 0
            bytes_in_temporary = 0
            generating_at_row += 1
        elif bytes_in_temporary >= bank.hardware_configuration.row_buffer_size_bytes:
            # Overshot, chunk
            while bytes_in_temporary >= bank.hardware_configuration.row_buffer_size_bytes and \
                    generating_at_row < base_row + row_count:
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

    # Either no more indies to process or we ran out of rows
    if generating_at_row < base_row + row_count and bytes_in_temporary > 0:
        # Place what we have left
        bytes_left_in_row = bank.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary
        for _ in range(bytes_left_in_row):
            temporary_row <<= 8
            temporary_row += bank.default_byte_value
        bank.set_raw_row(generating_at_row, temporary_row)
