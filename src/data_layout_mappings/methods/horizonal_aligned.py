from src.generators import DatabaseRecordGenerator
from src.hardware import Bank


def perform_record_aligned_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
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

    if records_per_row >= 1:
        # Multiple (or one) records per row; record size <= row buffer
        for row_index in range(row_count):
            # Do we need to end because we reached our limit?
            if records_placed >= limit:
                break

            # Construct a temporary row
            temporary_row = 0
            bytes_in_temporary = 0

            # Fetch all records in this row, if we are at the end, null pad with default bytes
            while bytes_in_temporary + record_generator.record_size_bytes <= \
                    bank.hardware_configuration.row_buffer_size_bytes and records_placed < limit:
                temporary_row <<= record_generator.record_size_bytes * 8
                temporary_row += record_generator.get_raw_record(records_placed)
                bytes_in_temporary += record_generator.record_size_bytes
                records_placed += 1

            # We either broke out because we filled an aligned buffer or we ran out of records to place
            # First pad with default bytes
            for _ in range(bank.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary):
                temporary_row <<= 8
                temporary_row += bank.default_byte_value

            # We have a full row, place it
            bank.set_raw_row(base_row + row_index, temporary_row)

    else:
        # Multiple rows per record; row buffer < record size
        rows_per_record = record_generator.record_size_bytes \
                          // bank.hardware_configuration.row_buffer_size_bytes
        generating_at_row = base_row

        # For all placeable records, extract row-buffer sized chunks and store them
        for record_index in range(limit):
            # Do we need to end early because we reached the row limit
            if generating_at_row >= base_row + row_count:
                break

            # Fetch/Generate the records
            temporary_row = record_generator.get_raw_record(record_index)
            bytes_in_temporary = record_generator.record_size_bytes

            # Row buffer align this
            if bytes_in_temporary % bank.hardware_configuration.row_buffer_size_bytes == 0:
                bytes_to_pad = 0
            else:
                bytes_to_pad = bank.hardware_configuration.row_buffer_size_bytes - \
                           (bytes_in_temporary % bank.hardware_configuration.row_buffer_size_bytes)
            for _ in range(bytes_to_pad):
                temporary_row <<= 8
                temporary_row += bank.default_byte_value
                bytes_in_temporary += 1

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


def perform_index_aligned_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
):
    """
    Places indices in row-buffer-aligned chunks within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [  index  ][  index  ][  index  ][  index  ]  -
    - [  index  ][  index  ]                        -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ...   [  index  ][  index  ]                 -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """
    # See if we are in a multi-record-per-row configuration or multi-row-per-record
    indices_per_row = bank.hardware_configuration.row_buffer_size_bytes \
        // record_generator.index_size_bytes
    records_placed = 0

    if indices_per_row >= 1:
        # Multiple (or one) index per row; index size <= row buffer
        for row_index in range(row_count):
            # Do we need to end because we reached our limit?
            if records_placed >= limit:
                break

            # Construct a temporary row
            temporary_row = 0
            bytes_in_temporary = 0

            # Fetch all records in this row, if we are at the end, null pad with default bytes
            while bytes_in_temporary + record_generator.index_size_bytes <= \
                    bank.hardware_configuration.row_buffer_size_bytes and records_placed < limit:
                temporary_row <<= record_generator.index_size_bytes * 8
                temporary_row += record_generator.get_raw_index_field(records_placed)
                bytes_in_temporary += record_generator.index_size_bytes
                records_placed += 1

            # We either broke out because we filled an aligned buffer or we ran out of records to place
            # First pad with default bytes
            for _ in range(bank.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary):
                temporary_row <<= 8
                temporary_row += bank.default_byte_value

            # We have a full row, place it
            bank.set_raw_row(base_row + row_index, temporary_row)

    else:
        # Multiple rows per index; row buffer < index size
        generating_at_row = base_row

        # For all placeable records, extract row-buffer sized chunks and store them
        for record_index in range(limit):
            # Do we need to end early because we reached the row limit
            if generating_at_row >= base_row + row_count:
                break

            # Fetch/Generate the records
            temporary_row = record_generator.get_raw_index_field(record_index)
            bytes_in_temporary = record_generator.index_size_bytes

            # Row buffer align this
            if bytes_in_temporary % bank.hardware_configuration.row_buffer_size_bytes == 0:
                bytes_to_pad = 0
            else:
                bytes_to_pad = bank.hardware_configuration.row_buffer_size_bytes - \
                           (bytes_in_temporary % bank.hardware_configuration.row_buffer_size_bytes)
            for _ in range(bytes_to_pad):
                temporary_row <<= 8
                temporary_row += bank.default_byte_value
                bytes_in_temporary += 1

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


def perform_data_aligned_horizontal_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
):
    """
    Places data/values in row-buffer-aligned chunks within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [  data  ][  data  ][  data  ][  data  ]      -
    - [  data  ][  data  ]                          -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ...   [  data  ][  data  ]                 -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """
    # See if we are in a multi-record-per-row configuration or multi-row-per-record
    data_values_per_row = bank.hardware_configuration.row_buffer_size_bytes \
        // record_generator.data_size_bytes
    records_placed = 0

    if data_values_per_row >= 1:
        # Multiple (or one) data/value per row; data/value size <= row buffer
        for row_index in range(row_count):
            # Do we need to end because we reached our limit?
            if records_placed >= limit:
                break

            # Construct a temporary row
            temporary_row = 0
            bytes_in_temporary = 0

            # Fetch all records in this row, if we are at the end, null pad with default bytes
            while bytes_in_temporary + record_generator.data_size_bytes <= \
                    bank.hardware_configuration.row_buffer_size_bytes and records_placed < limit:
                temporary_row <<= record_generator.data_size_bytes * 8
                temporary_row += record_generator.get_raw_data_field(records_placed)
                bytes_in_temporary += record_generator.data_size_bytes
                records_placed += 1

            # We either broke out because we filled an aligned buffer or we ran out of records to place
            # First pad with default bytes
            for _ in range(bank.hardware_configuration.row_buffer_size_bytes - bytes_in_temporary):
                temporary_row <<= 8
                temporary_row += bank.default_byte_value

            # We have a full row, place it
            bank.set_raw_row(base_row + row_index, temporary_row)

    else:
        # Multiple rows per data/value; row buffer < data/value size
        generating_at_row = base_row

        # For all placeable records, extract row-buffer sized chunks and store them
        for record_index in range(limit):
            # Do we need to end early because we reached the row limit
            if generating_at_row >= base_row + row_count:
                break

            # Fetch/Generate the records
            temporary_row = record_generator.get_raw_data_field(record_index)
            bytes_in_temporary = record_generator.data_size_bytes

            # Row buffer align this
            if bytes_in_temporary % bank.hardware_configuration.row_buffer_size_bytes == 0:
                bytes_to_pad = 0
            else:
                bytes_to_pad = bank.hardware_configuration.row_buffer_size_bytes - \
                           (bytes_in_temporary % bank.hardware_configuration.row_buffer_size_bytes)
            for _ in range(bytes_to_pad):
                temporary_row <<= 8
                temporary_row += bank.default_byte_value
                bytes_in_temporary += 1

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
