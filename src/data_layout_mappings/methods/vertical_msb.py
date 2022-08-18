from generators import DatabaseRecordGenerator
from hardware import Bank
from utils import bitmanip


def perform_record_msb_vertical_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
):
    """
    Places records MSB-first vertically within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -  [ [ [                                      [ -
    -  r r r                                      r -
    -  e e e               ...                    e -
    -  c c c                                      c -
    -  o o o                                      o -
    -  r r r               ...                    r -
    -  d d d                                      d -
    -  ] ] ]                                      ] -
    -                                               -
    -  ...                                          -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """

    # Load the record fields into the vertical columns row by row
    for pi_row in range(row_count):
        # Calculate meta-specifics
        record_page = pi_row // (record_generator.record_size_bytes * 8)
        record_bit_index = pi_row % (record_generator.record_size_bytes * 8)
        start_record = record_page * bank.hardware_configuration.row_buffer_size_bytes * 8

        # Generate a new constructed row based on the current meta-specifics
        raw_value = 0
        for j in range(bank.hardware_configuration.row_buffer_size_bytes * 8):
            # Ensure we are not over placing records
            if start_record + j < limit:
                # Fetch a record from the bank based on it's index
                raw_value <<= 1
                pi = record_generator.get_raw_record(start_record + j)
                bit = bitmanip.msb_bit(
                    pi,
                    record_bit_index,
                    record_generator.record_size_bytes * 8
                )
                raw_value += bit
            else:
                # If we reach our limit, null pad the rest of the columns
                raw_value <<= 1
                raw_value += bitmanip.msb_bit(
                    bank.default_byte_value,
                    j % 8,
                    8
                )

        # Set this new translated row into the bank
        bank.set_raw_row(base_row + pi_row, raw_value)


def perform_index_msb_vertical_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
):
    """
    Places record indices MSB-first vertically within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -  [ [ [                                      [ -
    -                                               -
    -  i i i               ...                    i -
    -  n n n                                      n -
    -  d d d                                      d -
    -  e e e               ...                    e -
    -  x x x                                      x -
    -                                               -
    -  ] ] ]                                      ] -
    -  ...                                          -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """

    # Load the P/I fields into the vertical columns row by row
    for pi_row in range(row_count):
        # Calculate meta-specifics
        record_page = pi_row // (record_generator.index_size_bytes * 8)
        record_bit_index = pi_row % (record_generator.index_size_bytes * 8)
        start_record = record_page * bank.hardware_configuration.row_buffer_size_bytes * 8

        # Generate a new constructed row based on the current meta-specifics
        raw_value = 0
        for j in range(bank.hardware_configuration.row_buffer_size_bytes * 8):
            # Ensure we are not over placing records
            if start_record + j < limit:
                # Fetch a record from the bank based on it's index
                raw_value <<= 1
                pi = record_generator.get_raw_pi_field(start_record + j)
                bit = bitmanip.msb_bit(
                    pi,
                    record_bit_index,
                    record_generator.index_size_bytes * 8
                )
                raw_value += bit
            else:
                # If we reach our limit, null pad the rest of the columns
                raw_value <<= 1
                raw_value += bitmanip.msb_bit(
                    bank.default_byte_value,
                    j % 8,
                    8
                )

        # Set this new translated row into the bank
        bank.set_raw_row(base_row + pi_row, raw_value)


def perform_data_msb_vertical_layout(
        base_row: int,
        row_count: int,
        bank: Bank,
        record_generator: DatabaseRecordGenerator,
        limit: int
):
    """
    Places record data/values MSB-first vertically within a bank.

    Placement pictorially can be seen as,

    base_row
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -  [ [ [                                      [ -
    -                                               -
    -  d d d               ...                    d -
    -  a a a                                      a -
    -  t t t                                      t -
    -  a a a               ...                    a -
    -                                               -
    -  ] ] ]                                      ] -
    -                                               -
    -  ...                                          -
    + - - - - - - - - - - - - - - - - - - - - - - - + base_row + row_count
    """

    # Load the data fields into the vertical columns row by row
    for data_row in range(row_count):
        # Calculate meta-specifics
        record_page = data_row // (record_generator.data_size_bytes * 8)
        record_bit_index = data_row % (record_generator.data_size_bytes * 8)
        start_record = record_page * bank.hardware_configuration.row_buffer_size_bytes * 8

        # Generate a new constructed row based on the current meta-specifics
        raw_value = 0
        for j in range(bank.hardware_configuration.row_buffer_size_bytes * 8):
            # Ensure we are not over placing records
            if start_record + j < limit:
                # Fetch a record from the bank based on it's index
                raw_value <<= 1
                data = record_generator.get_raw_data_field(start_record + j)
                bit = bitmanip.msb_bit(
                    data,
                    record_bit_index,
                    record_generator.data_size_bytes * 8
                )
                raw_value += bit
            else:
                # If we reach our limit, null pad the rest of the columns
                raw_value <<= 1
                raw_value += bitmanip.msb_bit(
                    bank.default_byte_value,
                    j % 8,
                    8
                )

        # Set this new translated row into the bank
        bank.set_raw_row(base_row + data_row, raw_value)
