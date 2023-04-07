import math

from src.configurations.hardware import HardwareConfiguration
from src.configurations.database import DatabaseConfiguration
from src.hardware import Bank
from src.generators import DatabaseRecordGenerator
from src.data_layout_mappings.methods import \
    perform_record_packed_horizontal_layout, \
    perform_record_aligned_horizontal_layout, \
    perform_index_packed_horizontal_layout, \
    perform_index_aligned_horizontal_layout, \
    perform_record_msb_vertical_layout, \
    perform_index_msb_vertical_layout
from src.data_layout_mappings import RowMappingSet, LayoutMetadata, DataLayoutConfiguration


class StandardPackedDataLayout(DataLayoutConfiguration):
    """
    Defines the row/data layout configuration for a standard DRAM database bank. This base configuration places records
    side by side within the bank fitting as many as possible row by row

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
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
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: HardwareConfiguration,
            database: DatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        self._row_mapping_set = RowMappingSet(
            data=(0, hardware.bank_rows)
        )

        limit_records = hardware.bank_size_bytes // database.total_record_size_bytes
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = LayoutMetadata(
            total_rows_for_records=hardware.bank_rows,
            total_records_processable=limit_records
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_record_packed_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class StandardAlignedDataLayout(DataLayoutConfiguration):
    """
    Defines the row/data layout configuration for a standard DRAM database bank. This base configuration places records
    side by side within the bank fitting as many as possible row by row without cutting off records

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
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
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: HardwareConfiguration,
            database: DatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        whole_records_to_row_buffer = self._hardware_configuration.row_buffer_size_bytes // \
            self._database_configuration.total_record_size_bytes

        # Multiple records to row buffer?
        if whole_records_to_row_buffer >= 1:
            # Simply count how many chunked records we can save
            processable_records = whole_records_to_row_buffer * hardware.bank_rows
            data_rows = hardware.bank_rows
        # Multiple rows per one record
        else:
            whole_rows_to_record = int(math.ceil(self._database_configuration.total_record_size_bytes /
                                                 self._hardware_configuration.row_buffer_size_bytes))
            processable_records = hardware.bank_rows // whole_rows_to_record
            data_rows = hardware.bank_rows - hardware.bank_rows % whole_rows_to_record

        self._row_mapping_set = RowMappingSet(
            data=(0, data_rows)
        )

        limit_records = processable_records
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = LayoutMetadata(
            total_rows_for_records=data_rows,
            total_records_processable=limit_records
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_record_aligned_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class StandardPackedIndexDataLayout(DataLayoutConfiguration):
    """
    Defines the row/data layout configuration for a standard DRAM database bank. This base configuration places record
    indices side by side within the bank fitting as many as possible row by row

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [     index     ][     index     ][     inde  -
    -  x     ]                                      -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ... [     index     ][     index     ]       -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: HardwareConfiguration,
            database: DatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        self._row_mapping_set = RowMappingSet(
            data=(0, hardware.bank_rows)
        )

        limit_records = hardware.bank_size_bytes // database.total_index_size_bytes
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = LayoutMetadata(
            total_rows_for_records=hardware.bank_rows,
            total_records_processable=limit_records
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_packed_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class StandardAlignedIndexDataLayout(DataLayoutConfiguration):
    """
    Defines the row/data layout configuration for a standard DRAM database bank. This base configuration places record
    indices side by side within the bank fitting as many as possible row by row without cutting off indices

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [     index     ][     index     ]            -
    - [     index     ]                             -
    -                      ...                      -
    -                                               -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -                                               -
    -  ... [     index     ][     index     ]       -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: HardwareConfiguration,
            database: DatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        whole_indices_to_row_buffer = self._hardware_configuration.row_buffer_size_bytes // \
            self._database_configuration.total_index_size_bytes

        # Multiple indices to row buffer?
        if whole_indices_to_row_buffer >= 1:
            # Simply count how many chunked indices we can save
            processable_indices = whole_indices_to_row_buffer * hardware.bank_rows
            data_rows = hardware.bank_rows
        # Multiple rows per one index
        else:
            whole_rows_to_index = int(math.ceil(self._database_configuration.total_index_size_bytes /
                                                self._hardware_configuration.row_buffer_size_bytes))
            processable_indices = hardware.bank_rows // whole_rows_to_index
            data_rows = hardware.bank_rows - hardware.bank_rows % whole_rows_to_index

        self._row_mapping_set = RowMappingSet(
            data=(0, data_rows)
        )

        limit_records = processable_indices
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = LayoutMetadata(
            total_rows_for_records=data_rows,
            total_records_processable=limit_records
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_aligned_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class StandardBitweaveVerticalRecordDataLayout(DataLayoutConfiguration):
    """
    Defines the row/data layout configuration for a standard DRAM database bank. This base configuration places records
    vertically within the bank fitting as many as possible row by row

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [ [ [ [ [                                [    -
    - r r r r r                                r    -
    - e e e e e            ...                 e    -
    - c c c c c                                c    -
    - o o o o o                                o    -
    - r r r r r            ...                 r    -
    - d d d d d                                d    -
    - ] ] ] ] ]                                ]    -
    -                                               -
    -                                               -
    -                                               -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: HardwareConfiguration,
            database: DatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (hardware.bank_rows // (self._database_configuration.total_record_size_bytes * 8))

        self._row_mapping_set = RowMappingSet(
            data=(0, hardware.bank_rows)
        )

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = LayoutMetadata(
            total_rows_for_records=hardware.bank_rows,
            total_records_processable=limit_records
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_record_msb_vertical_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class StandardBitweaveVerticalIndexDataLayout(DataLayoutConfiguration):
    """
    Defines the row/data layout configuration for a standard DRAM database bank. This base configuration places record
    indices vertically within the bank fitting as many as possible row by row

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    - [ [ [ [ [                                [    -
    - i i i i i                                i    -
    - n n n n n            ...                 n    -
    - d d d d d                                d    -
    - e e e e e                                e    -
    - x x x x x            ...                 x    -
    - ] ] ] ] ]                                ]    -
    -                                               -
    -                                               -
    -                                               -
    -                                               -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: HardwareConfiguration,
            database: DatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (hardware.bank_rows // (self._database_configuration.total_index_size_bytes * 8))

        self._row_mapping_set = RowMappingSet(
            data=(0, hardware.bank_rows)
        )

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = LayoutMetadata(
            total_rows_for_records=hardware.bank_rows,
            total_records_processable=limit_records
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_msb_vertical_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )
