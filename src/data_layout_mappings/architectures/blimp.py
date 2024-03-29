import math

from pydantic import Field
from typing import Union

from src.configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration, BlimpDatabaseConfiguration
from src.hardware import Bank
from src.generators import DatabaseRecordGenerator
from src.data_layout_mappings import RowMappingSet, RowMapping, LayoutMetadata, DataLayoutConfiguration
from src.data_layout_mappings.methods import perform_record_aligned_horizontal_layout, \
    perform_index_aligned_horizontal_layout, perform_record_msb_vertical_layout, perform_index_msb_vertical_layout
from src.data_layout_mappings.architectures.hitmap import \
    GenericHitmapBankLayoutConfiguration, GenericHitmapRowMapping, GenericHitmapLayoutMetadata


class BlimpRowMapping(RowMappingSet):
    """BLIMP row mappings for code and temp data regions"""
    blimp_code_region: RowMapping = Field(
        description="The start row address for blimp code and the number of rows this region contains")
    blimp_temp_region: RowMapping = Field(
        description="The start row address for blimp temporary data and the number of rows this region contains")


class BlimpLayoutMetadata(LayoutMetadata):
    """Metadata for standard BLIMP layout"""
    total_rows_for_configurable_data: int = Field(
        description="The total number of rows that are available after reservations are taken into effect")
    total_rows_for_blimp_code_region: int = Field(
        description="The total number of rows reserved for BLIMP code to reside")
    total_rows_for_blimp_temp_region: int = Field(
        description="The total number of rows reserved for temporary BLIMP data or stack space")


class BlimpHitmapRowMapping(BlimpRowMapping, GenericHitmapRowMapping):
    """Standard BLIMP row mappings but with space for hitmaps"""
    pass


class BlimpHitmapLayoutMetadata(BlimpLayoutMetadata, GenericHitmapLayoutMetadata):
    """Metadata for a standard BLIMP layout but with hitmaps"""
    pass


class StandardBlimpBankLayoutConfiguration(
    DataLayoutConfiguration[
        BlimpHardwareConfiguration, BlimpDatabaseConfiguration, BlimpLayoutMetadata, BlimpRowMapping
    ],
):
    """
    Defines the row/data layout configuration for a standard BLIMP database bank. This configuration places records
    in row-buffer-aligned chunks fitting as many whole-records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
            hardware: BlimpHardwareConfiguration,
            database: BlimpDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_for_configurable_data = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        if self._database_configuration.total_record_size_bytes > self._hardware_configuration.row_buffer_size_bytes:
            if self._database_configuration.total_record_size_bytes % \
                    self._hardware_configuration.row_buffer_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")
        else:
            if self._hardware_configuration.row_buffer_size_bytes % \
                    self._database_configuration.total_record_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")

        whole_records_to_row_buffer = self._hardware_configuration.row_buffer_size_bytes // \
            self._database_configuration.total_record_size_bytes

        # Multiple records to row buffer?
        if whole_records_to_row_buffer >= 1:
            # Simply count how many chunked records we can save
            processable_records = whole_records_to_row_buffer * total_rows_for_configurable_data
            data_rows = total_rows_for_configurable_data
        # Multiple rows per one record
        else:
            whole_rows_to_record = int(math.ceil(self._database_configuration.total_record_size_bytes /
                                                 self._hardware_configuration.row_buffer_size_bytes))
            processable_records = total_rows_for_configurable_data // whole_rows_to_record
            data_rows = total_rows_for_configurable_data - total_rows_for_configurable_data % whole_rows_to_record

        total_rows_for_records = data_rows  # Total rows for BLIMP-format records (pi field + data / k + v)
        total_records_processable = processable_records  # Total number of records operable with this configuration

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = BlimpLayoutMetadata(
            total_rows_for_records=total_rows_for_records,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_configurable_data,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_records)
        base += total_rows_for_records

        self._row_mapping_set = BlimpRowMapping(
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
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


class BlimpHitmapBankLayoutConfiguration(
    GenericHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        Union[BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration],
        BlimpHitmapDatabaseConfiguration, BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a BLIMP database bank with hitmaps. This configuration places records
    in row-buffer-aligned chunks fitting as many whole-records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
    -                    HITMAPS                    -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """
    def __init__(
            self,
            hardware: BlimpHardwareConfiguration,
            database: BlimpHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_configurable = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_configurable < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        if self._database_configuration.total_record_size_bytes > self._hardware_configuration.row_buffer_size_bytes:
            if self._database_configuration.total_record_size_bytes % \
                    self._hardware_configuration.row_buffer_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")
        else:
            if self._hardware_configuration.row_buffer_size_bytes % \
                    self._database_configuration.total_record_size_bytes != 0:
                raise ValueError("Record sizes must be row buffer aligned to at least a power of two")

        whole_records_to_row_buffer = self._hardware_configuration.row_buffer_size_bytes // \
            self._database_configuration.total_record_size_bytes
        whole_rows_to_record = int(math.ceil(self._database_configuration.total_record_size_bytes /
                                             self._hardware_configuration.row_buffer_size_bytes))

        # Multiple records to row buffer?
        if whole_records_to_row_buffer >= 1:
            # Simply count how many chunked records we can save
            total_records_processable = whole_records_to_row_buffer * total_rows_configurable
            total_rows_for_horizontal_data = int(math.ceil(total_records_processable / whole_records_to_row_buffer))
        # Multiple rows per one record
        else:
            total_records_processable = total_rows_configurable // whole_rows_to_record
            total_rows_for_horizontal_data = total_records_processable * whole_rows_to_record

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

            if whole_records_to_row_buffer >= 1:
                # Simply count how many chunked records we can save
                total_rows_for_horizontal_data = int(math.ceil(limit_records / whole_records_to_row_buffer))
            # Multiple rows per one record
            else:
                total_rows_for_horizontal_data = limit_records * whole_rows_to_record

        total_rows_for_hitmaps = int(math.ceil(
            limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
        ) * self._database_configuration.hitmap_count

        while total_rows_for_hitmaps + total_rows_for_horizontal_data > total_rows_configurable and limit_records > 0:
            # Start cutting back
            limit_records -= 1

            # Recalc
            if whole_records_to_row_buffer >= 1:
                total_rows_for_horizontal_data = int(math.ceil(limit_records / whole_records_to_row_buffer))
            else:
                total_rows_for_horizontal_data = limit_records * whole_rows_to_record

            total_rows_for_hitmaps = int(math.ceil(
                limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
            ) * self._database_configuration.hitmap_count

        # Ensure we have at least a non-zero number of records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        self._layout_metadata = BlimpHitmapLayoutMetadata(
            total_rows_for_records=total_rows_for_horizontal_data,
            total_records_processable=limit_records,
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_configurable_data=total_rows_configurable,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_horizontal_data)
        base += total_rows_for_horizontal_data

        hitmaps_region = (base, total_rows_for_hitmaps)
        base += total_rows_for_hitmaps

        self._row_mapping_set = BlimpHitmapRowMapping(
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
            hitmaps=hitmaps_region,
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


class BlimpIndexBankLayoutConfiguration(
    DataLayoutConfiguration[
        BlimpHardwareConfiguration, BlimpDatabaseConfiguration, BlimpLayoutMetadata, BlimpRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a standard BLIMP database bank. This configuration places record
    indices in row-buffer-aligned chunks fitting as many whole-index-records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
            hardware: BlimpHardwareConfiguration,
            database: BlimpDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_for_configurable_data = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        if self._database_configuration.total_index_size_bytes > self._hardware_configuration.row_buffer_size_bytes:
            if self._database_configuration.total_index_size_bytes % \
                    self._hardware_configuration.row_buffer_size_bytes != 0:
                raise ValueError("Index sizes must be row buffer aligned to at least a power of two")
        else:
            if self._hardware_configuration.row_buffer_size_bytes % \
                    self._database_configuration.total_index_size_bytes != 0:
                raise ValueError("Index sizes must be row buffer aligned to at least a power of two")

        whole_indices_to_row_buffer = self._hardware_configuration.row_buffer_size_bytes // \
            self._database_configuration.total_index_size_bytes
        whole_rows_to_index = int(math.ceil(self._database_configuration.total_index_size_bytes /
                                            self._hardware_configuration.row_buffer_size_bytes))

        # Multiple indices to row buffer?
        if whole_indices_to_row_buffer >= 1:
            # Simply count how many chunked indices we can save
            processable_indices = whole_indices_to_row_buffer * total_rows_for_configurable_data
            data_rows = total_rows_for_configurable_data
        # Multiple rows per one index
        else:
            processable_indices = total_rows_for_configurable_data // whole_rows_to_index
            data_rows = total_rows_for_configurable_data - total_rows_for_configurable_data % whole_rows_to_index

        total_rows_for_indices = data_rows  # Total rows for BLIMP-format indices (pi field / key)
        total_indices_processable = processable_indices  # Total number of indices operable with this configuration

        limit_records = total_indices_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

            if whole_indices_to_row_buffer >= 1:
                total_rows_for_indices = int(math.ceil(limit_records / whole_indices_to_row_buffer))
            else:
                total_rows_for_indices = limit_records * whole_rows_to_index

        self._layout_metadata = BlimpLayoutMetadata(
            total_rows_for_records=total_rows_for_indices,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_configurable_data,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_indices)
        base += total_rows_for_indices

        self._row_mapping_set = BlimpRowMapping(
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many indices into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_aligned_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class BlimpIndexHitmapBankLayoutConfiguration(
    GenericHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        Union[BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration],
        BlimpHitmapDatabaseConfiguration, BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a BLIMP database bank with hitmaps. This configuration places record
    indices in row-buffer-aligned chunks fitting as many whole-index records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
    -                    HITMAPS                    -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """
    def __init__(
            self,
            hardware: BlimpHardwareConfiguration,
            database: BlimpHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_configurable = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_configurable < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        if self._database_configuration.total_index_size_bytes > self._hardware_configuration.row_buffer_size_bytes:
            if self._database_configuration.total_index_size_bytes % \
                    self._hardware_configuration.row_buffer_size_bytes != 0:
                raise ValueError("Index sizes must be row buffer aligned to at least a power of two")
        else:
            if self._hardware_configuration.row_buffer_size_bytes % \
                    self._database_configuration.total_index_size_bytes != 0:
                raise ValueError("Index sizes must be row buffer aligned to at least a power of two")

        whole_indices_to_row_buffer = self._hardware_configuration.row_buffer_size_bytes // \
            self._database_configuration.total_index_size_bytes
        whole_rows_to_index = int(math.ceil(self._database_configuration.total_index_size_bytes /
                                            self._hardware_configuration.row_buffer_size_bytes))

        # Multiple indices to row buffer?
        if whole_indices_to_row_buffer >= 1:
            # Simply count how many chunked indices we can save
            total_indices_processable = whole_indices_to_row_buffer * total_rows_configurable
            total_rows_for_horizontal_data = int(math.ceil(total_indices_processable / whole_indices_to_row_buffer))
        # Multiple rows per one index
        else:
            total_indices_processable = total_rows_configurable // whole_rows_to_index
            total_rows_for_horizontal_data = total_indices_processable * whole_rows_to_index

        limit_records = total_indices_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

            if whole_indices_to_row_buffer >= 1:
                total_rows_for_horizontal_data = int(math.ceil(limit_records / whole_indices_to_row_buffer))
            else:
                total_rows_for_horizontal_data = limit_records * whole_rows_to_index

        total_rows_for_hitmaps = int(math.ceil(
            limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
        ) * self._database_configuration.hitmap_count

        while total_rows_for_hitmaps + total_rows_for_horizontal_data > total_rows_configurable and limit_records > 0:
            # Start cutting back
            limit_records -= 1

            # Recalc
            if whole_indices_to_row_buffer >= 1:
                total_rows_for_horizontal_data = int(math.ceil(limit_records / whole_indices_to_row_buffer))
            else:
                total_rows_for_horizontal_data = limit_records * whole_rows_to_index

            total_rows_for_hitmaps = int(math.ceil(
                limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
            ) * self._database_configuration.hitmap_count

        # Ensure we have at least a non-zero number of records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        self._layout_metadata = BlimpHitmapLayoutMetadata(
            total_rows_for_records=total_rows_for_horizontal_data,
            total_records_processable=limit_records,
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_configurable_data=total_rows_configurable,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_horizontal_data)
        base += total_rows_for_horizontal_data

        hitmaps_region = (base, total_rows_for_hitmaps)
        base += total_rows_for_hitmaps

        self._row_mapping_set = BlimpHitmapRowMapping(
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
            hitmaps=hitmaps_region,
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many indices into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_aligned_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class BlimpRecordBitweaveBankLayoutConfiguration(
    DataLayoutConfiguration[
        BlimpHardwareConfiguration, BlimpDatabaseConfiguration, BlimpLayoutMetadata, BlimpRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a BLIMP database bank. This configuration places records
    vertically in the bank fitting as many whole-records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
            hardware: BlimpHardwareConfiguration,
            database: BlimpDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_for_configurable_data = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (total_rows_for_configurable_data // (self._database_configuration.total_record_size_bytes * 8))

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = BlimpLayoutMetadata(
            total_rows_for_records=total_rows_for_configurable_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_configurable_data,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_configurable_data)
        base += total_rows_for_configurable_data

        self._row_mapping_set = BlimpRowMapping(
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
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


class BlimpIndexBitweaveBankLayoutConfiguration(
    DataLayoutConfiguration[
        BlimpHardwareConfiguration, BlimpDatabaseConfiguration, BlimpLayoutMetadata, BlimpRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a BLIMP database bank. This configuration places record
    indices vertically in the bank fitting as many whole-index records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
            hardware: BlimpHardwareConfiguration,
            database: BlimpDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_for_configurable_data = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (total_rows_for_configurable_data // (self._database_configuration.total_index_size_bytes * 8))

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = BlimpLayoutMetadata(
            total_rows_for_records=total_rows_for_configurable_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_configurable_data,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_configurable_data)
        base += total_rows_for_configurable_data

        self._row_mapping_set = BlimpRowMapping(
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
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


class BlimpHitmapRecordBitweaveBankLayoutConfiguration(
    GenericHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        BlimpHardwareConfiguration, BlimpHitmapDatabaseConfiguration, BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a BLIMP database bank with hitmaps. This configuration places records
    vertically in the bank fitting as many whole-records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
    -                     HITMAPS                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: BlimpHardwareConfiguration,
            database: BlimpHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_for_configurable_data = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (total_rows_for_configurable_data // (self._database_configuration.total_record_size_bytes * 8))

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        total_rows_for_data = int(math.ceil(
            limit_records * (self._database_configuration.total_record_size_bytes * 8) /
            (self._hardware_configuration.row_buffer_size_bytes * 8)
        ))
        total_rows_for_hitmaps = int(math.ceil(
            limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
        ) * self._database_configuration.hitmap_count

        while total_rows_for_hitmaps + total_rows_for_data > total_rows_for_configurable_data \
                and limit_records > 0:
            # Start cutting back
            if limit_records % (self.hardware_configuration.row_buffer_size_bytes * 8) == 0:
                limit_records -= self.hardware_configuration.row_buffer_size_bytes * 8
            else:
                limit_records -= limit_records % \
                                             (self.hardware_configuration.row_buffer_size_bytes * 8)
            # Recalc
            total_rows_for_data = int(math.ceil(
                limit_records * (self._database_configuration.total_record_size_bytes * 8) /
                (self._hardware_configuration.row_buffer_size_bytes * 8)
            ))
            total_rows_for_hitmaps = int(math.ceil(
                limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
            ) * self._database_configuration.hitmap_count

        # Ensure we have at least a non-zero number of records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        total_rows_for_configurable_data -= total_rows_for_hitmaps

        self._layout_metadata = BlimpHitmapLayoutMetadata(
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_records=total_rows_for_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_configurable_data,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_data)
        base += total_rows_for_data

        hitmap_region = (base, total_rows_for_hitmaps)
        base += total_rows_for_hitmaps

        self._row_mapping_set = BlimpHitmapRowMapping(
            hitmaps=hitmap_region,
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
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


class BlimpHitmapIndexBitweaveBankLayoutConfiguration(
    GenericHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        Union[BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration],
        BlimpHitmapDatabaseConfiguration, BlimpHitmapLayoutMetadata, BlimpHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a BLIMP database bank with hitmaps. This configuration places record
    indices vertically in the bank fitting as many whole-index records into a row buffer as it can at a time

    An example of what this bank would look like is as follows:
        [record] = (key,value) | (index,columns) | (index,data)

    Bank:
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP CODE                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  BLIMP TEMP                   -
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
    -                     HITMAPS                   -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: BlimpHardwareConfiguration,
            database: BlimpHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined rows dedicated to storing BLIMP compute code
        total_rows_for_blimp_code_region = int(
            math.ceil(
                self._database_configuration.blimp_code_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # User-defined rows dedicated to BLIMP temporary storage
        total_rows_for_blimp_temp_region = int(
            math.ceil(
                self._database_configuration.blimp_temporary_region_size_bytes /
                self._hardware_configuration.row_buffer_size_bytes
            )
        )

        # Total rows to play with when configuring the layout
        total_rows_for_configurable_data = self._hardware_configuration.bank_rows \
            - (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_for_configurable_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static BLIMP row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * \
            (total_rows_for_configurable_data // self._database_configuration.total_index_size_bytes)

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        total_rows_for_data = int(math.ceil(
            limit_records * (self._database_configuration.total_index_size_bytes * 8) /
            (self._hardware_configuration.row_buffer_size_bytes * 8)
        ))
        total_rows_for_hitmaps = int(math.ceil(
            limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
        ) * self._database_configuration.hitmap_count

        while total_rows_for_hitmaps + total_rows_for_data > total_rows_for_configurable_data \
                and limit_records > 0:
            # Start cutting back
            if limit_records % (self.hardware_configuration.row_buffer_size_bytes * 8) == 0:
                limit_records -= self.hardware_configuration.row_buffer_size_bytes * 8
            else:
                limit_records -= limit_records % (self.hardware_configuration.row_buffer_size_bytes * 8)
            # Recalc
            total_rows_for_data = int(math.ceil(
                limit_records * (self._database_configuration.total_index_size_bytes * 8) /
                (self._hardware_configuration.row_buffer_size_bytes * 8)
            ))
            total_rows_for_hitmaps = int(math.ceil(
                limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
            ) * self._database_configuration.hitmap_count

        # Ensure we have at least a non-zero number of index/records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        total_rows_for_configurable_data -= total_rows_for_hitmaps

        self._layout_metadata = BlimpHitmapLayoutMetadata(
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_records=total_rows_for_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_configurable_data,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        data_region = (base, total_rows_for_configurable_data)
        base += total_rows_for_configurable_data

        hitmap_region = (base, total_rows_for_hitmaps)
        base += total_rows_for_hitmaps

        self._row_mapping_set = BlimpHitmapRowMapping(
            hitmaps=hitmap_region,
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
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
