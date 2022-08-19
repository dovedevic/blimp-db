import math

from pydantic import Field

from src.configurations.hardware.blimp import BlimpHardwareConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration, BlimpDatabaseConfiguration
from hardware import Bank
from generators import DatabaseRecordGenerator
from data_layout_mappings import RowMappingSet, RowMapping, LayoutMetadata, DataLayoutConfiguration
from data_layout_mappings.methods import perform_record_aligned_horizontal_layout, place_hitmap, \
    perform_index_aligned_horizontal_layout


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


class BlimpHitmapRowMapping(BlimpRowMapping):
    """Standard BLIMP row mappings but with space for hitmaps"""
    hitmaps: RowMapping = Field(
        description="The start row address for blimp hitmaps and the number of rows this region contains")


class BlimpHitmapLayoutMetadata(BlimpLayoutMetadata):
    """Metadata for a standard BLIMP layout but with hitmaps"""
    total_rows_for_hitmaps: int = Field(
        description="The total number of rows reserved for hitmaps")


class StandardBlimpBankLayoutConfiguration(DataLayoutConfiguration):
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

    def __init__(self, hardware: BlimpHardwareConfiguration, database: BlimpDatabaseConfiguration):
        super().__init__(hardware, database)

        self._hardware_configuration = hardware
        self._database_configuration = database

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
            whole_rows_to_record = int(math.ceil(self._database_configuration.total_record_size_bytes //
                                                 self._hardware_configuration.row_buffer_size_bytes))
            processable_records = total_rows_for_configurable_data // whole_rows_to_record
            data_rows = total_rows_for_configurable_data - total_rows_for_configurable_data % whole_rows_to_record

        total_rows_for_records = data_rows  # Total rows for BLIMP-format records (pi field + data / k + v)
        total_records_processable = processable_records  # Total number of records operable with this configuration

        self._layout_metadata = BlimpLayoutMetadata(
            total_rows_for_records=total_rows_for_records,
            total_records_processable=total_records_processable,
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

    def perform_data_layout(self, bank: Bank, record_generator: DatabaseRecordGenerator):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_record_aligned_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=record_generator,
            limit=self.layout_metadata.total_records_processable
        )

    @classmethod
    def load(cls, path: str,
             hardware_config: callable = BlimpHardwareConfiguration,
             database_config: callable = BlimpDatabaseConfiguration
             ):
        """Load a layout configuration object"""
        return super().load(path, hardware_config, database_config)


class BlimpHitmapBankLayoutConfiguration(StandardBlimpBankLayoutConfiguration):
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
    def __init__(self, hardware: BlimpHardwareConfiguration, database: BlimpHitmapDatabaseConfiguration):
        super().__init__(hardware, database)

        self._hardware_configuration = hardware
        self._database_configuration = database

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

        data_rows = 0
        hitmap_rows = 0
        processable_records = 0
        record_to_row_buffer_ratio = self._database_configuration.total_record_size_bytes \
            / self._hardware_configuration.row_buffer_size_bytes
        while data_rows + hitmap_rows < total_rows_for_configurable_data:
            # Hitmap rows are calculated one bit per PI field, per each hitmap
            new_hitmap_rows = self._database_configuration.hitmap_count
            # Data rows are calculated by the row buffer width of records, multiplied by the size of the record
            # rb * 8 * data / rb
            new_data_rows = 8 * self._database_configuration.total_record_size_bytes

            # Can we fit a full set of records into the bank?
            if new_hitmap_rows + hitmap_rows + new_data_rows + data_rows < total_rows_for_configurable_data:
                # If we can, add this new block into our existing set
                hitmap_rows += new_hitmap_rows
                data_rows += new_data_rows
                processable_records += self._hardware_configuration.row_buffer_size_bytes * 8
                continue

            # Can we fit a subset of records into the bank?
            elif new_hitmap_rows + hitmap_rows + data_rows < total_rows_for_configurable_data:
                # If we can, ensure we can add at least one data record
                if record_to_row_buffer_ratio >= 1 and (new_hitmap_rows + hitmap_rows +
                   data_rows + record_to_row_buffer_ratio) > total_rows_for_configurable_data:
                    # If we can't fit at least one record, break out
                    break

                # At this point at least one record is placeable, so place the blocks
                hitmap_rows += new_hitmap_rows

                rows_remaining = total_rows_for_configurable_data - hitmap_rows - data_rows
                if record_to_row_buffer_ratio <= 1:
                    records_per_row = self._hardware_configuration.row_buffer_size_bytes \
                                      // self._database_configuration.total_record_size_bytes
                    processable_records += rows_remaining * records_per_row
                    data_rows += rows_remaining
                else:
                    records_in_remainder = rows_remaining // (
                            self._database_configuration.total_record_size_bytes
                            // self._hardware_configuration.row_buffer_size_bytes
                    )
                    processable_records += records_in_remainder
                    data_rows += records_in_remainder * (
                            self._database_configuration.total_record_size_bytes
                            // self._hardware_configuration.row_buffer_size_bytes
                    )
                break

            # Can no more blocks of rows can be placed
            else:
                break

        # Done heuristically placing rows in bank, finalize configuration
        # Ensure we are inbounds
        if data_rows + hitmap_rows > total_rows_for_configurable_data:
            raise AssertionError("Heuristic placement failed, alter parameters or reserved rows")

        total_rows_for_records = data_rows  # Total rows for BLIMP-format records (pi field + data / k + v)
        total_rows_for_hitmaps = hitmap_rows  # Total rows for BLIMP hitmap placement
        total_records_processable = processable_records  # Total number of records operable with this configuration

        self._layout_metadata = BlimpHitmapLayoutMetadata(
            total_rows_for_records=total_rows_for_records,
            total_records_processable=total_records_processable,
            total_rows_for_hitmaps=total_rows_for_hitmaps,
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

        hitmaps_region = (base, total_rows_for_hitmaps)
        base += total_rows_for_hitmaps

        self._row_mapping_set = BlimpHitmapRowMapping(
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            data=data_region,
            hitmaps=hitmaps_region,
        )

    def perform_data_layout(self, bank: Bank, record_generator: DatabaseRecordGenerator):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        super().perform_data_layout(bank, record_generator)

        rows_per_hitmap = self._layout_metadata.total_rows_for_hitmaps // self._database_configuration.hitmap_count
        for hitmap in range(self._database_configuration.hitmap_count):
            place_hitmap(
                self._row_mapping_set.hitmaps[0] + hitmap * rows_per_hitmap,
                rows_per_hitmap,
                bank,
                True,
                self.layout_metadata.total_records_processable
            )

    @classmethod
    def load(cls, path: str,
             hardware_config: callable=BlimpHardwareConfiguration,
             database_config: callable=BlimpHitmapDatabaseConfiguration
             ):
        """Load a layout configuration object"""
        return super().load(path, hardware_config, database_config)


class BlimpIndexBankLayoutConfiguration(DataLayoutConfiguration):
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

    def __init__(self, hardware: BlimpHardwareConfiguration, database: BlimpDatabaseConfiguration):
        super().__init__(hardware, database)

        self._hardware_configuration = hardware
        self._database_configuration = database

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

        # Multiple indices to row buffer?
        if whole_indices_to_row_buffer >= 1:
            # Simply count how many chunked indices we can save
            processable_indices = whole_indices_to_row_buffer * total_rows_for_configurable_data
            data_rows = total_rows_for_configurable_data
        # Multiple rows per one index
        else:
            whole_rows_to_index = int(math.ceil(self._database_configuration.total_index_size_bytes //
                                                self._hardware_configuration.row_buffer_size_bytes))
            processable_indices = total_rows_for_configurable_data // whole_rows_to_index
            data_rows = total_rows_for_configurable_data - total_rows_for_configurable_data % whole_rows_to_index

        total_rows_for_indices = data_rows  # Total rows for BLIMP-format indices (pi field / key)
        total_indices_processable = processable_indices  # Total number of indices operable with this configuration

        self._layout_metadata = BlimpLayoutMetadata(
            total_rows_for_records=total_rows_for_indices,
            total_records_processable=total_indices_processable,
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

    def perform_data_layout(self, bank: Bank, record_generator: DatabaseRecordGenerator):
        """Given a bank hardware and record generator, attempt to place as many indices into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_aligned_horizontal_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=record_generator,
            limit=self.layout_metadata.total_records_processable
        )

    @classmethod
    def load(cls, path: str,
             hardware_config: callable = BlimpHardwareConfiguration,
             database_config: callable = BlimpDatabaseConfiguration
             ):
        """Load a layout configuration object"""
        return super().load(path, hardware_config, database_config)
