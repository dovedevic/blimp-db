import math

from pydantic import Field

from src.configurations.hardware.ambit import BlimpPlusAmbitHardwareConfiguration
from src.configurations.database.ambit import \
    BlimpPlusAmbitDatabaseConfiguration, BlimpPlusAmbitHitmapDatabaseConfiguration
from src.hardware import Bank
from src.generators import DatabaseRecordGenerator
from src.data_layout_mappings import RowMapping, DataLayoutConfiguration
from src.data_layout_mappings.architectures.ambit import \
    AmbitRowMapping, AmbitLayoutMetadata, GenericAmbitBankLayoutConfiguration
from src.data_layout_mappings.architectures.blimp import BlimpRowMapping, BlimpLayoutMetadata
from src.data_layout_mappings.methods import perform_record_aligned_horizontal_layout, perform_index_msb_vertical_layout
from src.data_layout_mappings.architectures.hitmap import \
    GenericHitmapBankLayoutConfiguration, GenericHitmapRowMapping, GenericHitmapLayoutMetadata
from src.utils.generic import ceil_to_multiple


class BlimpAmbitRowMapping(BlimpRowMapping, AmbitRowMapping):
    """BLIMP and Ambit row mappings for compute and temp data regions"""
    horizontal_region: RowMapping = Field(
        description="The start row address for horizontal data and the number of rows this region contains")
    vertical_region: RowMapping = Field(
        description="The start row address for vertical data and the number of rows this region contains")


class BlimpAmbitLayoutMetadata(BlimpLayoutMetadata, AmbitLayoutMetadata):
    """Metadata for BLIMP-orchestrated AMBIT layouts"""
    total_rows_for_horizontal_data: int = Field(
        description="The total number of rows that are available after reservations are taken into effect for "
                    "horizontal data")
    total_rows_for_vertical_data: int = Field(
        description="The total number of rows that are available after reservations are taken into effect for "
                    "vertical data")


class BlimpAmbitHitmapRowMapping(BlimpAmbitRowMapping, GenericHitmapRowMapping):
    """BLIMP and Ambit row mappings with hitmaps for compute and temp data regions"""
    pass


class BlimpAmbitHitmapLayoutMetadata(BlimpAmbitLayoutMetadata, GenericHitmapLayoutMetadata):
    """Metadata for BLIMP-orchestrated AMBIT layouts with hitmaps"""
    pass


class GenericBlimpAmbitBankLayoutConfiguration(GenericAmbitBankLayoutConfiguration):
    pass


class GenericBlimpAmbitHitmapBankLayoutConfiguration(GenericBlimpAmbitBankLayoutConfiguration,
                                                     GenericHitmapBankLayoutConfiguration):
    pass


class StandardBlimpAmbitBankLayoutConfiguration(
    GenericBlimpAmbitBankLayoutConfiguration,
    DataLayoutConfiguration[
        BlimpPlusAmbitHardwareConfiguration, BlimpPlusAmbitDatabaseConfiguration,
        BlimpAmbitLayoutMetadata, BlimpAmbitRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a standard BLIMP orchestrated Ambit database bank. This configuration
    places indices vertically in the bank while fitting whole-records horizontally into the bank

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
    - [     record     ][     record     ]          -
    - [     record     ]                            -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -  ... [     record     ][     record     ]     -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -            AMBIT TEMPORARY SWAP SPACE         -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                AMBIT CONTROL ZERO             -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                AMBIT CONTROL ONE              -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                    AMBIT DCC                  -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  AMBIT COMPUTE                -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: BlimpPlusAmbitHardwareConfiguration,
            database: BlimpPlusAmbitDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined temporary rows (ambit D group) for ambit calculations
        total_rows_for_temporary_ambit_compute = self._database_configuration.ambit_temporary_bits

        # Ambit compute region rows, B and C groups, with optional T (temp) group
        total_rows_for_reserved_ambit_compute = \
            self._hardware_configuration.ambit_dcc_rows * 2 + \
            self._hardware_configuration.ambit_control_group_rows + \
            self._hardware_configuration.ambit_compute_register_rows

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

        # Data rows remaining for data
        total_rows_configurable = self._hardware_configuration.bank_rows - \
            (total_rows_for_reserved_ambit_compute + total_rows_for_temporary_ambit_compute) - \
            (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_configurable < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

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

        total_rows_for_vertical_data = ceil_to_multiple(
            limit_records * (self._database_configuration.total_index_size_bytes * 8) /
            (self._hardware_configuration.row_buffer_size_bytes * 8),
            base=self._database_configuration.total_index_size_bytes * 8
        )

        while total_rows_for_vertical_data + total_rows_for_horizontal_data > \
                total_rows_configurable and limit_records > 0:
            # Start cutting back
            limit_records -= 1

            # Recalc
            if whole_records_to_row_buffer >= 1:
                total_rows_for_horizontal_data = int(math.ceil(limit_records / whole_records_to_row_buffer))
            else:
                total_rows_for_horizontal_data = limit_records * whole_rows_to_record
            total_rows_for_vertical_data = ceil_to_multiple(
                limit_records * (self._database_configuration.total_index_size_bytes * 8) /
                (self._hardware_configuration.row_buffer_size_bytes * 8),
                base=self._database_configuration.total_index_size_bytes * 8
            )

        # Ensure we have at least a non-zero number of records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        self._layout_metadata = BlimpAmbitHitmapLayoutMetadata(
            total_rows_for_horizontal_data=total_rows_for_horizontal_data,
            total_rows_for_vertical_data=total_rows_for_vertical_data,
            total_rows_for_configurable_data=total_rows_configurable,
            total_rows_for_ambit_temp_region=total_rows_for_temporary_ambit_compute,
            total_rows_for_ambit_compute_region=total_rows_for_reserved_ambit_compute,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
            total_rows_for_records=total_rows_for_horizontal_data + total_rows_for_vertical_data,
            total_records_processable=limit_records,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        vertical_data_region = (base, total_rows_for_vertical_data)
        base += total_rows_for_vertical_data

        horizontal_data_region = (base, total_rows_for_horizontal_data)
        base += total_rows_for_horizontal_data

        ambit_temp_region = (base, total_rows_for_temporary_ambit_compute)
        base += total_rows_for_temporary_ambit_compute

        # Ambit regions are always at the bottom
        base = self._hardware_configuration.bank_rows

        base -= self._hardware_configuration.ambit_compute_register_rows
        ambit_compute_region = (base, self._hardware_configuration.ambit_compute_register_rows)

        base -= self._hardware_configuration.ambit_dcc_rows * 2
        ambit_dcc_region = (base, self._hardware_configuration.ambit_dcc_rows * 2)

        base -= self._hardware_configuration.ambit_control_group_rows
        ambit_control_region = (base, self._hardware_configuration.ambit_control_group_rows)

        self._row_mapping_set = BlimpAmbitRowMapping(
            ambit_control_rows=ambit_control_region,
            ambit_dcc_rows=ambit_dcc_region,
            ambit_compute_rows=ambit_compute_region,
            ambit_temp_rows=ambit_temp_region,
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            horizontal_region=horizontal_data_region,
            vertical_region=vertical_data_region,
            data=(vertical_data_region[0], total_rows_for_vertical_data + total_rows_for_horizontal_data),
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_msb_vertical_layout(
            base_row=self.row_mapping.vertical_region[0],
            row_count=self.row_mapping.vertical_region[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )

        perform_record_aligned_horizontal_layout(
            base_row=self.row_mapping.horizontal_region[0],
            row_count=self.row_mapping.horizontal_region[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class BlimpAmbitHitmapBankLayoutConfiguration(
    GenericBlimpAmbitHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        BlimpPlusAmbitHardwareConfiguration, BlimpPlusAmbitHitmapDatabaseConfiguration,
        BlimpAmbitHitmapLayoutMetadata, BlimpAmbitHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a standard BLIMP orchestrated Ambit database bank. This configuration
    places indices vertically in the bank while fitting whole-records horizontally into the bank with reservations for
    index hitmaps

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
    - [     record     ][     record     ]          -
    - [     record     ]                            -
    -                                               -
    -                      ...                      -
    -                                               -
    -                                               -
    -  ... [     record     ][     record     ]     -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                    HITMAPS                    -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -            AMBIT TEMPORARY SWAP SPACE         -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                AMBIT CONTROL ZERO             -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                AMBIT CONTROL ONE              -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                    AMBIT DCC                  -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  AMBIT COMPUTE                -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: BlimpPlusAmbitHardwareConfiguration,
            database: BlimpPlusAmbitHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined temporary rows (ambit D group) for ambit calculations
        total_rows_for_temporary_ambit_compute = self._database_configuration.ambit_temporary_bits

        # Ambit compute region rows, B and C groups, with optional T (temp) group
        total_rows_for_reserved_ambit_compute = \
            self._hardware_configuration.ambit_dcc_rows * 2 + \
            self._hardware_configuration.ambit_control_group_rows + \
            self._hardware_configuration.ambit_compute_register_rows

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

        # Data rows remaining for data and hitmaps
        total_rows_configurable = self._hardware_configuration.bank_rows - \
            (total_rows_for_reserved_ambit_compute + total_rows_for_temporary_ambit_compute) - \
            (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_configurable < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

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

        total_rows_for_vertical_data = ceil_to_multiple(
            limit_records * (self._database_configuration.total_index_size_bytes * 8) /
            (self._hardware_configuration.row_buffer_size_bytes * 8),
            base=self._database_configuration.total_index_size_bytes * 8
        )

        total_rows_for_hitmaps = int(math.ceil(
            limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
        ) * self._database_configuration.hitmap_count

        while total_rows_for_hitmaps + total_rows_for_vertical_data + total_rows_for_horizontal_data > \
                total_rows_configurable and limit_records > 0:
            # Start cutting back
            limit_records -= 1

            # Recalc
            if whole_records_to_row_buffer >= 1:
                total_rows_for_horizontal_data = int(math.ceil(limit_records / whole_records_to_row_buffer))
            else:
                total_rows_for_horizontal_data = limit_records * whole_rows_to_record
            total_rows_for_vertical_data = ceil_to_multiple(
                limit_records * (self._database_configuration.total_index_size_bytes * 8) /
                (self._hardware_configuration.row_buffer_size_bytes * 8),
                base=self._database_configuration.total_index_size_bytes * 8
            )
            total_rows_for_hitmaps = int(math.ceil(
                limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
            ) * self._database_configuration.hitmap_count

        # Ensure we have at least a non-zero number of records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        self._layout_metadata = BlimpAmbitHitmapLayoutMetadata(
            total_rows_for_horizontal_data=total_rows_for_horizontal_data,
            total_rows_for_vertical_data=total_rows_for_vertical_data,
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_configurable_data=total_rows_configurable,
            total_rows_for_ambit_temp_region=total_rows_for_temporary_ambit_compute,
            total_rows_for_ambit_compute_region=total_rows_for_reserved_ambit_compute,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
            total_rows_for_records=total_rows_for_horizontal_data + total_rows_for_vertical_data,
            total_records_processable=limit_records,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        vertical_data_region = (base, total_rows_for_vertical_data)
        base += total_rows_for_vertical_data

        horizontal_data_region = (base, total_rows_for_horizontal_data)
        base += total_rows_for_horizontal_data

        hitmap_region = (base, total_rows_for_hitmaps)
        base += total_rows_for_hitmaps

        ambit_temp_region = (base, total_rows_for_temporary_ambit_compute)
        base += total_rows_for_temporary_ambit_compute

        # Ambit regions are always at the bottom
        base = self._hardware_configuration.bank_rows

        base -= self._hardware_configuration.ambit_compute_register_rows
        ambit_compute_region = (base, self._hardware_configuration.ambit_compute_register_rows)

        base -= self._hardware_configuration.ambit_dcc_rows * 2
        ambit_dcc_region = (base, self._hardware_configuration.ambit_dcc_rows * 2)

        base -= self._hardware_configuration.ambit_control_group_rows
        ambit_control_region = (base, self._hardware_configuration.ambit_control_group_rows)

        self._row_mapping_set = BlimpAmbitHitmapRowMapping(
            hitmaps=hitmap_region,
            ambit_control_rows=ambit_control_region,
            ambit_dcc_rows=ambit_dcc_region,
            ambit_compute_rows=ambit_compute_region,
            ambit_temp_rows=ambit_temp_region,
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            horizontal_region=horizontal_data_region,
            vertical_region=vertical_data_region,
            data=(vertical_data_region[0], total_rows_for_vertical_data + total_rows_for_horizontal_data),
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_msb_vertical_layout(
            base_row=self.row_mapping.vertical_region[0],
            row_count=self.row_mapping.vertical_region[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )

        perform_record_aligned_horizontal_layout(
            base_row=self.row_mapping.horizontal_region[0],
            row_count=self.row_mapping.horizontal_region[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )


class BlimpAmbitIndexHitmapBankLayoutConfiguration(
    GenericBlimpAmbitHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        BlimpPlusAmbitHardwareConfiguration, BlimpPlusAmbitHitmapDatabaseConfiguration,
        BlimpAmbitHitmapLayoutMetadata, BlimpAmbitHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a standard BLIMP orchestrated Ambit database bank. This configuration
    places indices vertically in the bank only with reservations for index hitmaps

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
    -                      ...                      -
    -                                               -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                    HITMAPS                    -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -            AMBIT TEMPORARY SWAP SPACE         -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                AMBIT CONTROL ZERO             -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                AMBIT CONTROL ONE              -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                    AMBIT DCC                  -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    -                  AMBIT COMPUTE                -
    + - - - - - - - - - - - - - - - - - - - - - - - +
    +-----------------------------------------------+
    |                   ROW  BUFFER                 |
    +-----------------------------------------------+

    """

    def __init__(
            self,
            hardware: BlimpPlusAmbitHardwareConfiguration,
            database: BlimpPlusAmbitHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined temporary rows (ambit D group) for ambit calculations
        total_rows_for_temporary_ambit_compute = self._database_configuration.ambit_temporary_bits

        # Ambit compute region rows, B and C groups, with optional T (temp) group
        total_rows_for_reserved_ambit_compute = \
            self._hardware_configuration.ambit_dcc_rows * 2 + \
            self._hardware_configuration.ambit_control_group_rows + \
            self._hardware_configuration.ambit_compute_register_rows

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

        # Data rows remaining for data and hitmaps
        total_rows_configurable = self._hardware_configuration.bank_rows - \
            (total_rows_for_reserved_ambit_compute + total_rows_for_temporary_ambit_compute) - \
            (total_rows_for_blimp_code_region + total_rows_for_blimp_temp_region)

        if total_rows_configurable < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (total_rows_configurable // (self._database_configuration.total_index_size_bytes * 8))

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        total_rows_for_data = ceil_to_multiple(
            limit_records * (self._database_configuration.total_index_size_bytes * 8) /
            (self._hardware_configuration.row_buffer_size_bytes * 8),
            base=self._database_configuration.total_index_size_bytes * 8
        )
        total_rows_for_hitmaps = int(math.ceil(
            limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
        ) * self._database_configuration.hitmap_count

        while total_rows_for_hitmaps + total_rows_for_data > total_rows_configurable and limit_records > 0:
            # Start cutting back
            if limit_records % (self.hardware_configuration.row_buffer_size_bytes * 8) == 0:
                limit_records -= self.hardware_configuration.row_buffer_size_bytes * 8
            else:
                limit_records -= limit_records % \
                                             (self.hardware_configuration.row_buffer_size_bytes * 8)
            # Recalc
            total_rows_for_data = ceil_to_multiple(
                limit_records * (self._database_configuration.total_index_size_bytes * 8) /
                (self._hardware_configuration.row_buffer_size_bytes * 8),
                base=self._database_configuration.total_index_size_bytes * 8
            )
            total_rows_for_hitmaps = int(math.ceil(
                limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
            ) * self._database_configuration.hitmap_count

        # Ensure we have at least a non-zero number of records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        self._layout_metadata = BlimpAmbitHitmapLayoutMetadata(
            total_rows_for_horizontal_data=0,
            total_rows_for_vertical_data=total_rows_for_data,
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_configurable_data=total_rows_configurable,
            total_rows_for_ambit_temp_region=total_rows_for_temporary_ambit_compute,
            total_rows_for_ambit_compute_region=total_rows_for_reserved_ambit_compute,
            total_rows_for_blimp_code_region=total_rows_for_blimp_code_region,
            total_rows_for_blimp_temp_region=total_rows_for_blimp_temp_region,
            total_rows_for_records=total_rows_for_data,
            total_records_processable=limit_records,
        )

        base = 0

        # BLIMP Code region always starts at row 0
        blimp_code_region = (base, total_rows_for_blimp_code_region)
        base += total_rows_for_blimp_code_region

        blimp_temp_region = (base, total_rows_for_blimp_temp_region)
        base += total_rows_for_blimp_temp_region

        vertical_data_region = (base, total_rows_for_data)
        base += total_rows_for_data

        horizontal_data_region = (base, 0)
        base += 0

        hitmap_region = (base, total_rows_for_hitmaps)
        base += total_rows_for_hitmaps

        ambit_temp_region = (base, total_rows_for_temporary_ambit_compute)
        base += total_rows_for_temporary_ambit_compute

        # Ambit regions are always at the bottom
        base = self._hardware_configuration.bank_rows

        base -= self._hardware_configuration.ambit_compute_register_rows
        ambit_compute_region = (base, self._hardware_configuration.ambit_compute_register_rows)

        base -= self._hardware_configuration.ambit_dcc_rows * 2
        ambit_dcc_region = (base, self._hardware_configuration.ambit_dcc_rows * 2)

        base -= self._hardware_configuration.ambit_control_group_rows
        ambit_control_region = (base, self._hardware_configuration.ambit_control_group_rows)

        self._row_mapping_set = BlimpAmbitHitmapRowMapping(
            hitmaps=hitmap_region,
            ambit_control_rows=ambit_control_region,
            ambit_dcc_rows=ambit_dcc_region,
            ambit_compute_rows=ambit_compute_region,
            ambit_temp_rows=ambit_temp_region,
            blimp_code_region=blimp_code_region,
            blimp_temp_region=blimp_temp_region,
            horizontal_region=horizontal_data_region,
            vertical_region=vertical_data_region,
            data=(vertical_data_region[0], total_rows_for_data),
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many records into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_msb_vertical_layout(
            base_row=self.row_mapping.vertical_region[0],
            row_count=self.row_mapping.vertical_region[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )
