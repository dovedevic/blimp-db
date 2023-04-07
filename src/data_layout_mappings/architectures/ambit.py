import math

from pydantic import Field

from src.configurations.hardware.ambit import AmbitHardwareConfiguration
from src.configurations.database.ambit import AmbitDatabaseConfiguration, AmbitHitmapDatabaseConfiguration
from src.hardware import Bank
from src.generators import DatabaseRecordGenerator
from src.data_layout_mappings import RowMappingSet, RowMapping, LayoutMetadata, DataLayoutConfiguration
from src.data_layout_mappings.architectures.hitmap import \
    GenericHitmapBankLayoutConfiguration, GenericHitmapRowMapping, GenericHitmapLayoutMetadata
from src.data_layout_mappings.methods import perform_record_msb_vertical_layout, perform_index_msb_vertical_layout
from src.utils.generic import ceil_to_multiple


class AmbitRowMapping(RowMappingSet):
    """Ambit row mappings for compute and temp data regions"""
    ambit_control_rows: RowMapping = Field(
        description="The start row address for Ambit control registers (ZERO and ONE)")
    ambit_dcc_rows: RowMapping = Field(
        description="The start row address for Ambit DCC (Dual Contact Cell) rows. Includes both POS and NEG rows")
    ambit_compute_rows: RowMapping = Field(
        description="he start row address for Ambit reserved rows for compute orchestration (TRA, CP, DCC, ect...)")
    ambit_temp_rows: RowMapping = Field(
        description="The start row address for Ambit reserved rows temp storage or swap space")


class AmbitLayoutMetadata(LayoutMetadata):
    """Metadata for standard BLIMP layout"""
    total_rows_for_configurable_data: int = Field(
        description="The total number of rows that are available after reservations are taken into effect")
    total_rows_for_ambit_temp_region: int = Field(
        description="The total number of rows reserved for Ambit D-row temporary compute")
    total_rows_for_ambit_compute_region: int = Field(
        description="The total number of rows reserved for Ambit compute")


class AmbitHitmapRowMapping(AmbitRowMapping, GenericHitmapRowMapping):
    """Standard Ambit row mappings but with space for hitmaps"""
    pass


class AmbitHitmapLayoutMetadata(AmbitLayoutMetadata, GenericHitmapLayoutMetadata):
    """Metadata for a standard BLIMP layout but with hitmaps"""
    pass


class GenericAmbitBankLayoutConfiguration(DataLayoutConfiguration):
    def reset_ambit_control_rows(self, bank: Bank):
        """Reset/Initialize all ambit controlled rows; This sets the C-group, and defines the B-group rows"""
        self._logger.info("initializing ambit-reserved control and bitwise rows, temporary ambit D-group space")

        _ambit_one = (2 ** (bank.hardware_configuration.row_buffer_size_bytes * 8)) - 1
        _ambit_zero = 0

        # Set the C-group rows
        # C0
        bank.set_raw_row(self._row_mapping_set.ambit_control_rows[0], _ambit_zero)
        # C1
        bank.set_raw_row(self._row_mapping_set.ambit_control_rows[0] + 1, _ambit_one)

        # Set the B-group rows
        # Set the Dual-Contact-Cells (DCC)
        for dcc in range(bank.hardware_configuration.ambit_dcc_rows):
            # DCCi / !DCCi
            bank.set_raw_row(self._row_mapping_set.ambit_dcc_rows[0] + 2 * dcc, _ambit_zero)
            bank.set_raw_row(self._row_mapping_set.ambit_dcc_rows[0] + 2 * dcc + 1, _ambit_one)
        # Set the Compute/Temp (T) registers
        for t in range(bank.hardware_configuration.ambit_compute_register_rows):
            bank.set_raw_row(self._row_mapping_set.ambit_compute_rows[0] + t, _ambit_zero)


class GenericAmbitHitmapBankLayoutConfiguration(GenericAmbitBankLayoutConfiguration,
                                                GenericHitmapBankLayoutConfiguration):
    pass


class StandardAmbitBankLayoutConfiguration(
    GenericAmbitBankLayoutConfiguration,
    DataLayoutConfiguration[
        AmbitHardwareConfiguration, AmbitDatabaseConfiguration, AmbitLayoutMetadata, AmbitRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a standard Ambit database bank. This configuration places records
    vertically in the bank fitting as many whole-records into a row buffer as it can at a time

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
            hardware: AmbitHardwareConfiguration,
            database: AmbitDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined temporary rows (ambit D group) for ambit calculations
        total_rows_for_temporary_ambit_compute = self._database_configuration.ambit_temporary_bits

        # Ambit compute region rows, B and C groups, with optional T (temp) group
        total_rows_for_reserved_ambit_compute = \
            self._hardware_configuration.ambit_dcc_rows * 2 + \
            self._hardware_configuration.ambit_control_group_rows + \
            self._hardware_configuration.ambit_compute_register_rows

        # "Read-only" data rows remaining
        total_rows_for_data = self._hardware_configuration.bank_rows - \
            (total_rows_for_reserved_ambit_compute + total_rows_for_temporary_ambit_compute)

        if total_rows_for_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (total_rows_for_data // (self._database_configuration.total_record_size_bytes * 8))

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = AmbitLayoutMetadata(
            total_rows_for_records=total_rows_for_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_data,
            total_rows_for_ambit_compute_region=total_rows_for_reserved_ambit_compute,
            total_rows_for_ambit_temp_region=total_rows_for_temporary_ambit_compute,
        )

        base = 0

        # Ambit data region begins at the top
        ambit_data_region = (base, total_rows_for_data)
        base += total_rows_for_data

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

        self._row_mapping_set = AmbitRowMapping(
            ambit_temp_rows=ambit_temp_region,
            ambit_compute_rows=ambit_compute_region,
            ambit_dcc_rows=ambit_dcc_region,
            ambit_control_rows=ambit_control_region,
            data=ambit_data_region,
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


class AmbitIndexBankLayoutConfiguration(
    GenericAmbitBankLayoutConfiguration,
    DataLayoutConfiguration[
        AmbitHardwareConfiguration, AmbitDatabaseConfiguration, AmbitLayoutMetadata, AmbitRowMapping
    ]
):
    """
    Defines the row/data layout configuration for an Ambit database bank. This configuration places record indices
    vertically in the bank fitting as many whole-indices into a row buffer as it can at a time

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
            hardware: AmbitHardwareConfiguration,
            database: AmbitDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined temporary rows (ambit D group) for ambit calculations
        total_rows_for_temporary_ambit_compute = self._database_configuration.ambit_temporary_bits

        # Ambit compute region rows, B and C groups, with optional T (temp) group
        total_rows_for_reserved_ambit_compute = \
            self._hardware_configuration.ambit_dcc_rows * 2 + \
            self._hardware_configuration.ambit_control_group_rows + \
            self._hardware_configuration.ambit_compute_register_rows

        # "Read-only" data rows remaining
        total_rows_for_data = self._hardware_configuration.bank_rows - \
            (total_rows_for_reserved_ambit_compute + total_rows_for_temporary_ambit_compute)

        if total_rows_for_data < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (total_rows_for_data // (self._database_configuration.total_index_size_bytes * 8))

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        self._layout_metadata = AmbitLayoutMetadata(
            total_rows_for_records=total_rows_for_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_for_data,
            total_rows_for_ambit_compute_region=total_rows_for_reserved_ambit_compute,
            total_rows_for_ambit_temp_region=total_rows_for_temporary_ambit_compute,
        )

        base = 0

        # Ambit data region begins at the top
        ambit_data_region = (base, total_rows_for_data)
        base += total_rows_for_data

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

        self._row_mapping_set = AmbitRowMapping(
            ambit_temp_rows=ambit_temp_region,
            ambit_compute_rows=ambit_compute_region,
            ambit_dcc_rows=ambit_dcc_region,
            ambit_control_rows=ambit_control_region,
            data=ambit_data_region,
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


class AmbitHitmapBankLayoutConfiguration(
    GenericAmbitHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        AmbitHardwareConfiguration, AmbitHitmapDatabaseConfiguration, AmbitHitmapLayoutMetadata, AmbitHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for a Ambit database bank  with hitmaps. This configuration places records
    vertically in the bank fitting as many whole-records into a row buffer as it can at a time

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
    -                     HITMAPS                   -
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
            hardware: AmbitHardwareConfiguration,
            database: AmbitHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator
    ):
        super().__init__(hardware, database, generator)

        # User-defined temporary rows (ambit D group) for ambit calculations
        total_rows_for_temporary_ambit_compute = self._database_configuration.ambit_temporary_bits

        # Ambit compute region rows, B and C groups, with optional T (temp) group
        total_rows_for_reserved_ambit_compute = \
            self._hardware_configuration.ambit_dcc_rows * 2 + \
            self._hardware_configuration.ambit_control_group_rows + \
            self._hardware_configuration.ambit_compute_register_rows

        # Data rows remaining for data and hitmaps
        total_rows_configurable = self._hardware_configuration.bank_rows - \
            (total_rows_for_reserved_ambit_compute + total_rows_for_temporary_ambit_compute)

        if total_rows_configurable < 0:
            raise ValueError("There are not enough bank rows to satisfy static row constraints")

        total_records_processable = self._hardware_configuration.row_buffer_size_bytes * 8 * \
            (total_rows_configurable // (self._database_configuration.total_record_size_bytes * 8))

        limit_records = total_records_processable
        if self._record_generator.get_max_records() is not None:
            limit_records = min(limit_records, self._record_generator.get_max_records())

        total_rows_for_data = ceil_to_multiple(
            limit_records * (self._database_configuration.total_record_size_bytes * 8) /
            (self._hardware_configuration.row_buffer_size_bytes * 8),
            base=self._database_configuration.total_record_size_bytes * 8
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
                limit_records * (self._database_configuration.total_record_size_bytes * 8) /
                (self._hardware_configuration.row_buffer_size_bytes * 8),
                base=self._database_configuration.total_record_size_bytes * 8
            )
            total_rows_for_hitmaps = int(math.ceil(
                limit_records / (self.hardware_configuration.row_buffer_size_bytes * 8))
            ) * self._database_configuration.hitmap_count

        # Ensure we have at least a non-zero number of records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        self._layout_metadata = AmbitHitmapLayoutMetadata(
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_records=total_rows_for_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_configurable,
            total_rows_for_ambit_compute_region=total_rows_for_reserved_ambit_compute,
            total_rows_for_ambit_temp_region=total_rows_for_temporary_ambit_compute,
        )

        base = 0

        # Ambit data region begins at the top
        ambit_data_region = (base, total_rows_for_data)
        base += total_rows_for_data

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

        self._row_mapping_set = AmbitHitmapRowMapping(
            hitmaps=hitmap_region,
            ambit_temp_rows=ambit_temp_region,
            ambit_compute_rows=ambit_compute_region,
            ambit_dcc_rows=ambit_dcc_region,
            ambit_control_rows=ambit_control_region,
            data=ambit_data_region,
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


class AmbitIndexHitmapBankLayoutConfiguration(
    GenericAmbitHitmapBankLayoutConfiguration,
    DataLayoutConfiguration[
        AmbitHardwareConfiguration, AmbitHitmapDatabaseConfiguration, AmbitHitmapLayoutMetadata, AmbitHitmapRowMapping
    ]
):
    """
    Defines the row/data layout configuration for an Ambit database bank with hitmaps. This configuration places record
    indices vertically in the bank fitting as many whole-index records into a row buffer as it can at a time

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
    -                     HITMAPS                   -
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
            hardware: AmbitHardwareConfiguration,
            database: AmbitHitmapDatabaseConfiguration,
            generator: DatabaseRecordGenerator):
        super().__init__(hardware, database, generator)

        # User-defined temporary rows (ambit D group) for ambit calculations
        total_rows_for_temporary_ambit_compute = self._database_configuration.ambit_temporary_bits

        # Ambit compute region rows, B and C groups, with optional T (temp) group
        total_rows_for_reserved_ambit_compute = \
            self._hardware_configuration.ambit_dcc_rows * 2 + \
            self._hardware_configuration.ambit_control_group_rows + \
            self._hardware_configuration.ambit_compute_register_rows

        # Data rows remaining for data and hitmaps
        total_rows_configurable = self._hardware_configuration.bank_rows - \
            (total_rows_for_reserved_ambit_compute + total_rows_for_temporary_ambit_compute)

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

        # Ensure we have at least a non-zero number of index/records processable
        if limit_records <= 0:
            raise ValueError("There are not enough bank rows to satisfy dynamic row constraints")

        self._layout_metadata = AmbitHitmapLayoutMetadata(
            total_rows_for_hitmaps=total_rows_for_hitmaps,
            total_rows_for_records=total_rows_for_data,
            total_records_processable=limit_records,
            total_rows_for_configurable_data=total_rows_configurable,
            total_rows_for_ambit_compute_region=total_rows_for_reserved_ambit_compute,
            total_rows_for_ambit_temp_region=total_rows_for_temporary_ambit_compute,
        )

        base = 0

        # Ambit data region begins at the top
        ambit_data_region = (base, total_rows_for_data)
        base += total_rows_for_data

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

        self._row_mapping_set = AmbitHitmapRowMapping(
            hitmaps=hitmap_region,
            ambit_temp_rows=ambit_temp_region,
            ambit_compute_rows=ambit_compute_region,
            ambit_dcc_rows=ambit_dcc_region,
            ambit_control_rows=ambit_control_region,
            data=ambit_data_region,
        )

    def perform_data_layout(self, bank: Bank):
        """Given a bank hardware and record generator, attempt to place as many indecies into the bank as possible"""
        assert self._hardware_configuration.row_buffer_size_bytes == bank.hardware_configuration.row_buffer_size_bytes
        assert self._hardware_configuration.bank_size_bytes == bank.hardware_configuration.bank_size_bytes

        perform_index_msb_vertical_layout(
            base_row=self.row_mapping.data[0],
            row_count=self.row_mapping.data[1],
            bank=bank,
            record_generator=self._record_generator,
            limit=self.layout_metadata.total_records_processable
        )
