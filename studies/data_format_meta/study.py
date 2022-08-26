import json

from pydantic import BaseModel
from typing import Type, Optional

from configurations import HardwareConfiguration, DatabaseConfiguration
from data_layout_mappings import DataLayoutConfiguration, LayoutMetadata, RowMappingSet
from generators.record_generators import IncrementalKeyConstantDataRecordGenerator
from hardware import Bank
from utils.performance import start_performance_tracking, end_performance_tracking

from configurations.hardware.ambit import AmbitHardwareConfiguration
from configurations.hardware.ambit import BlimpPlusAmbitHitmapHardwareConfiguration
from configurations.hardware.blimp import BlimpHardwareConfiguration

from configurations.database.ambit import AmbitDatabaseConfiguration, AmbitHitmapDatabaseConfiguration
from configurations.database.ambit import BlimpPlusAmbitHitmapDatabaseConfiguration
from configurations.database.blimp import BlimpDatabaseConfiguration, BlimpHitmapDatabaseConfiguration

from data_layout_mappings.architectures import \
    StandardBlimpBankLayoutConfiguration, \
    BlimpHitmapBankLayoutConfiguration, \
    BlimpIndexHitmapBankLayoutConfiguration, \
    BlimpIndexBankLayoutConfiguration, \
    BlimpRecordBitweaveBankLayoutConfiguration, \
    BlimpIndexBitweaveBankLayoutConfiguration, \
    BlimpHitmapRecordBitweaveBankLayoutConfiguration, \
    BlimpHitmapIndexBitweaveBankLayoutConfiguration
from data_layout_mappings.architectures import \
    StandardAmbitBankLayoutConfiguration, \
    AmbitIndexBankLayoutConfiguration, \
    AmbitHitmapBankLayoutConfiguration, \
    AmbitIndexHitmapBankLayoutConfiguration
from data_layout_mappings.architectures import \
    StandardPackedDataLayout, \
    StandardAlignedDataLayout, \
    StandardPackedIndexDataLayout, \
    StandardAlignedIndexDataLayout, \
    StandardBitweaveVerticalRecordDataLayout, \
    StandardBitweaveVerticalIndexDataLayout
from data_layout_mappings.architectures import \
    BlimpAmbitHitmapBankLayoutConfiguration


class LayoutStudy(BaseModel):
    # Static
    layout_type: Type[DataLayoutConfiguration]
    hardware_type: Type[HardwareConfiguration]
    database_type: Type[DatabaseConfiguration]
    name: Optional[str]

    # Post-layout
    layout_metadata = Optional[LayoutMetadata]
    row_mapping = Optional[RowMappingSet]

    class Config:
        arbitrary_types_allowed = True


layout_formats = [
    # Standard Layouts
    LayoutStudy(  # 0
        layout_type=StandardPackedDataLayout,
        hardware_type=HardwareConfiguration,
        database_type=DatabaseConfiguration,
        name="Record Maximum",
    ),
    LayoutStudy(  # 1
        layout_type=StandardAlignedDataLayout,
        hardware_type=HardwareConfiguration,
        database_type=DatabaseConfiguration,
        name="Aligned Record Maximum",
    ),
    LayoutStudy(  # 2
        layout_type=StandardPackedIndexDataLayout,
        hardware_type=HardwareConfiguration,
        database_type=DatabaseConfiguration,
        name="Index Maximum",
    ),
    LayoutStudy(  # 3
        layout_type=StandardAlignedIndexDataLayout,
        hardware_type=HardwareConfiguration,
        database_type=DatabaseConfiguration,
        name="Aligned Index Maximum",
    ),
    LayoutStudy(  # 4
        layout_type=StandardBitweaveVerticalRecordDataLayout,
        hardware_type=HardwareConfiguration,
        database_type=DatabaseConfiguration,
        name="Bitweave-V Record Maximum",
    ),
    LayoutStudy(  # 5
        layout_type=StandardBitweaveVerticalIndexDataLayout,
        hardware_type=HardwareConfiguration,
        database_type=DatabaseConfiguration,
        name="Bitweave-V Index Maximum",
    ),

    # Ambit Layouts
    LayoutStudy(  # 6
        layout_type=StandardAmbitBankLayoutConfiguration,
        hardware_type=AmbitHardwareConfiguration,
        database_type=AmbitDatabaseConfiguration,
        name="Record Ambit",
    ),
    LayoutStudy(  # 7
        layout_type=AmbitIndexBankLayoutConfiguration,
        hardware_type=AmbitHardwareConfiguration,
        database_type=AmbitDatabaseConfiguration,
        name="Index Ambit",
    ),
    LayoutStudy(  # 8
        layout_type=AmbitHitmapBankLayoutConfiguration,
        hardware_type=AmbitHardwareConfiguration,
        database_type=AmbitHitmapDatabaseConfiguration,
        name="Record Ambit w/ Hitmaps",
    ),
    LayoutStudy(  # 9
        layout_type=AmbitIndexHitmapBankLayoutConfiguration,
        hardware_type=AmbitHardwareConfiguration,
        database_type=AmbitHitmapDatabaseConfiguration,
        name="Index Ambit w/ Hitmaps",
    ),

    # BLIMP Layouts
    LayoutStudy(  # 10
        layout_type=StandardBlimpBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpDatabaseConfiguration,
        name="Record BLIMP",
    ),
    LayoutStudy(  # 11
        layout_type=BlimpHitmapBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpHitmapDatabaseConfiguration,
        name="Record BLIMP w/ Hitmaps",
    ),
    LayoutStudy(  # 12
        layout_type=BlimpIndexHitmapBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpHitmapDatabaseConfiguration,
        name="Index BLIMP/-V w/ Hitmaps",
    ),
    LayoutStudy(  # 13
        layout_type=BlimpIndexBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpDatabaseConfiguration,
        name="Index BLIMP/-V",
    ),
    LayoutStudy(  # 14
        layout_type=BlimpRecordBitweaveBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpDatabaseConfiguration,
        name="Bitweave-V Record BLIMP/-V",
    ),
    LayoutStudy(  # 15
        layout_type=BlimpIndexBitweaveBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpDatabaseConfiguration,
        name="Bitweave-V Index BLIMP/-V",
    ),
    LayoutStudy(  # 16
        layout_type=BlimpHitmapRecordBitweaveBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpHitmapDatabaseConfiguration,
        name="Bitweave-V Record BLIMP/-V w/ Hitmaps",
    ),
    LayoutStudy(  # 17
        layout_type=BlimpHitmapIndexBitweaveBankLayoutConfiguration,
        hardware_type=BlimpHardwareConfiguration,
        database_type=BlimpHitmapDatabaseConfiguration,
        name="Bitweave-V Index BLIMP/-V w/ Hitmaps",
    ),

    # Hybrid BLIMP+AMBIT Layouts
    LayoutStudy(  # 18
        layout_type=BlimpAmbitHitmapBankLayoutConfiguration,
        hardware_type=BlimpPlusAmbitHitmapHardwareConfiguration,
        database_type=BlimpPlusAmbitHitmapDatabaseConfiguration,
        name="Ambit+BLIMP Index+Record w/ Hitmaps",
    ),

    # to be continued...?

]


RECORD_SIZE_BYTES = 64
INDEX_SIZE_BYTES = 4

generator = IncrementalKeyConstantDataRecordGenerator(
    INDEX_SIZE_BYTES,
    RECORD_SIZE_BYTES,
    0xCC
)


def main():

    for layout_format in layout_formats:
        print(f"Evaluating study `{layout_format.name or '<no name given>'}`...")
        # Create the data layout configuration
        print(f"\tSetting up layout...", end=' ')
        start_performance_tracking()
        layout_class = layout_format.layout_type(
            hardware=layout_format.hardware_type(
                bank_size_bytes=33554432,
                row_buffer_size_bytes=1024,
                time_to_row_activate_ns=33.0,
                time_to_column_activate_ns=15.0,
                time_to_precharge_ns=14.06,
                time_to_bank_communicate_ns=0,
                cpu_frequency=0,
                number_of_vALUs=32,
                number_of_vFPUs=0,
                blimpv_sew_max_bytes=8,
                blimpv_sew_min_bytes=1,
                blimp_frequency=200000000,
                time_to_v0_transfer_ns=5,
                blimp_processor_bit_architecture=64,
                ambit_compute_register_rows=6,
                ambit_dcc_rows=2,
            ),
            database=layout_format.database_type(
                total_record_size_bytes=RECORD_SIZE_BYTES,
                total_index_size_bytes=INDEX_SIZE_BYTES,
                blimp_code_region_size_bytes=2048,
                blimp_temporary_region_size_bytes=1024,
                ambit_temporary_bits=1,
                hitmap_count=1,
            )
        )
        time = end_performance_tracking()
        print(f'done in {time}s.')

        # Create a bank object for this hardware enumeration
        # print(f"\tSetting up bank...", end=' ')
        # start_performance_tracking()
        # bank = Bank(layout_class.hardware_configuration, default_byte_value=0)
        # time = end_performance_tracking()
        # print(f'done in {time}s.')

        # Perform the data layout for this bank, database, and layout configuration
        # print(f"\tPerforming data layout", end=' ')
        # start_performance_tracking()
        # layout_class.perform_data_layout(bank, generator)
        # time = end_performance_tracking()
        # print(f'done in {time}s.')

        # Extract the results
        print(f"\tResults extracted.")
        layout_format.layout_metadata = layout_class.layout_metadata
        layout_format.row_mapping = layout_class.row_mapping

    with open(input(" > Provide a save name: "), "w") as fp:
        for layout in layout_formats:
            fp.write(f"{layout.name}\n")
            fp.write(f"\t{json.dumps(layout.layout_metadata.dict())}\n")
            fp.write(f"\t{json.dumps(layout.row_mapping.dict())}\n")


if __name__ == '__main__':
    main()
