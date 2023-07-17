from src.generators import DatabaseRecordGenerator, DataGenerator
from src.generators.data_generators import NullDataGenerator, ConstantDataGenerator

from src.configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration
from src.configurations.database.blimp import BlimpDatabaseConfiguration
from src.data_layout_mappings.architectures import BlimpIndexBankLayoutConfiguration
from src.hardware.architectures import BlimpBank, BlimpVectorBank
from src.simulators.hardware import SimulatedBlimpBank, SimulatedBlimpVBank
from queries.aggregate.blimp import BlimpAggregate
from queries.aggregate.blimpv import BlimpVAggregate
from src.simulators.result import RuntimeResult, HitmapResult, MemoryArrayResult


USE_BLIMPV = False
KEY_SIZE_BYTES = 4
OUTPUT_SIZE = 4
BANK_SIZE_BYTES = 33554432
RECORD_LIMITER = 89411


class KnownTruthRecordGenerator(DatabaseRecordGenerator):

    def __init__(self, total_records=None, records=None):
        super().__init__(
            ConstantDataGenerator(KEY_SIZE_BYTES, 1),
            NullDataGenerator(),
            total_records,
            records
        )


record_generator_configuration = {
    "total_records": RECORD_LIMITER
}

generic_hardware_configuration = {
    "bank_size_bytes": BANK_SIZE_BYTES,
    "row_buffer_size_bytes": 1024,
    "time_to_row_activate_ns": 21,
    "time_to_column_activate_ns": 15.0,
    "time_to_precharge_ns": 21,
    "time_to_bank_communicate_ns": 100,
    "cpu_frequency": 2200000000,
    "cpu_cache_block_size_bytes": 64,
    "number_of_vALUs": 32,
    "number_of_vFPUs": 0,
    "blimpv_sew_max_bytes": 8,
    "blimpv_sew_min_bytes": 1,
    "blimp_frequency": 200000000,
    "time_to_v0_transfer_ns": 5,
    "blimp_processor_bit_architecture": 64,
    "ambit_compute_register_rows": 6,
    "ambit_dcc_rows": 2,
    "blimp_extension_popcount": True,
    "blimpv_extension_vpopcount": True,
}

generic_query_params = {
    "sum_size_bytes": OUTPUT_SIZE,
}

generic_database_configuration = {
    "total_record_size_bytes": KEY_SIZE_BYTES,
    "total_index_size_bytes": KEY_SIZE_BYTES,
    "blimp_code_region_size_bytes": 2048,
    "blimp_temporary_region_size_bytes": 1 * generic_hardware_configuration["row_buffer_size_bytes"],
    "ambit_temporary_bits": 0,
    "hitmap_count": 0,
    "early_termination_frequency": 4
}

record_generator = KnownTruthRecordGenerator(**record_generator_configuration)

hardware_configuration = (
    BlimpVectorHardwareConfiguration if USE_BLIMPV else BlimpHardwareConfiguration
)(**generic_hardware_configuration)
database_configuration = BlimpDatabaseConfiguration(**generic_database_configuration)
layout_configuration = BlimpIndexBankLayoutConfiguration(
    hardware=hardware_configuration,
    database=database_configuration,
    generator=record_generator
)

bank_hardware = (
    BlimpVectorBank if USE_BLIMPV else BlimpBank
)(layout_configuration.hardware_configuration, default_byte_value=0)
layout_configuration.perform_data_layout(bank=bank_hardware)
layout_configuration.display()

simulator = (SimulatedBlimpVBank if USE_BLIMPV else SimulatedBlimpBank)(bank_hardware)
query = (BlimpVAggregate if USE_BLIMPV else BlimpAggregate)(simulator=simulator, layout_configuration=layout_configuration)

runtime, memory_result = query.perform_operation(**generic_query_params)  # type: (RuntimeResult, MemoryArrayResult)
print(f"\tsimulated runtime was {runtime.runtime:,}ns")
print(f"\treturned {memory_result.result_count:,} hits")
print(f"\treturned {memory_result.result_array}")
