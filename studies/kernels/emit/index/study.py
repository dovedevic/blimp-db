from src.generators import DatabaseRecordGenerator, DataGenerator
from src.generators.data_generators import NullDataGenerator

from src.configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.hardware.architectures import BlimpBank, BlimpVectorBank
from src.simulators.hardware import SimulatedBlimpBank, SimulatedBlimpVBank
from queries.emit.index.blimp import BlimpHitmapEmit
from src.simulators.result import RuntimeResult, HitmapResult, MemoryArrayResult


USE_BLIMPV = True
KEY_SIZE_BYTES = 4
BANK_SIZE_BYTES = 33554432
RECORD_LIMITER = 8193


class KnownTruthRecordGenerator(DatabaseRecordGenerator):
    class KnownKeys(DataGenerator):
        def __init__(self):
            super().__init__(KEY_SIZE_BYTES)

        def _generate(self):
            return self.items_generated

    def __init__(self, total_records=None, records=None):
        super().__init__(
            self.KnownKeys(),
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
    "hitmap_index": 0,
    "output_array_start_row": 3,
    "return_labels": True,
}

generic_database_configuration = {
    "total_record_size_bytes": KEY_SIZE_BYTES,
    "total_index_size_bytes": KEY_SIZE_BYTES,
    "blimp_code_region_size_bytes": 2048,
    "blimp_temporary_region_size_bytes": 33 * generic_hardware_configuration["row_buffer_size_bytes"],
    "ambit_temporary_bits": 0,
    "hitmap_count": 1,
    "early_termination_frequency": 4
}

record_generator = KnownTruthRecordGenerator(**record_generator_configuration)

hardware_configuration = (
    BlimpVectorHardwareConfiguration if USE_BLIMPV else BlimpHardwareConfiguration
)(**generic_hardware_configuration)
database_configuration = BlimpHitmapDatabaseConfiguration(**generic_database_configuration)
layout_configuration = BlimpIndexHitmapBankLayoutConfiguration(
    hardware=hardware_configuration,
    database=database_configuration,
    generator=record_generator
)

bank_hardware = (
    BlimpVectorBank if USE_BLIMPV else BlimpBank
)(layout_configuration.hardware_configuration, default_byte_value=0)
layout_configuration.perform_data_layout(bank=bank_hardware)

layout_configuration.load_hitmap_result(
    bank=bank_hardware,
    result=HitmapResult.from_hitmap_byte_array(
        hitmap_byte_array=[255] + [0]*1022 + [254] + [255],
        num_bits=RECORD_LIMITER
    ),
    index=0
)
layout_configuration.display()

simulator = (SimulatedBlimpVBank if USE_BLIMPV else SimulatedBlimpBank)(bank_hardware)
query = BlimpHitmapEmit(simulator=simulator, layout_configuration=layout_configuration)

runtime, memory_result = query.perform_operation(**generic_query_params)  # type: (RuntimeResult, MemoryArrayResult)
print(f"\tsimulated runtime was {runtime.runtime:,}ns")
print(f"\treturned {memory_result.result_count:,} hits")
print(f"\treturned {memory_result.result_array}")
