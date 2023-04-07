import os
import math

from studies.study import QueryStudy
from utils.performance import start_performance_tracking, end_performance_tracking

from src.generators.record_generators import BoundedRandomKeyNullDataRecordGenerator


PARALLELISM_FACTOR = 2048
KEY_SIZE_BYTES = 2
BANK_SIZE_BYTES = 33554432
RECORD_LIMITER = math.ceil(600000000 / PARALLELISM_FACTOR)

record_generator_configuration = {
    "pi_record_size": KEY_SIZE_BYTES,
    "min_bound": 1990,
    "max_bound": 1999,
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

generic_database_configuration = {
    "total_record_size_bytes": KEY_SIZE_BYTES,
    "total_index_size_bytes": KEY_SIZE_BYTES,
    "blimp_code_region_size_bytes": 2048,
    "blimp_temporary_region_size_bytes": 0,
    "ambit_temporary_bits": 0,
    "hitmap_count": 1,
    "early_termination_frequency": 4
}

generic_query_params = {
    "pi_subindex_offset_bytes": 0,
    "pi_element_size_bytes": KEY_SIZE_BYTES,
    "value": 1995,
    "return_labels": True,
    "hitmap_index": 0
}

from src.configurations.hardware.ambit import AmbitHardwareConfiguration
from src.configurations.hardware.blimp import BlimpHardwareConfiguration
from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration
from src.configurations.hardware.ambit import BlimpPlusAmbitHardwareConfiguration

from src.configurations.database.ambit import AmbitHitmapDatabaseConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.configurations.database.ambit import BlimpPlusAmbitHitmapDatabaseConfiguration

from src.data_layout_mappings.architectures import AmbitIndexHitmapBankLayoutConfiguration
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.data_layout_mappings.architectures import BlimpHitmapIndexBitweaveBankLayoutConfiguration
from src.data_layout_mappings.architectures import BlimpAmbitIndexHitmapBankLayoutConfiguration

from src.hardware.architectures import AmbitBank, BlimpBank, BlimpVectorBank, BlimpAmbitBank

from src.simulators.hardware import SimulatedAmbitBank, SimulatedBlimpBank, SimulatedBlimpVBank, SimulatedBlimpAmbitBank

from src.queries.filter.gte.ambit import AmbitHitmapGreaterThanOrEqual
from src.queries.filter.gte.blimp import BlimpHitmapGreaterThanOrEqual
from src.queries.filter.gte.blimpv import BlimpVHitmapGreaterThanOrEqual
from src.queries.filter.gte.blimp_ambit import BlimpAmbitHitmapGreaterThanOrEqual
from src.queries.filter.gte.blimpv_bitweave import BlimpVBitweaveHitmapGreaterThanOrEqual
from src.queries.filter.gte.blimp_bitweave import BlimpBitweaveHitmapGreaterThanOrEqual

ambit_studies = [
    QueryStudy(
        layout_configuration_type=AmbitIndexHitmapBankLayoutConfiguration,
        hardware_configuration_type=AmbitHardwareConfiguration,
        database_configuration_type=AmbitHitmapDatabaseConfiguration,
        hardware_type=AmbitBank,
        simulator_type=SimulatedAmbitBank,
        query_type=AmbitHitmapGreaterThanOrEqual,
        name="ambit",
    ),
]

blimp_studies = [
    QueryStudy(
        layout_configuration_type=BlimpIndexHitmapBankLayoutConfiguration,
        hardware_configuration_type=BlimpHardwareConfiguration,
        database_configuration_type=BlimpHitmapDatabaseConfiguration,
        hardware_type=BlimpBank,
        simulator_type=SimulatedBlimpBank,
        query_type=BlimpHitmapGreaterThanOrEqual,
        name="blimp",
    ),
    QueryStudy(
        layout_configuration_type=BlimpHitmapIndexBitweaveBankLayoutConfiguration,
        hardware_configuration_type=BlimpHardwareConfiguration,
        database_configuration_type=BlimpHitmapDatabaseConfiguration,
        hardware_type=BlimpBank,
        simulator_type=SimulatedBlimpBank,
        query_type=BlimpBitweaveHitmapGreaterThanOrEqual,
        name="blimp bitweave",
    ),
]

blimpv_studies = [
    QueryStudy(
        layout_configuration_type=BlimpIndexHitmapBankLayoutConfiguration,
        hardware_configuration_type=BlimpVectorHardwareConfiguration,
        database_configuration_type=BlimpHitmapDatabaseConfiguration,
        hardware_type=BlimpVectorBank,
        simulator_type=SimulatedBlimpVBank,
        query_type=BlimpVHitmapGreaterThanOrEqual,
        name="blimpv",
    ),
    QueryStudy(
        layout_configuration_type=BlimpHitmapIndexBitweaveBankLayoutConfiguration,
        hardware_configuration_type=BlimpVectorHardwareConfiguration,
        database_configuration_type=BlimpHitmapDatabaseConfiguration,
        hardware_type=BlimpVectorBank,
        simulator_type=SimulatedBlimpVBank,
        query_type=BlimpVBitweaveHitmapGreaterThanOrEqual,
        name="blimpv bitweave",
    ),
]

blimp_ambit_studies = [
    QueryStudy(
        layout_configuration_type=BlimpAmbitIndexHitmapBankLayoutConfiguration,
        hardware_configuration_type=BlimpPlusAmbitHardwareConfiguration,
        database_configuration_type=BlimpPlusAmbitHitmapDatabaseConfiguration,
        hardware_type=BlimpAmbitBank,
        simulator_type=SimulatedBlimpAmbitBank,
        query_type=BlimpAmbitHitmapGreaterThanOrEqual,
        name="blimp ambit",
    ),
]


def perform_studies(study_path: str, studies: [QueryStudy]):
    study_path = study_path or ""
    save_files = study_path != ""
    result_tuples = []
    if save_files and not os.path.exists(study_path):
        print('creating studies directory...')
        os.mkdir(study_path)

    if os.path.exists(os.path.join(study_path, "records.save")):
        print("loading records.save...", end='')
        start_performance_tracking()
        record_generator = BoundedRandomKeyNullDataRecordGenerator.load(os.path.join(study_path, "records.save"))
        time = end_performance_tracking()
        print(f' loaded in {time}s')
    else:
        print("generating records...", end='')
        start_performance_tracking()
        record_generator = BoundedRandomKeyNullDataRecordGenerator(**record_generator_configuration)
        if save_files:
            record_generator.save(os.path.join(study_path, "records.save"))
        time = end_performance_tracking()
        print(f' generated and saved in {time}s')

    for study in studies:
        print(f'beginning study {study.name}')
        if save_files and not os.path.exists(os.path.join(study_path, study.name)):
            print('\tcreating study directory...')
            os.mkdir(os.path.join(study_path, study.name))

        # Generate/Load Configuration
        if os.path.exists(os.path.join(study_path, study.name, 'configuration.json')):
            print("\tloading configuration...", end='')
            start_performance_tracking()
            layout_configuration = study.layout_configuration_type.load(
                os.path.join(study_path, study.name, 'configuration.json'),
                hardware_config=study.hardware_configuration_type,
                database_config=study.database_configuration_type,
            )
            time = end_performance_tracking()
            print(f' loaded in {time}s')
        else:
            print("\tgenerating configuration...", end='')
            start_performance_tracking()
            hardware_configuration = study.hardware_configuration_type(**generic_hardware_configuration)
            database_configuration = study.database_configuration_type(**generic_database_configuration)
            layout_configuration = study.layout_configuration_type(
                hardware=hardware_configuration,
                database=database_configuration,
                generator=record_generator
            )
            if save_files:
                layout_configuration.save(os.path.join(study_path, study.name, 'configuration.json'))
            time = end_performance_tracking()
            print(f' generated and saved in {time}s')

        # Generate/Load Bank Memory
        if os.path.exists(os.path.join(study_path, study.name, 'bank.memdump')):
            print("\tloading memory state...", end='')
            start_performance_tracking()
            bank_hardware = study.hardware_type.load(
                os.path.join(study_path, study.name, 'bank.memdump'),
                hardware_config=study.hardware_configuration_type,
            )
            time = end_performance_tracking()
            print(f' loaded in {time}s')
        else:
            print("\tgenerating memory state...", end='')
            start_performance_tracking()
            bank_hardware = study.hardware_type(layout_configuration.hardware_configuration, default_byte_value=0)
            time = end_performance_tracking()
            print(f' generated in {time}s')

            print("\tperforming data layout...", end='')
            start_performance_tracking()
            layout_configuration.perform_data_layout(bank=bank_hardware)
            time = end_performance_tracking()
            print(f' laid out in {time}s')

            print("\tdumping memory state...", end='')
            start_performance_tracking()
            if save_files:
                bank_hardware.save(os.path.join(study_path, study.name, 'bank.memdump'), dump_with_ascii=False)
            time = end_performance_tracking()
            print(f' dumped in {time}s')

        # Initialize the simulator
        print("\tinitializing simulator...", end='')
        start_performance_tracking()
        simulator = study.simulator_type(bank_hardware)
        time = end_performance_tracking()
        print(f' initialized in {time}s')

        # Initialize the query
        print("\tinitializing query...", end='')
        start_performance_tracking()
        query = study.query_type(simulator=simulator, layout_configuration=layout_configuration)
        time = end_performance_tracking()
        print(f' initialized in {time}s')

        # Run the query
        print("\trunning query...", end='')
        start_performance_tracking()
        runtime, result = query.perform_operation(**generic_query_params)
        time = end_performance_tracking()
        print(f' completed in {time}s')

        # Save the results
        print("\tsaving query results...", end='')
        start_performance_tracking()
        study.runtime = runtime
        if save_files:
            runtime.save(os.path.join(study_path, study.name, 'query_runtime.txt'))
            result.save(os.path.join(study_path, study.name, 'query_result.txt'))
        time = end_performance_tracking()
        print(f' saved in {time}s')

        # Display the results
        print(f"\t{study.name} return {result.result_count:,} hits")
        print(f"\t{study.name} simulated runtime was {runtime.runtime:,}ns")

        result_tuples.append((
            study.name,
            runtime.runtime,
            result.result_count
        ))

        print(f'finished study {study.name}')
    print(f'finished study suite {study_path}')

    for name, runtime, hitcount in result_tuples:
        print(f"{name}\t{runtime}\t{hitcount}")


perform_studies("", ambit_studies + blimp_studies + blimpv_studies + blimp_ambit_studies)
