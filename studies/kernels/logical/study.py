import math
import os
import shutil

from studies.study import QueryStudy
from src.utils.performance import start_performance_tracking, end_performance_tracking


generic_hardware_configuration = {
    "bank_size_bytes": 33554432,
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
    "total_record_size_bytes": generic_hardware_configuration["row_buffer_size_bytes"],
    "total_index_size_bytes": generic_hardware_configuration["row_buffer_size_bytes"],
    "blimp_code_region_size_bytes": 2048,
    "blimp_temporary_region_size_bytes": 0,
    "ambit_temporary_bits": 0,
    "hitmap_count": 2,
    "early_termination_frequency": 4
}

from src.generators.record_generators import IncrementalKeyNullDataRecordGenerator

from src.configurations.hardware.ambit import AmbitHardwareConfiguration, BlimpPlusAmbitHardwareConfiguration
from src.configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration

from src.configurations.database.ambit import AmbitHitmapDatabaseConfiguration, BlimpPlusAmbitHitmapDatabaseConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration, BlimpVectorHitmapDatabaseConfiguration

from src.data_layout_mappings.architectures import AmbitHitmapBankLayoutConfiguration
from src.data_layout_mappings.architectures import BlimpHitmapBankLayoutConfiguration
from src.data_layout_mappings.architectures import BlimpAmbitHitmapBankLayoutConfiguration

from src.hardware.architectures import AmbitBank, BlimpBank, BlimpAmbitBank, BlimpVectorBank

from src.simulators.hardware import SimulatedAmbitBank, SimulatedBlimpBank, SimulatedBlimpVBank, SimulatedBlimpAmbitBank

from src.queries.logical import AmbitHitmapLogicalAnd, BlimpVHitmapLogicalAnd, BlimpAmbitHitmapLogicalAnd, BlimpHitmapLogicalAnd


and_logical_studies = [
    QueryStudy(  # Ambit AND
        layout_configuration_type=AmbitHitmapBankLayoutConfiguration,
        hardware_configuration_type=AmbitHardwareConfiguration,
        database_configuration_type=AmbitHitmapDatabaseConfiguration,
        hardware_type=AmbitBank,
        simulator_type=SimulatedAmbitBank,
        query_type=AmbitHitmapLogicalAnd,
        name="ambit",
    ),
    QueryStudy(  # BLIMP AND
        layout_configuration_type=BlimpHitmapBankLayoutConfiguration,
        hardware_configuration_type=BlimpHardwareConfiguration,
        database_configuration_type=BlimpHitmapDatabaseConfiguration,
        hardware_type=BlimpBank,
        simulator_type=SimulatedBlimpBank,
        query_type=BlimpHitmapLogicalAnd,
        name="blimp",
    ),
    QueryStudy(  # BLIMP+ AND
        layout_configuration_type=BlimpAmbitHitmapBankLayoutConfiguration,
        hardware_configuration_type=BlimpPlusAmbitHardwareConfiguration,
        database_configuration_type=BlimpPlusAmbitHitmapDatabaseConfiguration,
        hardware_type=BlimpAmbitBank,
        simulator_type=SimulatedBlimpAmbitBank,
        query_type=BlimpAmbitHitmapLogicalAnd,
        name="blimp+",
    ),
    QueryStudy(  # BLIMP-V AND
        layout_configuration_type=BlimpHitmapBankLayoutConfiguration,
        hardware_configuration_type=BlimpVectorHardwareConfiguration,
        database_configuration_type=BlimpVectorHitmapDatabaseConfiguration,
        hardware_type=BlimpVectorBank,
        simulator_type=SimulatedBlimpVBank,
        query_type=BlimpVHitmapLogicalAnd,
        name="blimp-v",
    )
]


def perform_study_sweep(studies: [QueryStudy], hitmap_size_bytes: int, save_dir):
    for study in studies:
        print(f"Evaluating study `{study.name or '<no name given>'}`...")
        # Create the data layout configuration
        print(f"\tSetting up layout...", end=' ')
        start_performance_tracking()
        layout_class = study.layout_configuration_type(
            hardware=study.hardware_configuration_type(**generic_hardware_configuration),
            database=study.database_configuration_type(**generic_database_configuration),
            generator=IncrementalKeyNullDataRecordGenerator(0, 1)
        )
        # Force the hitmap size, kinda hacky
        total_rows_for_hitmaps = 2 * math.ceil(
            hitmap_size_bytes /
            layout_class.hardware_configuration.row_buffer_size_bytes
        )
        layout_class.layout_metadata.total_rows_for_hitmaps = total_rows_for_hitmaps
        layout_class.row_mapping.hitmaps = (0, total_rows_for_hitmaps)

        time = end_performance_tracking()
        print(f'done in {time}s.')

        # Create a bank object for this hardware enumeration
        print(f"\tSetting up bank...", end=' ')
        start_performance_tracking()
        bank = study.hardware_type(layout_class.hardware_configuration, default_byte_value=0)
        time = end_performance_tracking()
        print(f'done in {time}s.')

        # Setup the simulation
        print(f"\tSetting up simulation...", end=' ')
        start_performance_tracking()
        simulator = study.simulator_type(bank)
        query = study.query_type(simulator, layout_class)
        time = end_performance_tracking()
        print(f'done in {time}s.')

        # Perform the simulation
        print(f"\tPerforming simulation...", end=' ')
        start_performance_tracking()
        runtime, result = query.perform_operation(0, 1)
        time = end_performance_tracking()
        print(f'done in {time}s.')

        # Display the results
        print(f"\t{study.name} simulated runtime was {runtime.runtime:,}ns")

        study.runtime = runtime

    if save_dir is not None:  # Only dump results if save_dir is specified as a string
        save_dir = save_dir or (input(" > Provide a save directory name (default is cwd): ") or "./")
        if os.path.exists(save_dir):
            if input(f"Study directory '{save_dir}' already found. Delete and recreate? (y/N): ") == "y":
                shutil.rmtree(save_dir)
            else:
                print('Exiting...')
                return
        os.mkdir(save_dir)
        for study in studies:
            study.runtime.save(os.path.join(save_dir, study.name.replace(' ', '_') + '.runtime'))

    # Dump runtime values to TSV format in the console
    print("")
    for study in studies:
        print(f"{study.name}\t{study.runtime.runtime}")
    return studies


def main():
    perform_study_sweep(and_logical_studies, 2048, None)


if __name__ == '__main__':
    main()
