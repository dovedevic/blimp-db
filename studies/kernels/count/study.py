import os
import math
import random

from src.utils.performance import start_performance_tracking, end_performance_tracking
from src.simulators.hardware import SimulatedBlimpVBank
from src.queries.count.blimpv import BlimpVHitmapCount
from src.hardware.architectures import BlimpVectorBank
from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.generators.record_generators import IncrementalKeyNullDataRecordGenerator
from src.data_layout_mappings.architectures import BlimpHitmapBankLayoutConfiguration


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
    "hitmap_count": 1,
    "early_termination_frequency": 4
}


def perform_study(study_path, hitmap_bit_array):
    if not os.path.exists(study_path):
        print('creating studies directory...')
        os.mkdir(study_path)

    # Create the data layout configuration
    print(f"\tSetting up layout...", end=' ')
    start_performance_tracking()
    layout_class = BlimpHitmapBankLayoutConfiguration(
        hardware=BlimpVectorHardwareConfiguration(**generic_hardware_configuration),
        database=BlimpHitmapDatabaseConfiguration(**generic_database_configuration),
        generator=IncrementalKeyNullDataRecordGenerator(0, 1)
    )
    # Force the hitmap size, kinda hacky
    total_hitmap_rows = math.ceil(
        len(hitmap_bit_array) / (layout_class.hardware_configuration.row_buffer_size_bytes * 8)
    )
    layout_class.layout_metadata.total_rows_for_hitmaps = total_hitmap_rows
    layout_class.row_mapping.hitmaps = (0, total_hitmap_rows)

    time = end_performance_tracking()
    print(f'done in {time}s.')

    # Create a bank object for this hardware enumeration
    print(f"\tSetting up bank...", end=' ')
    start_performance_tracking()
    bank = BlimpVectorBank(layout_class.hardware_configuration, default_byte_value=0)
    time = end_performance_tracking()
    print(f'done in {time}s.')

    # Set the hitmap rows to the desired set
    print(f"\tInserting hitmap array...", end=' ')
    start_performance_tracking()
    row_buffer_bits = layout_class.hardware_configuration.row_buffer_size_bytes * 8
    hitmap_row = 0
    while len(hitmap_bit_array):
        hitmap_buffer_chunk = hitmap_bit_array[:row_buffer_bits]
        hitmap_bit_array = hitmap_bit_array[row_buffer_bits:]
        if len(hitmap_buffer_chunk) != row_buffer_bits:
            hitmap_buffer_chunk += [0] * (row_buffer_bits - len(hitmap_buffer_chunk))
        chunk_value = 0
        for bit in hitmap_buffer_chunk:
            chunk_value <<= 1
            chunk_value += bit
        bank.set_raw_row(hitmap_row, chunk_value)
        hitmap_row += 1

    time = end_performance_tracking()
    print(f'done in {time}s.')

    # Setup the simulation
    print(f"\tSetting up simulation...", end=' ')
    start_performance_tracking()
    simulator = SimulatedBlimpVBank(bank)
    query = BlimpVHitmapCount(simulator, layout_class)
    time = end_performance_tracking()
    print(f'done in {time}s.')

    ###############################################################################

    # Run query #1
    query.hardware.hardware_configuration.blimpv_extension_vpopcount = True
    query.hardware.hardware_configuration.blimp_extension_popcount = False
    print("\trunning vpopcount query...", end='')
    start_performance_tracking()
    runtime, result = query.perform_operation(hitmap_index=0)
    time = end_performance_tracking()
    print(f' completed in {time}s')

    # Save the results
    print("\tsaving query results...", end='')
    start_performance_tracking()
    runtime.save(os.path.join(study_path, 'vpopcount_runtime.txt'))
    result.save(os.path.join(study_path, 'vpopcount_result.txt'))
    time = end_performance_tracking()
    print(f' saved in {time}s')

    # Display the results
    print(f"\tvpopcount returned {result.result_count:,}")
    print(f"\tvpopcount simulated runtime was {runtime.runtime:,}ns")

    ###############################################################################

    # Run query #2
    query.hardware.hardware_configuration.blimpv_extension_vpopcount = False
    query.hardware.hardware_configuration.blimp_extension_popcount = True
    print("\trunning popcount query...", end='')
    start_performance_tracking()
    runtime, result = query.perform_operation(hitmap_index=0)
    time = end_performance_tracking()
    print(f' completed in {time}s')

    # Save the results
    print("\tsaving query results...", end='')
    start_performance_tracking()
    runtime.save(os.path.join(study_path, 'popcount_runtime.txt'))
    result.save(os.path.join(study_path, 'popcount_result.txt'))
    time = end_performance_tracking()
    print(f' saved in {time}s')

    # Display the results
    print(f"\tpopcount returned {result.result_count:,}")
    print(f"\tpopcount simulated runtime was {runtime.runtime:,}ns")

    ###############################################################################

    # Run query #3
    query.hardware.hardware_configuration.blimpv_extension_vpopcount = False
    query.hardware.hardware_configuration.blimp_extension_popcount = False
    print("\trunning bit count query...", end='')
    start_performance_tracking()
    runtime, result = query.perform_operation(hitmap_index=0)
    time = end_performance_tracking()
    print(f' completed in {time}s')

    # Save the results
    print("\tsaving query results...", end='')
    start_performance_tracking()
    runtime.save(os.path.join(study_path, 'bitcount_runtime.txt'))
    result.save(os.path.join(study_path, 'bitcount_result.txt'))
    time = end_performance_tracking()
    print(f' saved in {time}s')

    # Display the results
    print(f"\tbitcount returned {result.result_count:,}")
    print(f"\tbitcount simulated runtime was {runtime.runtime:,}ns")


perform_study("_zero_count", [0]*2343750)
perform_study("_full_count", [1]*2343750)
perform_study("_fifty_percent_count", [int(random.randint(1, 100) <= 50) for _ in range(2343750)])
