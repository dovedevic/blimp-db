import logging
import os

from src.configurations.bank_layout import AmbitBankLayoutConfiguration
from src.configurations.database import AmbitDatabaseConfiguration
from src.configurations.hardware import AmbitHardwareConfiguration

logging.basicConfig(level=logging.INFO)

study_name = input("Enter a directory-friendly study name: ")
study_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), study_name)
overwriting = False

if os.path.exists(study_dir):
    second_chance = input("Study path already exists. Do you want to overwrite this studies configurations? (yes|no): ")
    if second_chance != 'yes':
        print("not performing changes")
        exit()
    overwriting = True
else:
    os.mkdir(study_dir)

# EDIT CONFIGURATIONS HERE PRIOR TO STUDY RUN
print("Generating configuration file")
configuration = AmbitBankLayoutConfiguration(
    AmbitHardwareConfiguration(
        ambit_temporary_register_rows=4,
        ambit_dcc_rows=2,
        number_of_vALUs=32,
        number_of_vFPUs=0,
        blimpv_sew_max_bytes=8,
        blimpv_sew_min_bytes=1,
        time_to_v0_transfer_ns=5,
        blimp_frequency=200000000,
        processor_bit_architecture=64,
        time_to_precharge_ns=14.06,
        time_to_column_activate_ns=15.0,
        time_to_row_activate_ns=33.0,
        row_buffer_size_bytes=16384,
        bank_size_bytes=33554432,
    ),
    AmbitDatabaseConfiguration(
        ambit_temporary_bits=0,
        blimp_code_region_size_bytes=2048,
        hitmap_count=1,
        total_index_size_bytes=8,
        total_record_size_bytes=512,
    )
)

configuration.display()

configuration_directory = os.path.join(study_dir, "configuration.json")
if not overwriting or input(f"Overwrite {configuration_directory}? (yes|no)? ") == "yes":
    print("Saving configuration file")
    configuration.save(configuration_directory)

print(f"Done.")
