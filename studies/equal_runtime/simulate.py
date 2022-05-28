import logging
import os

from src.configurations.bank_layout import AmbitBankLayoutConfiguration
from src.generators.records import DatabaseRecordGenerator, ConstantKeyRandomDataRecordGenerator
from src.hardware.bank import AmbitBank
from src.simulators.ambit import SimulatedAmbitBank

from src.queries.ambit_equality import AmbitEqual, AmbitNotEqual
from src.queries.blimp_equality import BlimpEqual, BlimpNotEqual
from src.queries.blimpv_equality import BlimpVEqual, BlimpVNotEqual
from src.queries.blimpv_et_equality import BlimpVETEqual, BlimpVETNotEqual
from src.queries.ambit_et_equality import AmbitETEqual, AmbitETNotEqual

logging.basicConfig(level=logging.INFO)
study_name = input("Enter the study name: ")
study_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), study_name)

if not os.path.exists(study_dir):
    print("no study found; use the setup script to generate one first")
    exit()

print("Reading configuration file")
configuration_directory = os.path.join(study_dir, "configuration.json")
configuration = AmbitBankLayoutConfiguration.load(configuration_directory)
configuration.display()
print("Configuration loaded")

print("Looking for existing database files...")
database_directory = os.path.join(study_dir, "database.db")
if os.path.exists(database_directory):
    overwrite_database = input(
        f"The database already exists, do you want to generate a new one (yes|no): ")
    if overwrite_database not in ['yes', 'no']:
        print("no valid option provided")
        exit()
    overwrite_database = overwrite_database == 'yes'
else:
    overwrite_database = True

if overwrite_database:
    print(f"generating new database")
    database = ConstantKeyRandomDataRecordGenerator(
        configuration.database_configuration.total_index_size_bytes,
        configuration.database_configuration.total_record_size_bytes,
        0x0
    )
else:
    print(f"loading database")
    database = DatabaseRecordGenerator.load(database_directory)

print("Looking for an existing bank memory file...")
bank_directory = os.path.join(study_dir, "bank.memdump")
if os.path.exists(bank_directory):
    overwrite_memdump = input(
        f"A bank memdump for the database already exists, do you want to generate a new one (yes|no): ")
    if overwrite_memdump not in ['yes', 'no']:
        print("no valid option provided")
        exit()
    overwrite_memdump = overwrite_memdump == 'yes'
else:
    overwrite_memdump = True

if overwrite_memdump:
    print(f"generating bank layout")
    bank = AmbitBank(configuration.hardware_configuration)
else:
    print(f"loading bank memory")
    bank = AmbitBank.load(bank_directory)

print("Initializing simulator")
simulator = SimulatedAmbitBank(configuration, bank)
if overwrite_memdump or overwrite_database:
    print("performing bank layout")
    simulator.layout(database)
    if overwrite_database:
        print("saving database")
        database.save(os.path.join(study_dir, "database.db"))
    print("saving bank memory")
    bank.save(os.path.join(study_dir, "bank.memdump"))

print("Simulator loaded, starting queries")
query_map = {
    "ambit_equal": AmbitEqual,
    "ambit_not_equal": AmbitNotEqual,
    "blimp_equal": BlimpEqual,
    "blimp_not_equal": BlimpNotEqual,
    "blimp_v_equal": BlimpVEqual,
    "blimp_v_not_equal": BlimpVNotEqual,
    "blimp_v_et_equal": BlimpVETEqual,
    "blimp_v_et_not_equal": BlimpVETNotEqual,
    "ambit_et_equal": AmbitETEqual,
    "ambit_et_not_equal": AmbitETNotEqual,
}

save_memdumps = input("Save post query memdumps? (yes|no): ")
if save_memdumps not in ['yes', 'no']:
    print("no valid option provided")
    exit()
save_memdumps = save_memdumps == 'yes'

for query in query_map:
    for case in ["best_case", "worst_case"]:
        is_best_case = case == "best_case"

        print("Looking for existing case query bank memory files...")
        query_directory = os.path.join(study_dir, f"{case}_{query}.bank.memdump")
        if os.path.exists(query_directory):
            redo_query = input(
                f"A memdump for query {case}_{query} already exists, do you want to re-simulate it (yes|no): "
            )
            if redo_query not in ['yes', 'no']:
                print("no valid option provided, assuming no")
                redo_query = False
            else:
                redo_query = redo_query == 'yes'
        else:
            redo_query = True

        if redo_query:
            print(f"performing {case} {query}")
            runtime_result, simulation_result = query_map[query](simulator).perform_operation(
                pi_subindex_offset_bytes=0,
                pi_element_size_bytes=configuration.database_configuration.total_index_size_bytes,
                value=2 ** (configuration.database_configuration.total_index_size_bytes * 8) - 1 if is_best_case else 0,
                return_labels=True,
                hitmap_index=0
            )
            print(f"query simulated runtime was: {runtime_result.runtime}")
            print(f"query simulated hits was: {simulation_result.result_count}")

            if save_memdumps:
                print(f"saving query dump")
                simulator.bank_hardware.save(query_directory)

            print(f"saving runtime result")
            query_run_result_directory = os.path.join(study_dir, f"{case}_{query}.runtime.sim")
            runtime_result.save(query_run_result_directory)

            print(f"saving simulation result")
            query_sim_result_directory = os.path.join(study_dir, f"{case}_{query}.result.sim")
            simulation_result.save(query_sim_result_directory)

            print(f"cleaning up")
            simulator.reset_all_hitmaps()

        else:
            print(f"skipping {case} {query}")
print("done")
