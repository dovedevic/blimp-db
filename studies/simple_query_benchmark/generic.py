import logging

from typing import Tuple, List

from src.configurations.hardware import HardwareConfiguration
from src.hardware import Bank
from src.simulators.hardware import SimulatedBank
from src.simulators.result import HitmapResult, RuntimeResult

from studies.simple_query_benchmark.database import SQBDatabase


class GenericSQBQuery:
    default_hardware_json = {
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
    default_parallelism_factor = 512
    bank_default_byte = 0x00
    default_db_a_size = 10 ** 9
    default_db_b_size = 10 ** 6
    default_db_index_size_bytes = 4

    hardware_configuration_class = HardwareConfiguration
    bank_object_class = Bank
    simulator_class = SimulatedBank
    runtime_class = RuntimeResult

    def __init__(self, logger=None, parallelism_factor=None, hardware_json=None, db_a_size=None, db_b_size=None, db_index_size_bytes=None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.__hardware_config = None
        self.__bank_object = None
        self.__simulator = None
        self.hardware_json = hardware_json or self.default_hardware_json
        self.parallelism_factor = parallelism_factor or self.default_parallelism_factor
        self.db_a_size = db_a_size or self.default_db_a_size
        self.db_b_size = db_b_size or self.default_db_b_size
        self.db_index_size_bytes = db_index_size_bytes or self.default_db_index_size_bytes

        self.db = SQBDatabase(self.db_a_size // self.parallelism_factor, self.db_b_size)

    def _get_hardware_config(self):
        if self.__hardware_config is None:
            self.__hardware_config = self.hardware_configuration_class(**self.hardware_json)
        return self.__hardware_config

    def _get_bank_object(self):
        if self.__bank_object is None:
            self.__bank_object = self.bank_object_class(
                configuration=self._get_hardware_config(),
                default_byte_value=self.bank_default_byte
            )
        return self.__bank_object

    def _get_simulator(self):
        if self.__simulator is None:
            self.__simulator = self.simulator_class(
                bank_hardware=self._get_bank_object(),
                runtime_class=self.runtime_class
            )
        return self.__simulator

    def _setup(self, **kwargs):
        pass

    def _perform_query(self, **kwargs) -> Tuple[HitmapResult, List[RuntimeResult]]:
        pass

    def _validate(self, *args):
        pass

    def run_query(self, **kwargs):
        self.logger.info('Performing setup...')
        self._setup(**kwargs)
        self.logger.info('Performing query...')
        results = self._perform_query(**kwargs)
        self.logger.info('Validating...')
        self._validate(*results)
        return results
