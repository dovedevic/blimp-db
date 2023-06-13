from typing import Tuple, List

from src.configurations.hardware import HardwareConfiguration
from src.hardware import Bank
from src.simulators.hardware import SimulatedBank
from src.simulators.result import HitmapResult, RuntimeResult


class GenericSSBQuery:
    hardware_json = {
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
        "blimp_frequency": 400000000,
        "time_to_v0_transfer_ns": 5,
        "blimp_processor_bit_architecture": 64,
        "ambit_compute_register_rows": 6,
        "ambit_dcc_rows": 2,
        "blimp_extension_popcount": True,
        "blimpv_extension_vpopcount": True,
    }
    hardware_configuration_class = HardwareConfiguration
    bank_object_class = Bank
    bank_default_byte = 0x00
    simulator_class = SimulatedBank
    scale_factor = 100
    parallelism_factor = 512

    __hardware_config = None

    def _get_hardware_config(self):
        if self.__hardware_config is None:
            self.__hardware_config = self.hardware_configuration_class(**self.hardware_json)
        return self.__hardware_config

    __bank_object = None

    def _get_bank_object(self):
        if self.__bank_object is None:
            self.__bank_object = self.bank_object_class(
                configuration=self._get_hardware_config(),
                default_byte_value=self.bank_default_byte
            )
        return self.__bank_object

    __simulator = None

    def _get_simulator(self):
        if self.__simulator is None:
            self.__simulator = self.simulator_class(
                bank_hardware=self._get_bank_object()
            )
        return self.__simulator

    def _setup(self, **kwargs):
        pass

    def _perform_query(self, **kwargs) -> Tuple[HitmapResult, List[RuntimeResult]]:
        pass

    def _validate(self, *args):
        pass

    def run_query(self, **kwargs):
        print('Performing setup...')
        self._setup(**kwargs)
        print('Performing query...')
        results = self._perform_query(**kwargs)
        print('Validating...')
        self._validate(*results)
        return results
