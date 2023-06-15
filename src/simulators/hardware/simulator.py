import logging

from typing import Generic, TypeVar

from src.hardware.bank import Bank
from src.simulators.result import RuntimeResult


BankHardware = TypeVar('BankHardware', bound=Bank)


class SimulatedBank(Generic[BankHardware]):
    """Defines base simulation parameters for a generic DRAM Bank"""
    def __init__(
            self,
            bank_hardware: BankHardware,
            logger=None,
            runtime_class=RuntimeResult
            ):
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self.bank_hardware = bank_hardware
        self.runtime_class = runtime_class

    def cpu_cycle(self, cycles=1, **runtime_kwargs) -> RuntimeResult:
        """Perform a specified number of CPU cycles"""
        if cycles <= 0:
            raise ValueError("argument 'cycles' cannot be less than one")
        if 'label' not in runtime_kwargs:
            runtime_kwargs['label'] = "cpu_cycle"
        return self.runtime_class(
            runtime=self.bank_hardware.hardware_configuration.time_per_cpu_cycle_ns,
            n=cycles,
            **runtime_kwargs
        )

    def cpu_fetch_cache_block(self, **runtime_kwargs) -> RuntimeResult:
        """Fetch a cache block from memory"""
        if 'label' not in runtime_kwargs:
            runtime_kwargs['label'] = "cpu_fetch_cache_block"
        return self.runtime_class(
            runtime=self.bank_hardware.hardware_configuration.time_to_row_activate_ns +
            self.bank_hardware.hardware_configuration.time_to_column_activate_ns +
            self.bank_hardware.hardware_configuration.time_to_precharge_ns,
            **runtime_kwargs
        )
