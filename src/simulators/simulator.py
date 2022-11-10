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
            logger=None
            ):
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self.bank_hardware = bank_hardware

    def cpu_cycle(self, cycles=1, label="", return_labels=True) -> RuntimeResult:
        """Perform a specified number of CPU cycles"""
        if cycles <= 0:
            raise ValueError("argument 'cycles' cannot be less than one")
        runtime = RuntimeResult(
            self.bank_hardware.hardware_configuration.time_per_cpu_cycle_ns,
            label if return_labels else ""
        )
        for c in range(cycles - 1):
            runtime.step(self.bank_hardware.hardware_configuration.time_per_cpu_cycle_ns)
        return runtime
