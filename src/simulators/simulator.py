import logging

from src.hardware.bank import Bank


class SimulatedBank:
    """Defines base simulation parameters for a generic DRAM Bank"""
    def __init__(
            self,
            bank_hardware: Bank,
            logger=None
            ):
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self.bank_hardware = bank_hardware
