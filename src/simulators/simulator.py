import logging

from src.configurations.bank_layout import BankLayoutConfiguration
from src.hardware.bank import Bank
from src.generators.records import DatabaseRecordGenerator


class SimulatedBank:
    """Defines base simulation parameters for a generic DRAM Bank"""
    def __init__(
            self,
            layout_configuration,
            bank_hardware,
            logger=None
            ):
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self.configuration = layout_configuration
        self.bank_hardware = bank_hardware

    def layout(self, record_set: DatabaseRecordGenerator, **kwargs):
        """
        @implementable
        Given a record generator, perform data layout in this bank
        """
        raise NotImplemented("This bank has no implementation for data layout")
