import logging

from src.configurations.bank import BankLayoutConfiguration
from src.hardware.bank import Bank
from src.generators.records import DatabaseRecordGenerator


class SimulatedBank:
    """Defines base simulation parameters for a generic DRAM Bank"""
    def __init__(
            self,
            layout_configuration: BankLayoutConfiguration,
            bank_hardware: Bank,
            logger=None
            ):
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.configuration: layout_configuration
        self.bank_hardware: bank_hardware

    def layout(self, record_set: DatabaseRecordGenerator):
        """
        @implementable
        Given a record generator, perform data layout in this bank
        """
        raise NotImplemented("This bank has no implementation for data layout")
