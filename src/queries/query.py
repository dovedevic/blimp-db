from typing import Generic, TypeVar

from simulators.result import RuntimeResult, HitmapResult
from simulators.simulator import SimulatedBank
from data_layout_mappings import DataLayoutConfiguration


Simulator = TypeVar('Simulator', bound=SimulatedBank)
LayoutConfiguration = TypeVar('LayoutConfiguration', bound=DataLayoutConfiguration)


class Query(Generic[Simulator, LayoutConfiguration]):
    """Base class for query operations"""
    def __init__(self, simulator: Simulator, layout_configuration: LayoutConfiguration):
        self.simulator = simulator
        self.hardware = simulator.bank_hardware
        self.layout_configuration = layout_configuration

    def perform_operation(self, **kwargs) -> (RuntimeResult, HitmapResult):
        """
        @implementable
        Overridable method for performing query operations on a simulated bank returning the resulting simulation result
        """
        return RuntimeResult(), HitmapResult()
