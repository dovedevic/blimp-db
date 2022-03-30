from simulators.result import RuntimeResult, SimulationResult
from simulators.simulator import SimulatedBank


class Query:
    """Base class for query operations"""
    def __init__(self, simulator: SimulatedBank):
        self.sim = simulator

    def perform_operation(self, **kwargs) -> (RuntimeResult, SimulationResult):
        """
        @implementable
        Overridable method for performing query operations on a simulated bank returning the resulting simulation result
        """
        return RuntimeResult(), SimulationResult()
