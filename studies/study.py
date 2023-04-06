from dataclasses import dataclass
from typing import Type, Optional

from src.hardware import Bank
from src.queries import Query, RuntimeResult
from src.configurations import HardwareConfiguration, DatabaseConfiguration
from src.data_layout_mappings import DataLayoutConfiguration
from src.simulators.hardware import SimulatedBank


@dataclass
class QueryStudy:
    # Static
    layout_configuration_type: Type[DataLayoutConfiguration]
    hardware_configuration_type: Type[HardwareConfiguration]
    database_configuration_type: Type[DatabaseConfiguration]
    hardware_type: Type[Bank]
    simulator_type: Type[SimulatedBank]
    query_type: Type[Query]
    name: Optional[str]

    # Post-simulation
    runtime: Optional[RuntimeResult] = None

    class Config:
        arbitrary_types_allowed = True
