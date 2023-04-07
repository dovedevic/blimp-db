from src.hardware import Bank
from src.configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration


class BlimpBank(
    Bank[BlimpHardwareConfiguration]
):
    """Defines bank operations for a BLIMP DRAM Bank"""
    pass


class BlimpVectorBank(
    BlimpBank,
    Bank[BlimpVectorHardwareConfiguration]
):
    """Defines bank operations for a BLIMP-V DRAM Bank"""
    pass
