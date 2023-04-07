from src.hardware import Bank
from src.hardware.architectures import BlimpBank, AmbitBank, BlimpVectorBank
from src.configurations.hardware.ambit import BlimpPlusAmbitHardwareConfiguration, \
    BlimpVectorPlusAmbitHardwareConfiguration


class BlimpAmbitBank(
    BlimpBank,
    AmbitBank,
    Bank[BlimpPlusAmbitHardwareConfiguration]
):
    """Defines bank operations for a BLIMP-orchestrated AMBIT DRAM Bank"""
    pass


class BlimpAmbitVectorBank(
    BlimpVectorBank,
    AmbitBank,
    Bank[BlimpVectorPlusAmbitHardwareConfiguration]
):
    """Defines bank operations for a BLIMP-V-orchestrated AMBIT DRAM Bank"""
    pass
