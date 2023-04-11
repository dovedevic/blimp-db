from src.configurations.hardware.ambit import AmbitHardwareConfiguration
from src.configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration


class BlimpPlusAmbitHardwareConfiguration(BlimpHardwareConfiguration, AmbitHardwareConfiguration):
    """Defines unchanging AMBIT system configurations sitting on top of an existing BLIMP system configuration"""
    pass


class BlimpVectorPlusAmbitHardwareConfiguration(BlimpVectorHardwareConfiguration, AmbitHardwareConfiguration):
    """Defines unchanging AMBIT system configurations sitting on top of an existing BLIMP-V system configuration"""
    pass
