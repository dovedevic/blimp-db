from configurations.hardware.ambit import AmbitHardwareConfiguration
from configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration


class BlimpPlusAmbitAmbitHardwareConfiguration(BlimpHardwareConfiguration, AmbitHardwareConfiguration):
    """Defines unchanging AMBIT system configurations sitting on top of an existing BLIMP system configuration"""
    pass


class BlimpVectorPlusAmbitAmbitHardwareConfiguration(BlimpVectorHardwareConfiguration, AmbitHardwareConfiguration):
    """Defines unchanging AMBIT system configurations sitting on top of an existing BLIMP-V system configuration"""
    pass
