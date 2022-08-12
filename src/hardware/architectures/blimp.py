from hardware import Bank
from configurations.hardware.blimp import BlimpHardwareConfiguration, BlimpVectorHardwareConfiguration


class BlimpBank(Bank):
    """Defines bank operations for a BLIMP DRAM Bank"""
    def __init__(self,
                 configuration: BlimpHardwareConfiguration,
                 memory: list=None,
                 default_byte_value: int=0xff):
        super(BlimpBank, self).__init__(configuration, memory, default_byte_value)


class BlimpVectorBank(BlimpBank):
    """Defines bank operations for a BLIMP-V DRAM Bank"""
    def __init__(self,
                 configuration: BlimpVectorHardwareConfiguration,
                 memory: list=None,
                 default_byte_value: int=0xff):
        super(BlimpVectorBank, self).__init__(configuration, memory, default_byte_value)
