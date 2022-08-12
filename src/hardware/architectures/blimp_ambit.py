from hardware.architectures import BlimpBank, AmbitBank, BlimpVectorBank
from configurations.hardware.ambit import BlimpPlusAmbitAmbitHardwareConfiguration, \
    BlimpVectorPlusAmbitAmbitHardwareConfiguration


class BlimpAmbitBank(BlimpBank, AmbitBank):
    """Defines bank operations for a BLIMP-orchestrated AMBIT DRAM Bank"""
    def __init__(self,
                 configuration: BlimpPlusAmbitAmbitHardwareConfiguration,
                 memory: list=None,
                 default_byte_value: int=0xff):
        super(BlimpAmbitBank, self).__init__(configuration, memory, default_byte_value)


class BlimpAmbitVectorBank(BlimpVectorBank, AmbitBank):
    """Defines bank operations for a BLIMP-V-orchestrated AMBIT DRAM Bank"""
    def __init__(self,
                 configuration: BlimpVectorPlusAmbitAmbitHardwareConfiguration,
                 memory: list=None,
                 default_byte_value: int=0xff):
        super(BlimpAmbitVectorBank, self).__init__(configuration, memory, default_byte_value)
