from configurations.hardware import HardwareConfiguration


class BlimpHardwareConfiguration(HardwareConfiguration):
    """Defines unchanging BLIMP system configurations sitting on top of an existing system configuration"""
    # Intrinsic Hardware Values
    blimp_frequency: int
    time_to_v0_transfer_ns: float
    blimp_processor_bit_architecture: int

    # Calculated Fields
    time_per_blimp_cycle_ns: float = None

    def __init__(self, **data):
        super().__init__(**data)

        self.time_per_blimp_cycle_ns = 1 / self.blimp_frequency * 1000000000


class BlimpVectorHardwareConfiguration(BlimpHardwareConfiguration):
    """Defines unchanging BLIMP-V system configurations sitting on top of an existing BLIMP configuration"""
    # Intrinsic Hardware Values
    number_of_vALUs: int
    number_of_vFPUs: int
    blimpv_sew_max_bytes: int
    blimpv_sew_min_bytes: int

