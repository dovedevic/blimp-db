import json

from pydantic import BaseModel


class HardwareConfiguration(BaseModel):
    """Defines unchanging system configurations intrinsic to the hardware placed in a system at runtime"""
    # Intrinsic Hardware Values
    bank_size_bytes: int
    row_buffer_size_bytes: int
    time_to_row_activate_ns: float
    time_to_column_activate_ns: float
    time_to_precharge_ns: float

    # Calculated Fields
    bank_rows: int = None

    def __init__(self, **data):
        super().__init__(**data)
        self.bank_rows = self.bank_size_bytes // self.row_buffer_size_bytes

    def display(self):
        """Dump the configuration into the console for visual inspection"""
        print(json.dumps(self.dict(), indent=4))

    def save(self, path: str, compact=False):
        """
        Save the system configuration object as a JSON object

        @param path: The path and filename to save the configuration
        @param compact: Whether the JSON saved should be compact or indented
        """
        with open(path, "w") as fp:
            if compact:
                json.dump(self.dict(), fp)
            else:
                json.dump(self.dict(), fp, indent=4)

    @classmethod
    def load(cls, path: str):
        """Load a system configuration object"""
        with open(path, 'r') as fp:
            return cls(**json.load(fp))


class BlimpHardwareConfiguration(HardwareConfiguration):
    """Defines unchanging BLIMP system configurations sitting on top of an existing system configuration"""
    # Intrinsic Hardware Values
    blimp_frequency: int
    time_to_v0_transfer_ns: float
    processor_bit_architecture: int

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


class AmbitHardwareConfiguration(HardwareConfiguration):
    """Defines unchanging AMBIT system configurations with a standard CPU system configuration"""
    # Intrinsic Hardware Values
    ambit_temporary_register_rows: int
    ambit_dcc_rows: int

    # Calculated Fields
    time_for_TRA_MAJ_ns: float = None
    time_for_AAP_rowclone_ns: float = None

    # Static Values
    @property
    def ambit_control_group_rows(self):
        return 2

    def __init__(self, **data):
        super().__init__(**data)

        # RowClone: Fast and energy-efficient in-DRAM bulk data copy and initialization
        # https://dl.acm.org/doi/pdf/10.1145/2540708.2540725?casa_token=39dPO-EJBEMAAAAA:9JmRlo4pa1ImGYQwtKMr9nfsYgqlSFlIbobpIdCrPg68G8T4th_RIGiIdFFrSA4QUNLlIx_wYDfK
        # https://scholar.google.com/scholar?hl=en&as_sdt=0%2C39&q=RowClone%3A+Fast+and+Energy-Efficient+in-DRAM+Bulk+Data+Copy+and+Initialization&btnG=
        # Section 7.1 Figure 6
        self.time_for_AAP_rowclone_ns = self.time_to_row_activate_ns * 2 + self.time_to_precharge_ns

        # SIMDRAM: An End-to-End Framework for Bit-Serial SIMD Computing in DRAM
        # https://arxiv.org/pdf/2105.12839.pdf
        # https://scholar.google.com/scholar?hl=en&as_sdt=0%2C39&q=simdram&btnG=
        # Section 2.2.2
        self.time_for_TRA_MAJ_ns = self.time_to_row_activate_ns + self.time_to_precharge_ns


class BlimpPlusAmbitAmbitHardwareConfiguration(BlimpHardwareConfiguration, AmbitHardwareConfiguration):
    """Defines unchanging AMBIT system configurations sitting on top of an existing BLIMP system configuration"""
    pass


class BlimpVectorPlusAmbitAmbitHardwareConfiguration(BlimpVectorHardwareConfiguration, AmbitHardwareConfiguration):
    """Defines unchanging AMBIT system configurations sitting on top of an existing BLIMP-V system configuration"""
    pass
