from src.configurations.hardware import HardwareConfiguration


class AmbitHardwareConfiguration(HardwareConfiguration):
    """Defines unchanging AMBIT system configurations with a standard CPU system configuration"""
    # Intrinsic Hardware Values
    ambit_compute_register_rows: int
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
