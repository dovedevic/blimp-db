import math
import json
import logging

from pydantic import BaseModel


class SystemConfiguration(BaseModel):
    bank_size: int
    row_buffer_size: int
    ambit_control_rows: int
    hitmap_count: int
    total_index_size: int
    record_to_rb_ratio: float
    time_to_row_activate: float
    time_to_column_activate: float
    time_to_precharge: float
    blimp_frequency: int

    # Calculated Fields
    total_available_rows: int = None
    total_available_rows_for_ambit: int = None
    total_available_rows_for_data: int = None
    total_available_rows_for_hitmap: int = None
    time_for_TRA_MAJ: float = None
    time_for_AAP_rowclone: float = None
    time_per_blimp_cycle: float = None

    # Check
    calculated: bool = False

    def calculate_rows(self, overwrite=False):
        logger = logging.getLogger(self.__class__.__name__)

        if self.calculated and not overwrite:
            logger.info("configuration was precalculated and is ready")
            return self

        if overwrite:
            logger.info("configuration is forcing new calculations...")
        self.calculated = True

        self.total_available_rows = self.bank_size // self.row_buffer_size - self.ambit_control_rows

        self.total_available_rows_for_data = int(math.ceil(
            max(self.total_available_rows - (8 * self.total_index_size + self.hitmap_count), 0) /
            (1 +
                (
                     (8 * self.total_index_size + self.hitmap_count) /
                     (8 * self.row_buffer_size * self.record_to_rb_ratio)  # bits per record
                )
            )
        ))

        self.total_available_rows_for_hitmap = int(
            self.hitmap_count *  # number of hit maps
            math.ceil(
                (self.total_available_rows_for_data / self.record_to_rb_ratio)  # records in bank
                / (self.row_buffer_size * 8)  # bool bits available per row-buffer
            )
        )

        self.total_available_rows_for_ambit = int(
            8 * self.total_index_size *  # rows per index block
            math.ceil(
                self.total_available_rows_for_data / self.record_to_rb_ratio  # records in bank
                / (self.row_buffer_size * 8)  # column bits per row-buffer
            )
        )

        # RowClone: Fast and energy-efficient in-DRAM bulk data copy and initialization
        # https://dl.acm.org/doi/pdf/10.1145/2540708.2540725?casa_token=39dPO-EJBEMAAAAA:9JmRlo4pa1ImGYQwtKMr9nfsYgqlSFlIbobpIdCrPg68G8T4th_RIGiIdFFrSA4QUNLlIx_wYDfK
        # https://scholar.google.com/scholar?hl=en&as_sdt=0%2C39&q=RowClone%3A+Fast+and+Energy-Efficient+in-DRAM+Bulk+Data+Copy+and+Initialization&btnG=
        # Section 7.1 Figure 6
        self.time_for_AAP_rowclone = self.time_to_row_activate * 2 + self.time_to_precharge

        # SIMDRAM: An End-to-End Framework for Bit-Serial SIMD Computing in DRAM
        # https://arxiv.org/pdf/2105.12839.pdf
        # https://scholar.google.com/scholar?hl=en&as_sdt=0%2C39&q=simdram&btnG=
        # Section 2.2.2
        self.time_for_TRA_MAJ = self.time_to_row_activate + self.time_to_precharge

        self.time_per_blimp_cycle = 1 / self.blimp_frequency * 1000000000

        assert(self.total_available_rows >= self.total_available_rows_for_ambit + self.total_available_rows_for_data + self.total_available_rows_for_hitmap)

        logger.debug("calculated fields")
        return self

    @staticmethod
    def construct_manually(
            bank_size: int,
            row_buffer_size: int,
            ambit_control_rows: int,
            hitmap_count: int,
            total_index_size: int,
            record_to_rb_ratio: float,
            time_to_row_activate: float,
            time_to_column_activate: float,
            time_to_precharge: float,
            blimp_frequency: int,
            total_available_rows: int = None,
            total_available_rows_for_ambit: int = None,
            total_available_rows_for_data: int = None,
            total_available_rows_for_hitmap: int = None
    ):
        return SystemConfiguration(**{
            "bank_size": bank_size,
            "row_buffer_size": row_buffer_size,
            "ambit_control_rows": ambit_control_rows,
            "hitmap_count": hitmap_count,
            "total_index_size": total_index_size,
            "record_to_rb_ratio": record_to_rb_ratio,
            "time_to_row_activate": time_to_row_activate,
            "time_to_column_activate": time_to_column_activate,
            "time_to_precharge": time_to_precharge,
            "blimp_frequency": blimp_frequency,
            "total_available_rows": total_available_rows,
            "total_available_rows_for_ambit": total_available_rows_for_ambit,
            "total_available_rows_for_data": total_available_rows_for_data,
            "total_available_rows_for_hitmap": total_available_rows_for_hitmap,
        }).calculate_rows()

    @staticmethod
    def construct_32mb_default_ambit_bank(
            row_buffer_size: int,
            hitmap_count: int,
            total_index_size: int,
            record_to_rb_ratio: float
    ):
        return SystemConfiguration.construct_manually(
            2**25,
            row_buffer_size,
            18,
            hitmap_count,
            total_index_size,
            record_to_rb_ratio,
            33.0,
            15,
            14.06,
            200000000
        )

    def display(self):
        print(json.dumps(self.dict(), indent=4))

    def save(self, name, prefix='configurations'):
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f"saving system configuration as: " + f"{prefix}/{name}.cfg.json")
        with open(f"{prefix}/{name}.cfg.json", "w") as fp:
            json.dump(self.dict(), fp, indent=4)
