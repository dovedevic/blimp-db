import logging

from simulation.src.configuration import SystemConfiguration

logging.basicConfig(level=logging.INFO)

# Realistic Real-world Values
ROW_BUFFER_SIZES = [512, 1024, 2048, 4096, 8192, 16384]
HITMAP_SIZES = [1, 2, 3, 4, 5, 6, 7, 8]
PRIMARY_INDEX_SIZES = [1, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
TOTAL_RECORD_SIZES = [0.25, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0]


def main():
    # Let it churnnnnnnn
    for r_s in ROW_BUFFER_SIZES:  # Enumerate Row Buffer Sizes
        for h_c in HITMAP_SIZES:  # Enumerate Hitmap/Subquery Amounts
            for i_s in PRIMARY_INDEX_SIZES:  # Enumerate Primary Index Field Sizes
                for c_s in TOTAL_RECORD_SIZES:  # Enumerate Total Record Sizes
                    config = SystemConfiguration.construct_32mb_default_ambit_bank(
                        r_s,  # r_s : Row Buffer Size (B)
                        h_c,  # h_c : Hitmap Count (int)
                        i_s,  # i_s : Primary/Index Field Total Size (B)
                        c_s  # c_s : Record to Row-Buffer Size Ratio (float)
                    ).save(f"{r_s}-{h_c}-{i_s}-{c_s}", prefix='configurations/enumerations')


if __name__ == "__main__":
    main()
