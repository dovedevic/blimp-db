import logging

from src.configuration import SystemConfiguration
from src.simulator import SimulatedBank

logging.basicConfig(level=logging.INFO)


config = SystemConfiguration.construct_32mb_default_ambit_bank(1024, 1, 4, 1)
# config = SystemConfiguration.construct_manually(
#     2**12,             # Bank size
#     8,                 # RB Size
#     6,                 # Ambit Rows
#     1,                 # Hitmap size
#     2,                 # Index Size
#     2.0,               # Record-to-RB ratio
#     33.0, 15, 14.06,   # Timing data
#     200000000          # BLIMP Frequency
# )

sb = SimulatedBank("test", config)
sb.checkpoint_bank("before")

r = sb.perform_not_equal_query(0, 4, 63)

sb.checkpoint_bank("after")


print(r.runtime)
print(r.history)
