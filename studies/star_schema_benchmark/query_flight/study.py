from studies.star_schema_benchmark.query_flight.query_flight import QueryFlights, BestQueryFlights, TestQueryFlights


def baseline_sweep():
    return QueryFlights.run_all()


def _best_run_all(blimp_frequency: 200000000, blimp_parallelism: 512):
    return TestQueryFlights.run_all(
        hardware_json={
            "bank_size_bytes": 33554432,
            "row_buffer_size_bytes": 1024,
            "time_to_row_activate_ns": 21,
            "time_to_column_activate_ns": 15.0,
            "time_to_precharge_ns": 21,
            "time_to_bank_communicate_ns": 100,
            "cpu_frequency": 2200000000,
            "cpu_cache_block_size_bytes": 64,
            "number_of_vALUs": 32,
            "number_of_vFPUs": 0,
            "blimpv_sew_max_bytes": 8,
            "blimpv_sew_min_bytes": 1,
            "blimp_frequency": blimp_frequency,
            "time_to_v0_transfer_ns": 5,
            "blimp_processor_bit_architecture": 64,
            "ambit_compute_register_rows": 6,
            "ambit_dcc_rows": 2,
            "blimp_extension_popcount": True,
            "blimpv_extension_vpopcount": True,
        },
        parallelism_factor=blimp_parallelism
    )


def best_baseline():
    return _best_run_all(blimp_frequency=200000000, blimp_parallelism=512)


def best_400mhz_512par():
    return _best_run_all(blimp_frequency=400000000, blimp_parallelism=512)


def best_200mhz_512par():
    return _best_run_all(blimp_frequency=200000000, blimp_parallelism=1024)


def best_400mhz_1024par():
    return _best_run_all(blimp_frequency=400000000, blimp_parallelism=1024)


def best_200mhz_256par():
    return _best_run_all(blimp_frequency=200000000, blimp_parallelism=256)


def best_400mhz_256par():
    return _best_run_all(blimp_frequency=400000000, blimp_parallelism=256)


# uncomment the study to be performed
baseline_sweep()
# best_baseline()()
# best_400mhz_512par()
# best_200mhz_512par()
# best_400mhz_1024par()
# best_200mhz_256par()
# best_400mhz_256par()
