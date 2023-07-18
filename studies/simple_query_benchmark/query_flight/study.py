from studies.simple_query_benchmark.query_flight.query_flight import QueryFlights


def baseline_sweep():
    return QueryFlights.run_all()


# uncomment the study to be performed
baseline_sweep()
