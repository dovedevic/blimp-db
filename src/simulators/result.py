class RuntimeResult:
    """Defines a list of simulation steps, runtimes, and actions"""
    def __init__(self, runtime: float=0, label: str=None):
        self.history = list() if runtime == 0 and label is None else [(runtime, label)]
        self.runtime = runtime

    def __add__(self, other):
        if isinstance(other, self.__class__):
            combined_result_set = RuntimeResult()
            combined_result_set.history = self.history + other.history
            combined_result_set.runtime = self.runtime + other.runtime
            return combined_result_set
        raise NotImplemented()

    def step(self, runtime: int, label: str=None):
        """Perform a simulation step, similar to adding two results"""
        self.history.append((runtime, label))
        self.runtime += runtime

    def save(self, path: str):
        """Save the runtime result"""
        with open(path, 'w') as fp:
            fp.write(f"runtime: {self.runtime}ns\n")
            fp.write(f"history: \n")
            for runtime, label in self.history:
                fp.write(f"\t{runtime}\t{label or ''}\n")


class SimulationResult:
    """Defines the return of the query. Returns the indexes of query-hit records"""
    def __init__(self, result_record_indexes: [int]=list):
        self.result_record_indexes = result_record_indexes
        self.result_count = len(self.result_record_indexes)
