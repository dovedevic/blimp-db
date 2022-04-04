import typing


class RuntimeResult:
    """Defines a list of simulation steps, runtimes (in ns), and actions"""
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
        return self

    def save(self, path: str):
        """Save the runtime result"""
        with open(path, 'w') as fp:
            fp.write(f"runtime: {self.runtime}ns\n")
            fp.write(f"history: \n")
            for runtime, label in self.history:
                fp.write(f"\t{runtime}\t{label or ''}\n")


class SimulationResult:
    """Defines the return of the query. Returns the indexes of query-hit records"""
    def __init__(self, result_record_indexes: typing.List[int]=list):
        self.result_record_indexes = result_record_indexes
        self.result_count = len(self.result_record_indexes)

    @staticmethod
    def from_hitmap_byte_array(hitmap_byte_array: list, num_bits: int):
        """Given a byte array, fetch all indexes that have a 1-bit"""
        bitmaps_processed = 0
        bit_indexes = []

        # Iterate over all bytes
        for byte in hitmap_byte_array:
            # Have we exhausted all bitmaps in the hitmap?
            if bitmaps_processed >= num_bits:
                break

            # Can this entire byte be processed?
            if bitmaps_processed + 8 <= num_bits:
                for b in range(8):
                    if (1 << (7 - b)) & byte > 0:
                        bit_indexes.append(bitmaps_processed)
                    bitmaps_processed += 1
            # Only a partial bit of the byte is processable
            else:
                for b in range(8 - (num_bits - bitmaps_processed)):
                    if (1 << (7 - b)) & byte > 0:
                        bit_indexes.append(bitmaps_processed)
                    bitmaps_processed += 1
        return SimulationResult(bit_indexes)
