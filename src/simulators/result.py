import typing

from src.utils import bitmanip


class RuntimeResult:
    """Defines a list of simulation steps, runtimes (in ns), and actions"""
    def __init__(self, runtime: float=0, label: str=None):
        self.history = list() if runtime == 0 and label is None else [(runtime, label)]
        self.runtime = runtime

    def __add__(self, other):
        if isinstance(other, self.__class__):
            self.runtime += other.runtime
            self.history += other.history
            del other
            return self
        raise NotImplemented()

    def step(self, runtime: float, label: str=None):
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


class HitmapResult:
    """Defines the result of a hitmap-returning query. Returns the indexes of query-hit records"""
    def __init__(self, result_record_indexes: typing.List[int], max_bits: int):
        self.result_record_indexes = result_record_indexes
        self.max_bits = max_bits
        self.result_count = len(self.result_record_indexes)

    def save(self, path: str):
        """Save the simulation result"""
        with open(path, 'w') as fp:
            fp.write(f"hits: {self.result_count}\n")
            fp.write(f"max_hits: {self.max_bits}\n")
            fp.write(f"indices: {self.result_record_indexes}\n")

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
        return HitmapResult(bit_indexes, num_bits)


class MemoryArrayResult:
    """Defines the result of a memory-array-returning query."""
    def __init__(self, result_array: typing.List[object]=list):
        self.result_array = result_array
        self.result_count = len(self.result_array)

    def save(self, path: str):
        """Save the array result"""
        with open(path, 'w') as fp:
            fp.write(f"hits: {self.result_count}\n")
            fp.write(f"array items: [\n")
            for array_item in self.result_array:
                fp.write(f"\t{str(array_item)},\n")
            fp.write("]")

    @staticmethod
    def from_byte_array(byte_array: list, element_width: int, cast_as: callable=int):
        """Given a byte array, fetch all array values and attempt to cast them"""
        assert len(byte_array) % element_width == 0, "there is not a integer multiple number of values in this array"
        values = [
            cast_as(
                bitmanip.byte_array_to_int(byte_array[n:n+element_width])
            ) for n in range(0, len(byte_array), element_width)
        ]
        return MemoryArrayResult(values)
