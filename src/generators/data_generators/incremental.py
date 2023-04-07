from src.generators import DataGenerator


class IncrementalDataGenerator(DataGenerator):
    """Generate data by counting up from the bias (default 0) by one, keep data size in mind for bit overflows"""
    def __init__(self, data_size_in_bytes: int, bias: int=0):
        super().__init__(data_size_in_bytes)

        self._bias = bias
        self._rotation = 2**(self._data_size_in_bytes*8)

        assert bias >= 0, "Bias bounds are bounded by positive integers"
        assert bias < self._rotation, "Starting bias is larger than the data size available"

    def _generate(self):
        return (self._bias + self.items_generated) % self._rotation
