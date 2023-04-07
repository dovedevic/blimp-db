from src.generators import DataGenerator


class ConstantDataGenerator(DataGenerator):
    """Generate data using a constant value"""
    def __init__(self, data_size_in_bytes: int, constant: int):
        super().__init__(data_size_in_bytes)

        assert constant >= 0, "Constants are bounded by positive integers"
        assert constant <= (2 ** (self._data_size_in_bytes * 8) - 1), "Constant cannot be larger than the data size available"

        self._constant = constant

    def _generate(self):
        return self._constant
