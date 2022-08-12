from generators import DataGenerator


class ConstantDataGenerator(DataGenerator):
    """Generate data using a constant value"""
    def __init__(self, data_size_in_bytes: int, constant: int):
        super().__init__(data_size_in_bytes)
        self._constant = constant

    def _generate(self):
        return self._constant
