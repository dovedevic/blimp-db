import random

from generators import DataGenerator


class UniformRandomDataGenerator(DataGenerator):
    """Generate data using a uniform random data stream"""
    def __init__(self, data_size_in_bytes: int, seed: int=None):
        super().__init__(data_size_in_bytes)
        self._seed = seed
        if seed:
            random.seed(self._seed)

    def _generate(self):
        return random.getrandbits(self._data_size_in_bytes * 8)
