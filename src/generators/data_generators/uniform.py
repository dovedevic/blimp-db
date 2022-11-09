import random

from generators import DataGenerator


class BoundedUniformRandomDataGenerator(DataGenerator):
    """Generate data using a uniform random data stream"""
    def __init__(self, data_size_in_bytes: int, min_bound: int, max_bound: int, seed: int=None):
        super().__init__(data_size_in_bytes)
        self._seed = seed
        self._min_bound = min_bound
        self._max_bound = max_bound
        if seed:
            random.seed(self._seed)

        if data_size_in_bytes <= 0:  # no need to check when were going to be generating nulls anyways
            return

        assert min_bound < max_bound, "Minimum bound must be strictly less than the maximum bound"
        assert min_bound >= 0, "Minimum bounds are bounded by positive integers"
        assert max_bound >= 0, "Maximum bounds are bounded by positive integers"
        assert max_bound <= 2**(self._data_size_in_bytes*8) - 1, "Max bound is larger than the data size available"

    def _generate(self):
        return random.randint(self._min_bound, self._max_bound)


class UniformRandomDataGenerator(BoundedUniformRandomDataGenerator):
    """Generate data using a uniform random data stream"""
    def __init__(self, data_size_in_bytes: int, seed: int=None):
        super().__init__(data_size_in_bytes, 0, 2**(data_size_in_bytes*8) - 1, seed)
