from generators import DataGenerator


class IncrementalDataGenerator(DataGenerator):
    """Generate data by counting up from 0 by one, keep data size in mind for bit overflows"""
    def _generate(self):
        return self.items_generated % (2**(self._data_size_in_bytes*8))
