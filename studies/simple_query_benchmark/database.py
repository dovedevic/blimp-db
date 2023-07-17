import random

from src.generators import DatabaseRecordGenerator, DataGenerator
from src.generators.data_generators import NullDataGenerator


class SQBDatabase:
    def __init__(
            self,
            table_a_size: int,
            table_b_size: int,
            shuffle_indices: bool=False,
            seed: int=0,
            index_size_bytes: int=4
    ):
        random.seed(seed)  # Initialize the random generator with a seed to ensure reproducibility.
        self._a = [
            (i, random.randint(0, table_b_size - 1), random.randint(0, 9), random.randint(0, 99))
            for i in range(table_a_size)
        ]
        self._b = [
            (i, random.randint(0, 9), random.randint(0, 99))
            for i in range(table_b_size)
        ]
        self.index_size_bytes = index_size_bytes

        if shuffle_indices:
            random.shuffle(self._a)
            random.shuffle(self._b)

        class SQBColumnDataGenerator(DataGenerator):
            def __init__(self, data_link, data_index, index_size):
                self.data_link = data_link
                self.data_index = data_index
                super().__init__(index_size)

            def _generate(self):
                return self.data_link[self.items_generated][self.data_index]

        class SQBDatabaseRecordGenerator(DatabaseRecordGenerator):
            def __init__(self, data_link, data_index, index_size, total_records):
                super().__init__(
                    pi_generator=SQBColumnDataGenerator(data_link, data_index, index_size),
                    data_generator=NullDataGenerator(),
                    total_records=total_records
                )

        self.a_k_generator = SQBDatabaseRecordGenerator(self._a, 0, index_size_bytes, len(self._a))
        self.a_10_generator = SQBDatabaseRecordGenerator(self._a, 2, index_size_bytes, len(self._a))
        self.a_100_generator = SQBDatabaseRecordGenerator(self._a, 3, index_size_bytes, len(self._a))

    @property
    def a(self):
        return self._a

    @property
    def b(self):
        return self._b
