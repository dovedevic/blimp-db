from src.generators import DatabaseRecordGenerator
from src.generators.data_generators import \
    NullDataGenerator, UniformRandomDataGenerator, IncrementalDataGenerator, BoundedUniformRandomDataGenerator


class RandomKeyNullDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where the keys are uniformly random and the data is null
    """
    def __init__(self, pi_record_size: int, total_records: int=None):
        pidg = UniformRandomDataGenerator(pi_record_size)
        ddg = NullDataGenerator()
        super().__init__(pidg, ddg, total_records)


class IncrementalKeyNullDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where the keys are incremented and the data is null
    """
    def __init__(self, pi_record_size: int, total_records: int=None):
        pidg = IncrementalDataGenerator(pi_record_size)
        ddg = NullDataGenerator()
        super().__init__(pidg, ddg, total_records)


class BoundedRandomKeyNullDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where the keys are uniformly bounded random and the data is null
    """
    def __init__(self, pi_record_size: int, min_bound: int, max_bound: int, total_records: int=None):
        pidg = BoundedUniformRandomDataGenerator(pi_record_size, min_bound, max_bound)
        ddg = NullDataGenerator()
        super().__init__(pidg, ddg, total_records)
