from generators import DatabaseRecordGenerator
from generators.data_generators import NullDataGenerator, UniformRandomDataGenerator, IncrementalDataGenerator


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
