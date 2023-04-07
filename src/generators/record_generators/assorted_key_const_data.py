from src.generators import DatabaseRecordGenerator
from src.generators.data_generators import ConstantDataGenerator, UniformRandomDataGenerator, IncrementalDataGenerator


class RandomKeyConstantDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where the keys are uniformly random and the data is a constant
    """
    def __init__(self, pi_record_size: int, total_record_size: int, constant: int, total_records: int=None):
        pidg = UniformRandomDataGenerator(pi_record_size)
        cdg = ConstantDataGenerator(total_record_size - pi_record_size, constant)
        super().__init__(pidg, cdg, total_records)


class IncrementalKeyConstantDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where the keys are incremented and the data is a constant
    """
    def __init__(self, pi_record_size: int, total_record_size: int, constant: int, total_records: int=None):
        pidg = IncrementalDataGenerator(pi_record_size)
        cdg = ConstantDataGenerator(total_record_size - pi_record_size, constant)
        super().__init__(pidg, cdg, total_records)
