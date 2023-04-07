from src.generators import DatabaseRecordGenerator
from src.generators.data_generators import ConstantDataGenerator, UniformRandomDataGenerator, IncrementalDataGenerator


class ConstantKeyRandomDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where the keys are constant values and the data is generated randomly
    """
    def __init__(self, pi_record_size: int, total_record_size: int, constant: int, total_records: int=None):
        pidg = ConstantDataGenerator(pi_record_size, constant)
        ddg = UniformRandomDataGenerator(total_record_size - pi_record_size)
        super().__init__(pidg, ddg, total_records)


class IncrementalKeyRandomDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where keys are generated consecutively and the data is generated randomly
    """
    def __init__(self, pi_record_size: int, total_record_size: int, total_records: int=None):
        pidg = IncrementalDataGenerator(pi_record_size)
        ddg = UniformRandomDataGenerator(total_record_size - pi_record_size)
        super().__init__(pidg, ddg, total_records)
