import random
import logging
import typing


class DataGenerator:
    """Base Class for generic data generation"""
    def __init__(self, data_size_in_bytes: int, pregenerated_items: int=0, generatable: bool=True):
        self._data_size_in_bytes = data_size_in_bytes
        self._items_generated = pregenerated_items
        self._generatable = generatable

        if data_size_in_bytes <= 0:
            self._generate = self._null

    def generate_data(self) -> typing.Optional[int]:
        """
        @final no override
        public accessor for generating data; handles metadata updating
        """
        if not self._generatable:
            raise RuntimeError("This generator is not able to generate more records")

        data = self._generate()
        self._items_generated += 1
        return data

    @property
    def items_generated(self):
        """The amount of records generated in this instance"""
        return self._items_generated

    @property
    def data_size(self):
        """The length or amount of data per generation, in bytes"""
        return self._data_size_in_bytes

    @property
    def generatable(self):
        """The state of the generation production capability"""
        return self._generatable

    def set_generatable(self, state: bool):
        """Set the state of the generation production capability"""
        self._generatable = state

    def _generate(self) -> typing.Optional[int]:
        """
        @implementable
        Overridable method for generating data
        """
        return 0

    def _null(self):
        """Used to fulfill null-data generation"""
        return None


class UniformRandomDataGenerator(DataGenerator):
    """Generate data using a uniform random data stream"""
    def __init__(self, data_size_in_bytes: int, seed: int=None):
        super().__init__(data_size_in_bytes)
        self._seed = seed
        if seed:
            random.seed(self._seed)

    def _generate(self):
        return random.getrandbits(self._data_size_in_bytes * 8)


class IncrementalDataGenerator(DataGenerator):
    """Generate data by counting up from 0 by one, keep data size in mind for bit overflows"""
    def _generate(self):
        return self.items_generated % (2**(self._data_size_in_bytes*8))


class ConstantDataGenerator(DataGenerator):
    """Generate data using a constant value"""
    def __init__(self, data_size_in_bytes: int, constant: int):
        super().__init__(data_size_in_bytes)
        self._constant = constant

    def _generate(self):
        return self._constant


class DatabaseRecordGenerator:
    """
    Base Class for generating key/value or pi field/data records

    @param pi_generator: The data generator for generating keys or pi (primary/index) fields.
    @param data_generator: The data generator for generating values or data fields.
    @param total_records: The total number of records to generate. If set, no more than :ref:total_records will be
                            generated, and fetches outside this range result in an IndexError. If not set, no bound
                            on the number of records is placed. Fetches outside this range result in generation of up to
                            that many records to fulfill the request.
    @param records: A list of existing records to be used in conjunction with the generators supplied.
    """
    def __init__(
            self,
            pi_generator: DataGenerator,
            data_generator: DataGenerator,
            total_records=None, records=None
            ):
        self.logger = logging.getLogger(self.__class__.__name__)

        self._total_records = total_records
        self._pi_generator = pi_generator
        self._data_generator = data_generator

        self.records = records or []
        if total_records and total_records >= 1:  # if total_records is set, pre-generate records
            self.__fill_records_to(total_records)

    def _generate_record(self) -> tuple:
        """Generate a k/v or pi/data record"""
        return self._pi_generator.generate_data(), self._data_generator.generate_data()

    def _add_record(self, record_tuple: tuple):
        """Add and return a record tuple to our record set"""
        self.records.append(record_tuple)
        return record_tuple

    def __fill_records_to(self, length: int):
        """Keep generating records until a specified length"""
        while len(self.records) < length:
            self._add_record(self._generate_record())

    def get_record(self, index) -> tuple:
        """Fetch a record from the corpus given an index"""
        if index >= len(self.records) and self._total_records is None:
            self.__fill_records_to(index + 1)
        elif index >= len(self.records):
            raise IndexError(f"Attempted to fetch record {index} outside of pre-generated record limit {self._total_records}")
        return self.records[index]

    def get_records(self) -> tuple:
        """Generate a stream of records, if no record limit is set, continuously generate records"""
        for record in self.records:
            yield record

        if self._total_records is not None:
            return
        else:
            while True:
                yield self._add_record(self._generate_record())

    def _transform_to_raw(self, record: tuple):
        """Given a record tuple, convert it to a raw binary format string"""
        pi_field, data = record
        raw = ''
        if pi_field is not None:
            raw += format(pi_field, f'0{self._pi_generator.data_size * 8}b')[:self._pi_generator.data_size * 8]
        if data is not None:
            raw += format(data, f'0{self._data_generator.data_size * 8}b')[:self._data_generator.data_size * 8]
        return raw

    def get_raw_record(self, index) -> str:
        """Fetch a raw binary record from the corpus given an index"""
        return self._transform_to_raw(self.get_record(index))

    def get_raw_records(self) -> str:
        """Generate a stream of raw records, if no record limit is set, continuously generate and return raw records"""
        for record in self.get_records():
            yield self._transform_to_raw(record)

    def save(self, path: str):
        """Save the current state of the record set. When loading, no further generation is possible"""
        with open(path, 'w') as fp:
            fp.write(f"{self._pi_generator.data_size}\t{self._data_generator.data_size}\t{len(self.records)}\n")
            for pi, data in self.records:
                fp.write(f"{pi} {data}\n")

    @staticmethod
    def load(path: str):
        """Load an existing record set. No meaningful record generation is allowed"""
        with open(path, 'r') as fp:
            preamble = fp.readline()
            try:
                pi_size, data_size, record_length = preamble.split('\t')
                pi_size = int(pi_size)
                data_size = int(data_size)
                record_length = int(record_length)
            except ValueError:
                raise ValueError("File does not appear to be a record save state or is missing the file preamble")
            records = []
            try:
                for record in fp.readlines():
                    pi, data = record.strip().split(' ')
                    pi = int(pi) if pi != 'None' else None
                    data = int(data) if data != 'None' else None
                    records.append((pi, data))
            except ValueError:
                raise ValueError("File contains non-parseable records")
            try:
                assert len(records) == record_length
            except AssertionError:
                raise AssertionError("The number of records processed does not match the record preamble")
        return DatabaseRecordGenerator(
            DataGenerator(pi_size, record_length, False),
            DataGenerator(data_size, record_length, False),
            record_length,
            records
        )


class IncrementalKeyRandomDataRecordGenerator(DatabaseRecordGenerator):
    """
    Record generator where keys are generated consecutively and the data is generated randomly
    """
    def __init__(self, pi_record_size: int, total_record_size: int, total_records: int=None):
        pidg = IncrementalDataGenerator(pi_record_size)
        ddg = UniformRandomDataGenerator(total_record_size - pi_record_size)
        super().__init__(pidg, ddg, total_records)
