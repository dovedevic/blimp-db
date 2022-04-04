import logging


from src.generators.data import DataGenerator, IncrementalDataGenerator, UniformRandomDataGenerator


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

    def get_null_record(self) -> tuple:
        """Return a null record used for padding or compliance"""
        return 0, 0

    def get_pi_field(self, index) -> int:
        """Fetch just the primary/index field"""
        pi, _ = self.get_record(index)
        return pi

    def get_data_field(self, index) -> int:
        """Fetch just the data field"""
        _, data = self.get_record(index)
        return data

    def get_key_field(self, index) -> int:
        """Fetch just the key field, aliased to :func:get_pi_field"""
        return self.get_pi_field(index)

    def get_value_field(self, index) -> int:
        """Fetch just the value field, aliased to :func:get_data_field"""
        return self.get_data_field(index)

    def get_raw_record(self, index) -> int:
        """Fetch a raw record from the corpus given an index"""
        pi, data = self.get_record(index)
        return (pi << (self.data_size_bytes * 8)) | data

    def get_raw_records(self) -> int:
        """Generate a stream of raw records, if no record limit is set, continuously generate and return raw records"""
        for pi, data in self.get_records():
            yield (pi << (self.data_size_bytes * 8)) | data

    def get_raw_null_record(self) -> int:
        """Return a raw null record used for padding or compliance"""
        return 0

    @property
    def pi_size_bytes(self) -> int:
        """Return the primary index generator size in bytes"""
        return self._pi_generator.data_size

    @property
    def key_size_bytes(self) -> int:
        """Return the key generator size in bytes, aliased to :func:pi_size_bytes"""
        return self.pi_size_bytes

    @property
    def data_size_bytes(self) -> int:
        """Return the data generator size in bytes"""
        return self._data_generator.data_size

    @property
    def value_size_bytes(self) -> int:
        """Return the value generator size in bytes, aliased to :func:data_size_bytes"""
        return self.data_size_bytes

    @property
    def record_size_bytes(self) -> int:
        """Return the total size of a generated record in bytes"""
        return self.pi_size_bytes + self.data_size_bytes

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
