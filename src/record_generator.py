import random
import logging


class BaseRecordGenerator:
    def __init__(self, record_size, record_pi_size, total_corpus_size, seed=None, records=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        if seed:
            random.seed(seed)

        self.logger.info("creating corpus with prebuilt records..." if records else "creating corpus with generated records")

        self._seed = seed
        self.record_size = record_size
        self.record_pi_size = record_pi_size
        self.record_data_size = record_size - record_pi_size
        self.total_corpus_size = total_corpus_size

        self.records = records or []
        if not records:
            for i in range(0, total_corpus_size // record_size):
                self.records.append(self.generate_record(record_pi_size, record_size, i))

        self.logger.info(f"corpus stats -- record_size: {self.record_size} record_pi_size: {self.record_pi_size} record_data_size: {self.record_data_size} corpus_size: {self.total_corpus_size} num_records: {len(self.records)}")

    @staticmethod
    def generate_record(record_pi_size, record_size, record_index) -> tuple:
        pi_field = record_index
        remainder_length = record_size - record_pi_size
        remainder = random.getrandbits(remainder_length * 8)
        return pi_field, remainder

    def get_raw_record(self, index) -> str:
        pi_field, data = self.get_record(index)
        return format(pi_field, f'0{self.record_pi_size * 8}b')[:self.record_pi_size * 8] + \
            format(data, f'0{self.record_data_size * 8}b')[:self.record_data_size * 8]

    def get_raw_records(self) -> str:
        for record in self.records:
            pi_field, data = record
            yield format(pi_field, f'0{self.record_pi_size * 8}b')[:self.record_pi_size * 8] + \
                format(data, f'0{self.record_data_size * 8}b')[:self.record_data_size * 8]

    def get_record(self, index) -> tuple:
        return self.records[index]

    def get_records(self) -> tuple:
        for record in self.records:
            yield record

    def save(self, name, prefix='records'):
        self.logger.info(f"saving record corpus configuration as: " + f"{prefix}/{name}.{self.record_size}.{self.record_pi_size}.{self.total_corpus_size}.records")
        with open(f"{prefix}/{name}.{self.record_size}.{self.record_pi_size}.{self.total_corpus_size}.records", "w") as fp:
            for pi, data in self.records:
                fp.write(str(pi) + ' ' + str(data) + '\n')

    @staticmethod
    def load(name, prefix='records'):
        with open(f"{prefix}/{name}", "r") as fp:
            records = []
            _, record_size, pi_size, corpus_size, _ = name.split('.')
            for row in fp.readlines():
                pi, data = row.split(' ')
                pi = int(pi)
                data = int(data)
                records.append((pi, data))
        return BaseRecordGenerator(int(record_size), int(pi_size), int(corpus_size), records=records)
