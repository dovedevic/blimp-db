from typing import Type

from src.generators import DatabaseRecordGenerator, DataGenerator
from src.generators.data_generators import NullDataGenerator

from studies.star_schema_benchmark.ssb import SSBTable
from studies.star_schema_benchmark.helpers import LinearScale

from studies.star_schema_benchmark.ssb import SSBLineOrderTable


class _SSBColumnGenerator(DatabaseRecordGenerator):
    column_size: int = 0
    ssb_table_class: Type[SSBTable] = SSBTable

    def __init__(self, scale_factor, total_records):
        super().__init__(
            pi_generator=DataGenerator(self.column_size, generatable=False),
            data_generator=NullDataGenerator()
        )
        self._total_records = total_records

        if total_records > 0:
            for idx, record in enumerate(self.ssb_table_class(scale_factor=scale_factor, no_storage=True).records):
                self.records.append((self._column(record), 0))
                if idx + 1 >= total_records:
                    break
            else:  # we ran out of records
                raise RuntimeError(f"the SSB part file for sf:{scale_factor} could not supply {total_records} columns")

    def _column(self, record):
        raise NotImplemented


class _LineOrderColumn(_SSBColumnGenerator, LinearScale):
    base_factor = 6_000_000
    ssb_table_class = SSBLineOrderTable


class LineOrderDiscount(_LineOrderColumn):
    column_size = 1

    def _column(self, record):
        return record.discount


class LineOrderQuantity(_LineOrderColumn):
    column_size = 1

    def _column(self, record):
        return record.quantity


class LineOrderOrderDate(_LineOrderColumn):
    column_size = 4

    def _column(self, record):
        return record.order_date_key


class LineOrderPartKey(_LineOrderColumn):
    column_size = 4

    def _column(self, record):
        return record.part_key


class LineOrderSupplyKey(_LineOrderColumn):
    column_size = 4

    def _column(self, record):
        return record.supplier_key
