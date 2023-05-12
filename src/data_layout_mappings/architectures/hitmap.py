import math

from pydantic import Field

from src.data_layout_mappings.methods import place_hitmap
from src.simulators.result import HitmapResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.data_layout_mappings import RowMapping, RowMappingSet, LayoutMetadata
from src.hardware import Bank


class GenericHitmapBankLayoutConfiguration(DataLayoutConfiguration):
    def reset_hitmaps_to_value(self, bank: Bank, value: bool):
        for hitmap in range(self._database_configuration.hitmap_count):
            self.reset_hitmap_index_to_value(bank, value, hitmap)

    def reset_hitmap_index_to_value(self, bank: Bank, value: bool, index: int):
        rows_per_hitmap = self._layout_metadata.total_rows_for_hitmaps // self._database_configuration.hitmap_count
        place_hitmap(
            self._row_mapping_set.hitmaps[0] + index * rows_per_hitmap,
            rows_per_hitmap,
            bank,
            value,
            self._layout_metadata.total_records_processable
        )

    def load_hitmap_result(self, bank: Bank, result: HitmapResult, index: int):
        rb_bits = self.hardware_configuration.row_buffer_size_bytes * 8
        rows_per_hitmap = self._layout_metadata.total_rows_for_hitmaps // self.database_configuration.hitmap_count
        result_index = 0

        for hitmap_index in range(math.ceil(result.max_bits / rb_bits)):
            stop_value = rb_bits * (hitmap_index + 1)
            hitmap_value = 0

            while result_index < len(result.result_record_indexes) and \
                    result.result_record_indexes[result_index] < stop_value:
                hitmap_value += 1 << ((rb_bits - 1) - result.result_record_indexes[result_index] % rb_bits)
                result_index += 1

            bank.set_raw_row(
                row_index=self._row_mapping_set.hitmaps[0] + index * rows_per_hitmap + hitmap_index,
                value=hitmap_value
            )


class GenericHitmapRowMapping(RowMappingSet):
    """Standard row mappings but with space for hitmaps"""
    hitmaps: RowMapping = Field(
        description="The start row address for ambit hitmaps and the number of rows this region contains")


class GenericHitmapLayoutMetadata(LayoutMetadata):
    """Metadata for a standard layouts but with hitmaps"""
    total_rows_for_hitmaps: int = Field(
        description="The total number of rows reserved for hitmaps")
