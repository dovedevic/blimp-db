from src.data_layout_mappings.methods import place_hitmap
from src.simulators.result import HitmapResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.data_layout_mappings import RowMapping, RowMappingSet, LayoutMetadata
from src.hardware import Bank
from pydantic import Field


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
        pass


class GenericHitmapRowMapping(RowMappingSet):
    """Standard row mappings but with space for hitmaps"""
    hitmaps: RowMapping = Field(
        description="The start row address for ambit hitmaps and the number of rows this region contains")


class GenericHitmapLayoutMetadata(LayoutMetadata):
    """Metadata for a standard layouts but with hitmaps"""
    total_rows_for_hitmaps: int = Field(
        description="The total number of rows reserved for hitmaps")
