from typing import Tuple

from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult, RuntimeResult, MemoryArrayResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.filter.bt.blimp import BlimpHitmapBetween
from src.queries.filter.bt.blimpv import BlimpVHitmapBetween
from src.queries.filter.lt.blimp import BlimpHitmapLessThan
from src.queries.filter.lt.blimpv import BlimpVHitmapLessThan
from src.queries.join.hitmap.early_pruning import BlimpVHashmapEarlyPruningJoin, BlimpHashmapEarlyPruningJoin


from studies.star_schema_benchmark.ssb import SSBDateTable, SSBLineOrderTable
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.q1_x import SSBQuery1pX, SSBQuery1pXQuantityDiscountDate


class SSBQuery1p2(SSBQuery1pX):

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return record.year_month_num == 199401

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        date_fks = set()
        for idx, record in enumerate(SSBDateTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.year_month_num == 199401:
                date_fks.add(record.date_key)

        join_fks = set()
        limit = GenericLineOrderColumn.scale(self.scale_factor) // self.parallelism_factor
        for index, record in enumerate(SSBLineOrderTable(scale_factor=self.scale_factor, no_storage=True).records):
            if index >= limit:
                break

            if (26 <= record.quantity <= 35) and (4 <= record.discount <= 6) and (record.order_date_key in date_fks):
                join_fks.add(index)

        assert set(final_hitmap_result.result_record_indexes) == join_fks


class SSBQuery1p2QuantityDiscountDate(SSBQuery1p2, SSBQuery1pXQuantityDiscountDate):
    def _get_operation_1_args(self):
        return {
            "value_low": 26,
            "value_high": 35,
        }

    def _get_operation_2_args(self):
        return {
            "value_low": 4,
            "value_high": 6,
        }

    def _perform_emit_2_layout(self, *args):
        self._get_discount_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_2_placement(self, final_hitmap, *args):
        self._get_discount_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_2_query_class(self._get_simulator(), self._get_discount_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=self._get_discount_layout_configuration().row_mapping.blimp_temp_region[0],
            hitmap_index=0,
            return_labels=False
        )

        if save_query_output:
            kernel_memory_array.save('discount_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_2_runtime.res')

        return kernel_runtime, kernel_memory_array


class SSBQuery1p2BlimpVQuantityDiscountDate(SSBQuery1p2QuantityDiscountDate):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    operation_1_query_class = BlimpVHitmapBetween
    operation_2_query_class = BlimpVHitmapBetween
    operation_3_query_class = BlimpVHashmapEarlyPruningJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmit


class SSBQuery1p2BlimpQuantityDiscountDate(SSBQuery1p2QuantityDiscountDate):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    operation_1_query_class = BlimpHitmapBetween
    operation_2_query_class = BlimpHitmapBetween
    operation_3_query_class = BlimpHashmapEarlyPruningJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmit
