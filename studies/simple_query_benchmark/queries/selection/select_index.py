from typing import Tuple

from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import RuntimeResult, MemoryArrayResult
from src.queries.emit.index.blimp import BlimpHitmapEmit
from src.utils.generic import ceil_to_multiple
from src.queries.filter_emit_index.lt.blimp import BlimpLessThanEmitIndex
from src.queries.filter_emit_index.lt.blimpv import BlimpVLessThanEmitIndex


from studies.simple_query_benchmark.queries.selection.common_filter import SQBCommonFilter


class SQBSelectIndexLT(SQBCommonFilter):

    def _validate(self, final_memory_result: MemoryArrayResult, *args):
        indices = set()
        for a_k, a_b_k, a_10, a_100 in self.db.a:
            if a_100 < self._get_operation_1_args()["value"]:
                indices.add(a_k)

        assert set(final_memory_result.result_array) == indices

    def _calculate_filter_inout_size(self):
        return ceil_to_multiple(  # selection against a_100, add 1% buffer for expected value
            int(self.db_a_size // self.parallelism_factor * (
                        (self._get_operation_1_args()["value"] + 1) / 100)) * self.db_index_size_bytes,
            self._get_hardware_config().row_buffer_size_bytes
        )

    def _perform_operation_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False):
        kernel = self.operation_1_query_class(self._get_simulator(), self._get_filter_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            pi_element_size_bytes=self.db_index_size_bytes,
            output_array_start_row=self._get_filter_layout_configuration().row_mapping.blimp_temp_region[0],
            output_index_size_bytes=self.db_index_size_bytes,
            **self._get_operation_1_args()
        )

        if save_query_output:
            kernel_hitmap.save('filter.res')
        if save_runtime_output:
            kernel_runtime.save('filter_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_emit_1_layout(self, *args):
        pass

    def _perform_emit_1_placement(self, final_hitmap, *args):
       pass

    def _perform_emit_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return RuntimeResult(), MemoryArrayResult()


class SQBSelectLT1(SQBSelectIndexLT):
    def _get_operation_1_args(self):
        return {
            "value": 1
        }


class SQBSelectLT5(SQBSelectIndexLT):
    def _get_operation_1_args(self):
        return {
            "value": 5
        }


class SQBSelectLT25(SQBSelectIndexLT):
    def _get_operation_1_args(self):
        return {
            "value": 25
        }


class SQBSelectIndexLTBlimpV(SQBSelectIndexLT):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    operation_1_query_class = BlimpVLessThanEmitIndex
    emit_1_query_class = BlimpHitmapEmit


class SQBSelectIndexLTBlimp(SQBSelectIndexLT):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    operation_1_query_class = BlimpLessThanEmitIndex
    emit_1_query_class = BlimpHitmapEmit


class SQBSelectIndexLT1BlimpV(SQBSelectIndexLTBlimpV, SQBSelectLT1):
    pass


class SQBSelectIndexLT5BlimpV(SQBSelectIndexLTBlimpV, SQBSelectLT5):
    pass


class SQBSelectIndexLT25BlimpV(SQBSelectIndexLTBlimpV, SQBSelectLT25):
    pass


class SQBSelectIndexLT1Blimp(SQBSelectIndexLTBlimp, SQBSelectLT1):
    pass


class SQBSelectIndexLT5Blimp(SQBSelectIndexLTBlimp, SQBSelectLT5):
    pass


class SQBSelectIndexLT25Blimp(SQBSelectIndexLTBlimp, SQBSelectLT25):
    pass
