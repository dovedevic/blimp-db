import math

from typing import Tuple

from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import RuntimeResult, MemoryArrayResult
from src.utils.generic import ceil_to_multiple
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.index import BlimpVHashmapIndexJoin, BlimpHashmapIndexJoin


from studies.simple_query_benchmark.queries.semijoin.common_semijoin import SQBCommonSemiJoin, \
    BlimpHashSet, BlimpVHashSet


class SQBSemiJoin(SQBCommonSemiJoin):

    def _record_semijoin_condition(self, record: tuple) -> bool:
        return record[2] < self._get_operation_1_args()["value"]

    def _calculate_semijoin_inout_size(self):
        return ceil_to_multiple(self.semijoin_hash_table.size, self._get_hardware_config().row_buffer_size_bytes) + \
            ceil_to_multiple(  # selection against a_100, add 1% buffer for expected value
            int(self.db_a_size // self.parallelism_factor * (
                        (self._get_operation_1_args()["value"] + 1) / 100)) * self.db_index_size_bytes,
            self._get_hardware_config().row_buffer_size_bytes
        )

    def _validate(self, final_memory_result: MemoryArrayResult, *args):
        semijoin_fks = set()
        for b_k, b_10, b_100 in self.db.b:
            if b_100 < self._get_operation_1_args()["value"]:
                semijoin_fks.add(b_k)

        indices = set()
        for a_k, a_b_k, a_10, a_100 in self.db.a:
            if a_b_k in semijoin_fks:
                indices.add(a_k)

        assert set(final_memory_result.result_array) == indices

    def _perform_operation_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False):
        kernel = self.operation_1_query_class(self._get_simulator(), self._get_semijoin_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            pi_element_size_bytes=self.db_index_size_bytes,
            hash_map=self.semijoin_hash_table,
            output_array_start_row=self._get_semijoin_layout_configuration().row_mapping.blimp_temp_region[0] +
            math.ceil(self.semijoin_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes),
            output_index_size_bytes=self.db_index_size_bytes,
            **self._get_operation_1_args()
        )

        if save_query_output:
            kernel_hitmap.save('semijoin.res')
        if save_runtime_output:
            kernel_runtime.save('semijoin_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_emit_1_layout(self, *args):
        pass

    def _perform_emit_1_placement(self, final_hitmap, *args):
        pass

    def _perform_emit_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return RuntimeResult(), MemoryArrayResult()


class SQBSemiJoin1(SQBSemiJoin):
    def _get_operation_1_args(self):
        return {
            "value": 1
        }


class SQBSemiJoin5(SQBSemiJoin):
    def _get_operation_1_args(self):
        return {
            "value": 5
        }


class SQBSemiJoin25(SQBSemiJoin):
    def _get_operation_1_args(self):
        return {
            "value": 25
        }


class SQBSemiJoinIndexBlimpV(SQBSemiJoin):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    operation_1_query_class = BlimpVHashmapIndexJoin
    emit_1_query_class = BlimpHitmapEmit


class SQBSemiJoinIndexBlimp(SQBSemiJoin):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    operation_1_query_class = BlimpHashmapIndexJoin
    emit_1_query_class = BlimpHitmapEmit


class SQBSemiJoinIndex1BlimpV(SQBSemiJoinIndexBlimpV, SQBSemiJoin1):
    semijoin_hash_table = BlimpVHashSet(2048, 4096)


class SQBSemiJoinIndex5BlimpV(SQBSemiJoinIndexBlimpV, SQBSemiJoin5):
    semijoin_hash_table = BlimpVHashSet(4096, 8192)


class SQBSemiJoinIndex25BlimpV(SQBSemiJoinIndexBlimpV, SQBSemiJoin25):
    semijoin_hash_table = BlimpVHashSet(16384, 32768)


class SQBSemiJoinIndex1Blimp(SQBSemiJoinIndexBlimp, SQBSemiJoin1):
    semijoin_hash_table = BlimpHashSet(32768, 65536)


class SQBSemiJoinIndex5Blimp(SQBSemiJoinIndexBlimp, SQBSemiJoin5):
    semijoin_hash_table = BlimpHashSet(131072, 262144)


class SQBSemiJoinIndex25Blimp(SQBSemiJoinIndexBlimp, SQBSemiJoin25):
    semijoin_hash_table = BlimpHashSet(524288, 1048576)
