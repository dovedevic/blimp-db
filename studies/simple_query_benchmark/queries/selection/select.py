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


from studies.simple_query_benchmark.queries.selection.common_filter import SQBCommonFilter


class SQBSelectLT(SQBCommonFilter):

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        indices = set()
        for a_k, a_b_k, a_10, a_100 in self.db.a:
            if a_100 < self._get_operation_1_args()["value"]:
                indices.add(a_k)

        assert set(final_hitmap_result.result_record_indexes) == indices


class SQBSelectLT1(SQBSelectLT):
    def _get_operation_1_args(self):
        return {
            "value": 1
        }


class SQBSelectLT5(SQBSelectLT):
    def _get_operation_1_args(self):
        return {
            "value": 5
        }


class SQBSelectLT25(SQBSelectLT):
    def _get_operation_1_args(self):
        return {
            "value": 25
        }


class SQBSelectLTBlimpV(SQBSelectLT):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    operation_1_query_class = BlimpVHitmapLessThan
    emit_1_query_class = BlimpHitmapEmit


class SQBSelectLTBlimp(SQBSelectLT):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    operation_1_query_class = BlimpHitmapLessThan
    emit_1_query_class = BlimpHitmapEmit


class SQBSelectLT1BlimpV(SQBSelectLTBlimpV, SQBSelectLT1):
    pass


class SQBSelectLT5BlimpV(SQBSelectLTBlimpV, SQBSelectLT5):
    pass


class SQBSelectLT25BlimpV(SQBSelectLTBlimpV, SQBSelectLT25):
    pass


class SQBSelectLT1Blimp(SQBSelectLTBlimp, SQBSelectLT1):
    pass


class SQBSelectLT5Blimp(SQBSelectLTBlimp, SQBSelectLT5):
    pass


class SQBSelectLT25Blimp(SQBSelectLTBlimp, SQBSelectLT25):
    pass
