from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.hitmap import BlimpVHashmapJoin, BlimpHashmapJoin


from studies.simple_query_benchmark.queries.semijoin.common_semijoin import SQBCommonSemiJoin, \
    BlimpHashSet, BlimpVHashSet


class SQBSemiJoin(SQBCommonSemiJoin):

    def _record_semijoin_condition(self, record: tuple) -> bool:
        return record[2] < self._get_operation_1_args()["value"]

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        semijoin_fks = set()
        for b_k, b_10, b_100 in self.db.b:
            if b_100 < self._get_operation_1_args()["value"]:
                semijoin_fks.add(b_k)

        indices = set()
        for a_k, a_b_k, a_10, a_100 in self.db.a:
            if a_b_k in semijoin_fks:
                indices.add(a_k)

        assert set(final_hitmap_result.result_record_indexes) == indices


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


class SQBSemiJoinBlimpV(SQBSemiJoin):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    operation_1_query_class = BlimpVHashmapJoin
    emit_1_query_class = BlimpHitmapEmit


class SQBSemiJoinBlimp(SQBSemiJoin):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    operation_1_query_class = BlimpHashmapJoin
    emit_1_query_class = BlimpHitmapEmit


class SQBSemiJoin1BlimpV(SQBSemiJoinBlimpV, SQBSemiJoin1):
    semijoin_hash_table = BlimpVHashSet(2048, 4096)


class SQBSemiJoin5BlimpV(SQBSemiJoinBlimpV, SQBSemiJoin5):
    semijoin_hash_table = BlimpVHashSet(4096, 8192)


class SQBSemiJoin25BlimpV(SQBSemiJoinBlimpV, SQBSemiJoin25):
    semijoin_hash_table = BlimpVHashSet(16384, 32768)


class SQBSemiJoin1Blimp(SQBSemiJoinBlimp, SQBSemiJoin1):
    semijoin_hash_table = BlimpHashSet(32768, 65536)


class SQBSemiJoin5Blimp(SQBSemiJoinBlimp, SQBSemiJoin5):
    semijoin_hash_table = BlimpHashSet(131072, 262144)


class SQBSemiJoin25Blimp(SQBSemiJoinBlimp, SQBSemiJoin25):
    semijoin_hash_table = BlimpHashSet(524288, 1048576)
