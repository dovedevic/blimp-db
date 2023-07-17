from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.hitmap_payload import BlimpVHashmapHitmapPayloadJoin, BlimpHashmapHitmapPayloadJoin


from studies.simple_query_benchmark.queries.join.common_join import SQBCommonJoin, \
    BlimpHashMap, BlimpVHashMap


class SQBJoin(SQBCommonJoin):

    def _record_join_condition(self, record: tuple) -> bool:
        return record[2] < self._get_operation_1_args()["value"]

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        join_fks = set()
        for b_k, b_10, b_100 in self.db.b:
            if b_100 < self._get_operation_1_args()["value"]:
                join_fks.add(b_k)

        indices = set()
        for a_k, a_b_k, a_10, a_100 in self.db.a:
            if a_b_k in join_fks:
                indices.add(a_k)

        assert set(final_hitmap_result.result_record_indexes) == indices


class SQBJoin1(SQBJoin):
    def _get_operation_1_args(self):
        return {
            "value": 1
        }


class SQBJoin5(SQBJoin):
    def _get_operation_1_args(self):
        return {
            "value": 5
        }


class SQBJoin25(SQBJoin):
    def _get_operation_1_args(self):
        return {
            "value": 25
        }


class SQBJoinBlimpV(SQBJoin):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    operation_1_query_class = BlimpVHashmapHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit


class SQBJoinBlimp(SQBJoin):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    operation_1_query_class = BlimpHashmapHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit


class SQBJoin1BlimpV(SQBJoinBlimpV, SQBJoin1):
    join_hash_table = BlimpVHashMap(2048, 4096)


class SQBJoin5BlimpV(SQBJoinBlimpV, SQBJoin5):
    join_hash_table = BlimpVHashMap(8192, 16384)


class SQBJoin25BlimpV(SQBJoinBlimpV, SQBJoin25):
    join_hash_table = BlimpVHashMap(32768, 65536)


class SQBJoin1Blimp(SQBJoinBlimp, SQBJoin1):
    join_hash_table = BlimpHashMap(32768, 65536)


class SQBJoin5Blimp(SQBJoinBlimp, SQBJoin5):
    join_hash_table = BlimpHashMap(131072, 262144)


class SQBJoin25Blimp(SQBJoinBlimp, SQBJoin25):
    join_hash_table = BlimpHashMap(524288, 1048576)
