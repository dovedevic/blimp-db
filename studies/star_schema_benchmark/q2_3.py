from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from queries.emit.index.blimp import BlimpHitmapEmit
from src.simulators.result import HitmapResult
from src.queries.join.hitmap import BlimpVHashmapJoin, BlimpHashmapJoin
from src.queries.join.hitmap.early_pruning import BlimpVHashmapEarlyPruningJoin, BlimpHashmapEarlyPruningJoin
from src.queries.join.hitmap_payload.early_pruning import BlimpVHashmapEarlyPruningHitmapPayloadJoin, BlimpHashmapEarlyPruningHitmapPayloadJoin
from src.queries.emit.hashmap_payload import BlimpHitmapEmitHashmapPayload, BlimpVHitmapEmitHashmapPayload


from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBPartTable, SSBDateTable, SSBLineOrderTable
from studies.star_schema_benchmark.ssb import SSBRegionEncoding, SSBBrandEncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.q2_x import SSBQuery2pX, SSBQuery2pXSupplierPartDate, SSBQuery2pXPartSupplierDate


class SSBQuery2p3(SSBQuery2pX):
    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.EUROPE

    def _part_record_join_condition(self, record: SSBPartTable.TableRecord) -> bool:
        return record.brand == SSBBrandEncoding.convert("MFGR#2221")

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return True

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        supplier_fks = set()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.region == SSBRegionEncoding.EUROPE:
                supplier_fks.add(record.supplier_key)

        part_fks = set()
        for idx, record in enumerate(SSBPartTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.brand == SSBBrandEncoding.convert("MFGR#2221"):
                part_fks.add(record.part_key)

        join_fks = set()
        limit = GenericLineOrderColumn.scale(self.scale_factor) // self.parallelism_factor
        for index, record in enumerate(SSBLineOrderTable(scale_factor=self.scale_factor, no_storage=True).records):
            if index >= limit:
                break

            if record.part_key in part_fks and record.supplier_key in supplier_fks:
                join_fks.add(index)

        assert set(final_hitmap_result.result_record_indexes) == join_fks


class SSBQuery2p3BlimpVSupplierPartDate(SSBQuery2pXSupplierPartDate, SSBQuery2p3):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit


class SSBQuery2p3BlimpSupplierPartDate(SSBQuery2pXSupplierPartDate, SSBQuery2p3):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit


class SSBQuery2p3BlimpVPartSupplierDate(SSBQuery2pXPartSupplierDate, SSBQuery2p3):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload


class SSBQuery2p3BlimpPartSupplierDate(SSBQuery2pXPartSupplierDate, SSBQuery2p3):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
