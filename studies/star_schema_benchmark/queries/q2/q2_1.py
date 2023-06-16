from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.hitmap import BlimpVHashmapJoin, BlimpHashmapJoin
from src.queries.join.hitmap.early_pruning import BlimpVHashmapEarlyPruningJoin, BlimpHashmapEarlyPruningJoin
from src.queries.join.hitmap_payload.early_pruning import BlimpVHashmapEarlyPruningHitmapPayloadJoin, \
    BlimpHashmapEarlyPruningHitmapPayloadJoin
from src.queries.emit.hashmap_payload import BlimpHitmapEmitHashmapPayload, BlimpVHitmapEmitHashmapPayload


from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBPartTable, SSBDateTable, SSBLineOrderTable
from studies.star_schema_benchmark.ssb import SSBRegionEncoding, SSBCategoryEncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.queries.q2.q2_x import SSBQuery2pX, SSBQuery2pXSupplierPartDate, \
    SSBQuery2pXPartSupplierDate
from studies.star_schema_benchmark.queries.q2.q2_x import BlimpSupplierHashSet, BlimpVSupplierHashSet, \
    BlimpPartHashMap, BlimpVPartHashMap, BlimpDateHashMap, BlimpVDateHashMap


class SSBQuery2p1(SSBQuery2pX):
    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.AMERICA

    def _part_record_join_condition(self, record: SSBPartTable.TableRecord) -> bool:
        return record.category == SSBCategoryEncoding.convert('MFGR#12')

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return True

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        supplier_fks = set()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.region == SSBRegionEncoding.AMERICA:
                supplier_fks.add(record.supplier_key)

        part_fks = set()
        for idx, record in enumerate(SSBPartTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.category == SSBCategoryEncoding.convert('MFGR#12'):
                part_fks.add(record.part_key)

        join_fks = set()
        limit = GenericLineOrderColumn.scale(self.scale_factor) // self.parallelism_factor
        for index, record in enumerate(SSBLineOrderTable(scale_factor=self.scale_factor, no_storage=True).records):
            if index >= limit:
                break

            if record.part_key in part_fks and record.supplier_key in supplier_fks:
                join_fks.add(index)

        assert set(final_hitmap_result.result_record_indexes) == join_fks


class SSBQuery2p1BlimpVSupplierPartDate(SSBQuery2pXSupplierPartDate, SSBQuery2p1):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    supplier_join_hash_table = BlimpVSupplierHashSet(2048, 4096)
    part_join_hash_table = BlimpVPartHashMap(8192, 16384)
    date_join_hash_table = BlimpVDateHashMap(256, 256)


class SSBQuery2p1BlimpSupplierPartDate(SSBQuery2pXSupplierPartDate, SSBQuery2p1):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    supplier_join_hash_table = BlimpSupplierHashSet(65536, 131072)
    part_join_hash_table = BlimpPartHashMap(262144, 524288)
    date_join_hash_table = BlimpDateHashMap(4096, 4096)


class SSBQuery2p1BlimpVPartSupplierDate(SSBQuery2pXPartSupplierDate, SSBQuery2p1):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload
    supplier_join_hash_table = BlimpVSupplierHashSet(2048, 4096)
    part_join_hash_table = BlimpVPartHashMap(8192, 16384)
    date_join_hash_table = BlimpVDateHashMap(256, 256)


class SSBQuery2p1BlimpPartSupplierDate(SSBQuery2pXPartSupplierDate, SSBQuery2p1):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
    supplier_join_hash_table = BlimpSupplierHashSet(65536, 131072)
    part_join_hash_table = BlimpPartHashMap(262144, 524288)
    date_join_hash_table = BlimpDateHashMap(4096, 4096)
