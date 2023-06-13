from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.hitmap import BlimpVHashmapJoin, BlimpHashmapJoin
from src.queries.join.hitmap.early_pruning import BlimpVHashmapEarlyPruningJoin, BlimpHashmapEarlyPruningJoin
from src.queries.join.hitmap_payload.early_pruning import BlimpVHashmapEarlyPruningHitmapPayloadJoin, BlimpHashmapEarlyPruningHitmapPayloadJoin
from src.queries.emit.hashmap_payload import BlimpHitmapEmitHashmapPayload, BlimpVHitmapEmitHashmapPayload

from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBCustomerTable, SSBDateTable, SSBLineOrderTable
from studies.star_schema_benchmark.ssb import SSBRegionEncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.q3_x import SSBQuery3pX, SSBQuery3pXSupplierCustomerDate, SSBQuery3pXCustomerSupplierDate


class SSBQuery3p1(SSBQuery3pX):
    def _supplier_record_joined_hashtable_object(self, record: SSBSupplierTable.TableRecord):
        return self.Blimp32bk8bpHashMap.Blimp32bk8bpBucket.Hash32bitObject8bPayload(record.supplier_key, record.nation)

    def _customer_record_joined_hashtable_object(self, record: SSBCustomerTable.TableRecord):
        return self.Blimp32bk8bpHashMap.Blimp32bk8bpBucket.Hash32bitObject8bPayload(record.customer_key, record.nation)

    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.ASIA

    def _customer_record_join_condition(self, record: SSBCustomerTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.ASIA

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return 1992 <= record.year <= 1997

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        supplier_fks = set()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.region == SSBRegionEncoding.ASIA:
                supplier_fks.add(record.supplier_key)

        customer_fks = set()
        for idx, record in enumerate(SSBCustomerTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.region == SSBRegionEncoding.ASIA:
                customer_fks.add(record.customer_key)

        date_fks = set()
        for idx, record in enumerate(SSBDateTable(scale_factor=self.scale_factor, no_storage=True).records):
            if 1992 <= record.year <= 1997:
                date_fks.add(record.date_key)

        join_fks = set()
        limit = GenericLineOrderColumn.scale(self.scale_factor) // self.parallelism_factor
        for index, record in enumerate(SSBLineOrderTable(scale_factor=self.scale_factor, no_storage=True).records):
            if index >= limit:
                break

            if record.customer_key in customer_fks and record.supplier_key in supplier_fks and record.order_date_key in date_fks:
                join_fks.add(index)

        assert set(final_hitmap_result.result_record_indexes) == join_fks


class SSBQuery3p1BlimpVSupplierCustomerDate(SSBQuery3pXSupplierCustomerDate, SSBQuery3p1):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload


class SSBQuery3p1BlimpSupplierCustomerDate(SSBQuery3pXSupplierCustomerDate, SSBQuery3p1):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
    emit_3_query_class = BlimpHitmapEmitHashmapPayload


class SSBQuery3p1BlimpVCustomerSupplierDate(SSBQuery3pXCustomerSupplierDate, SSBQuery3p1):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload


class SSBQuery3p1BlimpCustomerSupplierDate(SSBQuery3pXCustomerSupplierDate, SSBQuery3p1):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
    emit_3_query_class = BlimpHitmapEmitHashmapPayload
