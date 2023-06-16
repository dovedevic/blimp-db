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

from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBCustomerTable, SSBDateTable, SSBLineOrderTable
from studies.star_schema_benchmark.ssb import SSBCityEncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.queries.q3.q3_x import SSBQuery3pX, SSBQuery3pXSupplierCustomerDate, \
    SSBQuery3pXCustomerSupplierDate
from studies.star_schema_benchmark.queries.q3.q3_x import BlimpSupplierHashMap, BlimpVSupplierHashMap, \
    BlimpCustomerHashMap, BlimpVCustomerHashMap, BlimpDateHashMap, BlimpVDateHashMap


class SSBQuery3p4(SSBQuery3pX):
    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.city == SSBCityEncoding.convert("UNITED KI1") or record.city == SSBCityEncoding.convert("UNITED KI5")

    def _customer_record_join_condition(self, record: SSBCustomerTable.TableRecord) -> bool:
        return record.city == SSBCityEncoding.convert("UNITED KI1") or record.city == SSBCityEncoding.convert("UNITED KI5")

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return record.year_month == "Dec1997"

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        supplier_fks = set()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.city == SSBCityEncoding.convert("UNITED KI1") or record.city == SSBCityEncoding.convert("UNITED KI5"):
                supplier_fks.add(record.supplier_key)

        customer_fks = set()
        for idx, record in enumerate(SSBCustomerTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.city == SSBCityEncoding.convert("UNITED KI1") or record.city == SSBCityEncoding.convert("UNITED KI5"):
                customer_fks.add(record.customer_key)

        date_fks = set()
        for idx, record in enumerate(SSBDateTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.year_month == "Dec1997":
                date_fks.add(record.date_key)

        join_fks = set()
        limit = GenericLineOrderColumn.scale(self.scale_factor) // self.parallelism_factor
        for index, record in enumerate(SSBLineOrderTable(scale_factor=self.scale_factor, no_storage=True).records):
            if index >= limit:
                break

            if record.customer_key in customer_fks and record.supplier_key in supplier_fks and record.order_date_key in date_fks:
                join_fks.add(index)

        assert set(final_hitmap_result.result_record_indexes) == join_fks


class SSBQuery3p4BlimpVSupplierCustomerDate(SSBQuery3pXSupplierCustomerDate, SSBQuery3p4):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload
    supplier_join_hash_table = BlimpVSupplierHashMap(1024, 2048)
    customer_join_hash_table = BlimpVCustomerHashMap(16384, 32768)
    date_join_hash_table = BlimpVDateHashMap(64, 64)


class SSBQuery3p4BlimpSupplierCustomerDate(SSBQuery3pXSupplierCustomerDate, SSBQuery3p4):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
    emit_3_query_class = BlimpHitmapEmitHashmapPayload
    supplier_join_hash_table = BlimpSupplierHashMap(32768, 65536)
    customer_join_hash_table = BlimpCustomerHashMap(131072, 262144)
    date_join_hash_table = BlimpDateHashMap(64, 64)


class SSBQuery3p4BlimpVCustomerSupplierDate(SSBQuery3pXCustomerSupplierDate, SSBQuery3p4):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningJoin
    join_3_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload
    supplier_join_hash_table = BlimpVSupplierHashMap(1024, 2048)
    customer_join_hash_table = BlimpVCustomerHashMap(16384, 32768)
    date_join_hash_table = BlimpVDateHashMap(64, 64)


class SSBQuery3p4BlimpCustomerSupplierDate(SSBQuery3pXCustomerSupplierDate, SSBQuery3p4):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningJoin
    join_3_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
    emit_3_query_class = BlimpHitmapEmitHashmapPayload
    supplier_join_hash_table = BlimpSupplierHashMap(32768, 65536)
    customer_join_hash_table = BlimpCustomerHashMap(131072, 262144)
    date_join_hash_table = BlimpDateHashMap(64, 64)
