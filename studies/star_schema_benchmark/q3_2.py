from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.hitmap import BlimpVHashmapJoin, BlimpHashmapJoin
from src.queries.join.hitmap.early_termination import BlimpVHashmapEarlyTerminationJoin, BlimpHashmapEarlyTerminationJoin
from src.queries.join.hitmap_payload.early_termination import BlimpVHashmapEarlyTerminationHitmapPayloadJoin, BlimpHashmapEarlyTerminationHitmapPayloadJoin
from src.queries.emit.hashmap_payload import BlimpHitmapEmitHashmapPayload, BlimpVHitmapEmitHashmapPayload

from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBCustomerTable, SSBDateTable, SSBLineOrderTable
from studies.star_schema_benchmark.ssb import SSBNationEncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.q3_x import SSBQuery3pX, SSBQuery3pXSupplierCustomerDate, SSBQuery3pXCustomerSupplierDate


class SSBQuery3p2(SSBQuery3pX):
    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.nation == SSBNationEncoding.UNITED_STATES

    def _customer_record_join_condition(self, record: SSBCustomerTable.TableRecord) -> bool:
        return record.nation == SSBNationEncoding.UNITED_STATES

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return 1992 <= record.year <= 1997

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        supplier_fks = set()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.nation == SSBNationEncoding.UNITED_STATES:
                supplier_fks.add(record.supplier_key)

        customer_fks = set()
        for idx, record in enumerate(SSBCustomerTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.nation == SSBNationEncoding.UNITED_STATES:
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


class SSBQuery3p2BlimpVSupplierCustomerDate(SSBQuery3pXSupplierCustomerDate, SSBQuery3p2):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyTerminationJoin
    join_3_query_class = BlimpVHashmapEarlyTerminationHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload


class SSBQuery3p2BlimpSupplierCustomerDate(SSBQuery3pXSupplierCustomerDate, SSBQuery3p2):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyTerminationJoin
    join_3_query_class = BlimpHashmapEarlyTerminationHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
    emit_3_query_class = BlimpHitmapEmitHashmapPayload


class SSBQuery3p2BlimpVCustomerSupplierDate(SSBQuery3pXCustomerSupplierDate, SSBQuery3p2):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyTerminationJoin
    join_3_query_class = BlimpVHashmapEarlyTerminationHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpVHitmapEmitHashmapPayload
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload


class SSBQuery3p2BlimpCustomerSupplierDate(SSBQuery3pXCustomerSupplierDate, SSBQuery3p2):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyTerminationJoin
    join_3_query_class = BlimpHashmapEarlyTerminationHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmitHashmapPayload
    emit_3_query_class = BlimpHitmapEmitHashmapPayload


SSBQuery3p2BlimpVSupplierCustomerDate().run_query()
SSBQuery3p2BlimpVCustomerSupplierDate().run_query()

SSBQuery3p2BlimpSupplierCustomerDate().run_query()
SSBQuery3p2BlimpCustomerSupplierDate().run_query()
