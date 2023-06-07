import math

from typing import Tuple

from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.configurations.hashables.blimp import BlimpSimpleHashSet, Hash32bitObjectNullPayload
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult, RuntimeResult, MemoryArrayResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.hitmap import BlimpVHashmapJoin, BlimpHashmapJoin
from src.queries.join.hitmap.early_termination import BlimpVHashmapEarlyTerminationJoin, BlimpHashmapEarlyTerminationJoin
from src.queries.join.hitmap_payload.early_termination import BlimpVHashmapEarlyTerminationHitmapPayloadJoin, BlimpHashmapEarlyTerminationHitmapPayloadJoin
from src.queries.emit.hashmap_payload import BlimpHitmapEmitHashmapPayload, BlimpVHitmapEmitHashmapPayload

from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBCustomerTable, SSBDateTable, SSBLineOrderTable, SSBPartTable
from studies.star_schema_benchmark.ssb import SSBRegionEncoding, SSBMFGREncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.q4_x import SSBQuery4pX, \
    SSBQuery4pXCustomerPartSupplierDate, SSBQuery4pXCustomerSupplierPartDate, \
    SSBQuery4pXSupplierCustomerPartDate, SSBQuery4pXSupplierPartCustomerDate, \
    SSBQuery4pXPartCustomerSupplierDate, SSBQuery4pXPartSupplierCustomerDate


class SSBQuery4p1(SSBQuery4pX):
    supplier_join_hash_table = BlimpSimpleHashSet(2048, 4096)
    part_join_hash_table = BlimpSimpleHashSet(32768, 32768*2)
    customer_join_hash_table = SSBQuery4pX.Blimp32bk8bpHashMap(32768, 32768 * 2)

    def _supplier_record_joined_hashtable_object(self, record: SSBSupplierTable.TableRecord):
        return Hash32bitObjectNullPayload(record.supplier_key)

    def _part_record_joined_hashtable_object(self, record: SSBPartTable.TableRecord):
        return Hash32bitObjectNullPayload(record.part_key)

    def _customer_record_joined_hashtable_object(self, record: SSBCustomerTable.TableRecord):
        return self.Blimp32bk8bpHashMap.Blimp32bk8bpBucket.Hash32bitObject8bPayload(record.customer_key, record.nation)

    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.AMERICA

    def _part_record_join_condition(self, record: SSBPartTable.TableRecord) -> bool:
        return record.mfgr == SSBMFGREncoding.MFGR_1 or record.mfgr == SSBMFGREncoding.MFGR_2

    def _customer_record_join_condition(self, record: SSBCustomerTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.AMERICA

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return True

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        supplier_fks = set()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.region == SSBRegionEncoding.AMERICA:
                supplier_fks.add(record.supplier_key)

        customer_fks = set()
        for idx, record in enumerate(SSBCustomerTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.region == SSBRegionEncoding.AMERICA:
                customer_fks.add(record.customer_key)

        part_fks = set()
        for idx, record in enumerate(SSBPartTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.mfgr == SSBMFGREncoding.MFGR_1 or record.mfgr == SSBMFGREncoding.MFGR_2:
                part_fks.add(record.part_key)

        join_fks = set()
        limit = GenericLineOrderColumn.scale(self.scale_factor) // self.parallelism_factor
        for index, record in enumerate(SSBLineOrderTable(scale_factor=self.scale_factor, no_storage=True).records):
            if index >= limit:
                break

            if record.customer_key in customer_fks and record.supplier_key in supplier_fks and record.part_key in part_fks:
                join_fks.add(index)

        assert set(final_hitmap_result.result_record_indexes) == join_fks

    def _perform_emit_3_layout(self, *args):
        self._get_customer_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_3_placement(self, final_hitmap, *args):
        self._get_customer_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_3_query_class(self._get_simulator(), self._get_customer_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=(
                    self._get_customer_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(
                        self.customer_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            hitmap_index=0,
            hash_map=self.customer_join_hash_table,
            return_labels=False
        )

        if save_query_output:
            kernel_memory_array.save('customer_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_3_runtime.res')

        return kernel_runtime, kernel_memory_array

    def _perform_emit_4_layout(self, *args):
        pass

    def _perform_emit_4_placement(self, *args):
        pass

    def _perform_emit_4_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return RuntimeResult(), MemoryArrayResult()


class SSBQuery4p1BlimpVXYZ(SSBQuery4p1):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyTerminationJoin
    join_3_query_class = BlimpVHashmapEarlyTerminationJoin
    join_4_query_class = BlimpVHashmapEarlyTerminationHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmit
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload


class SSBQuery4p1BlimpXYZ(SSBQuery4p1):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyTerminationJoin
    join_3_query_class = BlimpHashmapEarlyTerminationJoin
    join_4_query_class = BlimpHashmapEarlyTerminationHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmit
    emit_3_query_class = BlimpHitmapEmitHashmapPayload


class SSBQuery4p1BlimpVCustomerPartSupplierDate(SSBQuery4pXCustomerPartSupplierDate, SSBQuery4p1BlimpVXYZ):
    pass


class SSBQuery4p1BlimpVCustomerSupplierPartDate(SSBQuery4pXCustomerSupplierPartDate, SSBQuery4p1BlimpVXYZ):
    pass


class SSBQuery4p1BlimpVSupplierCustomerPartDate(SSBQuery4pXSupplierCustomerPartDate, SSBQuery4p1BlimpVXYZ):
    pass


class SSBQuery4p1BlimpVPartCustomerSupplierDate(SSBQuery4pXPartCustomerSupplierDate, SSBQuery4p1BlimpVXYZ):
    pass


class SSBQuery4p1BlimpVSupplierPartCustomerDate(SSBQuery4pXSupplierPartCustomerDate, SSBQuery4p1BlimpVXYZ):
    join_3_query_class = BlimpVHashmapEarlyTerminationHitmapPayloadJoin
    emit_3_query_class = None

    def _perform_emit_3_layout(self, *args):
        pass

    def _perform_emit_3_placement(self, *args):
        pass

    def _perform_emit_3_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return RuntimeResult(), MemoryArrayResult()


class SSBQuery4p1BlimpVPartSupplierCustomerDate(SSBQuery4pXPartSupplierCustomerDate, SSBQuery4p1BlimpVXYZ):
    join_3_query_class = BlimpVHashmapEarlyTerminationHitmapPayloadJoin
    emit_3_query_class = None

    def _perform_emit_3_layout(self, *args):
        pass

    def _perform_emit_3_placement(self, *args):
        pass

    def _perform_emit_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return RuntimeResult(), MemoryArrayResult()


class SSBQuery4p1BlimpCustomerPartSupplierDate(SSBQuery4pXCustomerPartSupplierDate, SSBQuery4p1BlimpXYZ):
    pass


class SSBQuery4p1BlimpCustomerSupplierPartDate(SSBQuery4pXCustomerSupplierPartDate, SSBQuery4p1BlimpXYZ):
    pass


class SSBQuery4p1BlimpSupplierCustomerPartDate(SSBQuery4pXSupplierCustomerPartDate, SSBQuery4p1BlimpXYZ):
    pass


class SSBQuery4p1BlimpPartCustomerSupplierDate(SSBQuery4pXPartCustomerSupplierDate, SSBQuery4p1BlimpXYZ):
    pass


class SSBQuery4p1BlimpSupplierPartCustomerDate(SSBQuery4pXSupplierPartCustomerDate, SSBQuery4p1BlimpXYZ):
    join_3_query_class = BlimpHashmapEarlyTerminationHitmapPayloadJoin
    emit_3_query_class = None

    def _perform_emit_3_layout(self, *args):
        pass

    def _perform_emit_3_placement(self, *args):
        pass

    def _perform_emit_3_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return RuntimeResult(), MemoryArrayResult()


class SSBQuery4p1BlimpPartSupplierCustomerDate(SSBQuery4pXPartSupplierCustomerDate, SSBQuery4p1BlimpXYZ):
    join_3_query_class = BlimpHashmapEarlyTerminationHitmapPayloadJoin
    emit_3_query_class = None

    def _perform_emit_3_layout(self, *args):
        pass

    def _perform_emit_3_placement(self, *args):
        pass

    def _perform_emit_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return RuntimeResult(), MemoryArrayResult()


SSBQuery4p1BlimpVCustomerPartSupplierDate().run_query()
SSBQuery4p1BlimpVCustomerSupplierPartDate().run_query()
SSBQuery4p1BlimpVSupplierCustomerPartDate().run_query()
SSBQuery4p1BlimpVPartCustomerSupplierDate().run_query()
SSBQuery4p1BlimpVSupplierPartCustomerDate().run_query()
SSBQuery4p1BlimpVPartSupplierCustomerDate().run_query()

SSBQuery4p1BlimpCustomerPartSupplierDate().run_query()
SSBQuery4p1BlimpCustomerSupplierPartDate().run_query()
SSBQuery4p1BlimpSupplierCustomerPartDate().run_query()
SSBQuery4p1BlimpPartCustomerSupplierDate().run_query()
SSBQuery4p1BlimpSupplierPartCustomerDate().run_query()
SSBQuery4p1BlimpPartSupplierCustomerDate().run_query()
