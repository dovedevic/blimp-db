import math

from typing import Tuple

from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.configurations.hashables.blimp import BlimpSimpleHashSet, Hash32bitObjectNullPayload
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.simulators.result import HitmapResult, RuntimeResult, MemoryArrayResult
from queries.emit.index.blimp import BlimpHitmapEmit
from src.queries.join.hitmap import BlimpVHashmapJoin, BlimpHashmapJoin
from src.queries.join.hitmap.early_pruning import BlimpVHashmapEarlyPruningJoin, BlimpHashmapEarlyPruningJoin
from src.queries.join.hitmap_payload.early_pruning import BlimpVHashmapEarlyPruningHitmapPayloadJoin, BlimpHashmapEarlyPruningHitmapPayloadJoin
from src.queries.emit.hashmap_payload import BlimpHitmapEmitHashmapPayload, BlimpVHitmapEmitHashmapPayload

from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBCustomerTable, SSBDateTable, SSBLineOrderTable, SSBPartTable
from studies.star_schema_benchmark.ssb import SSBRegionEncoding, SSBMFGREncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.q4_x import SSBQuery4pX, \
    SSBQuery4pXCustomerPartSupplierDate, SSBQuery4pXCustomerSupplierPartDate, \
    SSBQuery4pXSupplierCustomerPartDate, SSBQuery4pXSupplierPartCustomerDate, \
    SSBQuery4pXPartCustomerSupplierDate, SSBQuery4pXPartSupplierCustomerDate


class SSBQuery4p2(SSBQuery4pX):
    supplier_join_hash_table = SSBQuery4pX.Blimp32bk8bpHashMap(2048, 2048 * 2)
    part_join_hash_table = SSBQuery4pX.Blimp32bk8bpHashMap(32768, 32768 * 2)
    customer_join_hash_table = BlimpSimpleHashSet(32768, 32768 * 2)

    def _customer_record_joined_hashtable_object(self, record: SSBCustomerTable.TableRecord):
        return Hash32bitObjectNullPayload(record.customer_key)

    def _supplier_record_joined_hashtable_object(self, record: SSBSupplierTable.TableRecord):
        return self.Blimp32bk8bpHashMap.Blimp32bk8bpBucket.Hash32bitObject8bPayload(record.supplier_key, record.nation)

    def _part_record_joined_hashtable_object(self, record: SSBPartTable.TableRecord):
        return self.Blimp32bk8bpHashMap.Blimp32bk8bpBucket.Hash32bitObject8bPayload(record.part_key, record.category)

    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.AMERICA

    def _part_record_join_condition(self, record: SSBPartTable.TableRecord) -> bool:
        return record.mfgr == SSBMFGREncoding.MFGR_1 or record.mfgr == SSBMFGREncoding.MFGR_2

    def _customer_record_join_condition(self, record: SSBCustomerTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.AMERICA

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return record.year == 1997 or record.year == 1998

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

        date_fks = set()
        for idx, record in enumerate(SSBDateTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.year == 1997 or record.year == 1998:
                date_fks.add(record.date_key)

        join_fks = set()
        limit = GenericLineOrderColumn.scale(self.scale_factor) // self.parallelism_factor
        for index, record in enumerate(SSBLineOrderTable(scale_factor=self.scale_factor, no_storage=True).records):
            if index >= limit:
                break

            if record.customer_key in customer_fks and record.supplier_key in supplier_fks and record.part_key in part_fks and record.order_date_key in date_fks:
                join_fks.add(index)

        assert set(final_hitmap_result.result_record_indexes) == join_fks

    def _perform_emit_3_layout(self, *args):
        self._get_supplier_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_3_placement(self, final_hitmap, *args):
        self._get_supplier_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_3_query_class(self._get_simulator(), self._get_supplier_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=(
                    self._get_supplier_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(
                        self.supplier_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            hitmap_index=0,
            hash_map=self.supplier_join_hash_table,
            return_labels=False
        )

        if save_query_output:
            kernel_memory_array.save('supplier_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_3_runtime.res')

        return kernel_runtime, kernel_memory_array

    def _perform_emit_4_layout(self, *args):
        self._get_part_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_4_placement(self, final_hitmap, *args):
        self._get_part_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_4_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_4_query_class(self._get_simulator(), self._get_part_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=(
                    self._get_part_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(
                        self.part_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            hitmap_index=0,
            hash_map=self.part_join_hash_table,
            return_labels=False
        )

        if save_query_output:
            kernel_memory_array.save('part_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_3_runtime.res')

        return kernel_runtime, kernel_memory_array


class SSBQuery4p2BlimpVXYZ(SSBQuery4p2):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    join_1_query_class = BlimpVHashmapJoin
    join_2_query_class = BlimpVHashmapEarlyPruningJoin
    join_3_query_class = BlimpVHashmapEarlyPruningJoin
    join_4_query_class = BlimpVHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmit
    emit_3_query_class = BlimpVHitmapEmitHashmapPayload
    emit_4_query_class = BlimpVHitmapEmitHashmapPayload


class SSBQuery4p2BlimpXYZ(SSBQuery4p2):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    join_1_query_class = BlimpHashmapJoin
    join_2_query_class = BlimpHashmapEarlyPruningJoin
    join_3_query_class = BlimpHashmapEarlyPruningJoin
    join_4_query_class = BlimpHashmapEarlyPruningHitmapPayloadJoin
    emit_1_query_class = BlimpHitmapEmit
    emit_2_query_class = BlimpHitmapEmit
    emit_3_query_class = BlimpHitmapEmitHashmapPayload
    emit_4_query_class = BlimpHitmapEmitHashmapPayload


class SSBQuery4p2BlimpVCustomerPartSupplierDate(SSBQuery4pXCustomerPartSupplierDate, SSBQuery4p2BlimpVXYZ):
    pass


class SSBQuery4p2BlimpVCustomerSupplierPartDate(SSBQuery4pXCustomerSupplierPartDate, SSBQuery4p2BlimpVXYZ):
    pass


class SSBQuery4p2BlimpVSupplierCustomerPartDate(SSBQuery4pXSupplierCustomerPartDate, SSBQuery4p2BlimpVXYZ):
    pass


class SSBQuery4p2BlimpVPartCustomerSupplierDate(SSBQuery4pXPartCustomerSupplierDate, SSBQuery4p2BlimpVXYZ):
    pass


class SSBQuery4p2BlimpVSupplierPartCustomerDate(SSBQuery4pXSupplierPartCustomerDate, SSBQuery4p2BlimpVXYZ):
    pass


class SSBQuery4p2BlimpVPartSupplierCustomerDate(SSBQuery4pXPartSupplierCustomerDate, SSBQuery4p2BlimpVXYZ):
    pass


class SSBQuery4p2BlimpCustomerPartSupplierDate(SSBQuery4pXCustomerPartSupplierDate, SSBQuery4p2BlimpXYZ):
    pass


class SSBQuery4p2BlimpCustomerSupplierPartDate(SSBQuery4pXCustomerSupplierPartDate, SSBQuery4p2BlimpXYZ):
    pass


class SSBQuery4p2BlimpSupplierCustomerPartDate(SSBQuery4pXSupplierCustomerPartDate, SSBQuery4p2BlimpXYZ):
    pass


class SSBQuery4p2BlimpPartCustomerSupplierDate(SSBQuery4pXPartCustomerSupplierDate, SSBQuery4p2BlimpXYZ):
    pass


class SSBQuery4p2BlimpSupplierPartCustomerDate(SSBQuery4pXSupplierPartCustomerDate, SSBQuery4p2BlimpXYZ):
    pass


class SSBQuery4p2BlimpPartSupplierCustomerDate(SSBQuery4pXPartSupplierCustomerDate, SSBQuery4p2BlimpXYZ):
    pass


SSBQuery4p2BlimpVCustomerPartSupplierDate().run_query()
SSBQuery4p2BlimpVCustomerSupplierPartDate().run_query()
SSBQuery4p2BlimpVSupplierCustomerPartDate().run_query()
SSBQuery4p2BlimpVPartCustomerSupplierDate().run_query()
SSBQuery4p2BlimpVSupplierPartCustomerDate().run_query()
SSBQuery4p2BlimpVPartSupplierCustomerDate().run_query()

SSBQuery4p2BlimpCustomerPartSupplierDate().run_query()
SSBQuery4p2BlimpCustomerSupplierPartDate().run_query()
SSBQuery4p2BlimpSupplierCustomerPartDate().run_query()
SSBQuery4p2BlimpPartCustomerSupplierDate().run_query()
SSBQuery4p2BlimpSupplierPartCustomerDate().run_query()
SSBQuery4p2BlimpPartSupplierCustomerDate().run_query()
