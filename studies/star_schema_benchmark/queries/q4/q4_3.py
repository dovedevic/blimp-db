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
from studies.star_schema_benchmark.ssb import SSBRegionEncoding, SSBCategoryEncoding, SSBNationEncoding
from studies.star_schema_benchmark.columns import GenericLineOrderColumn
from studies.star_schema_benchmark.queries.q4.q4_x import SSBQuery4pX, \
    SSBQuery4pXCustomerPartSupplierDate, SSBQuery4pXCustomerSupplierPartDate, \
    SSBQuery4pXSupplierCustomerPartDate, SSBQuery4pXSupplierPartCustomerDate, \
    SSBQuery4pXPartCustomerSupplierDate, SSBQuery4pXPartSupplierCustomerDate
from studies.star_schema_benchmark.queries.q4.q4_x import BlimpDateHashMap, BlimpVDateHashMap, \
    BlimpCustomerHashSet, BlimpVCustomerHashSet, BlimpSupplierNationCityHashMap, BlimpVSupplierNationCityHashMap, \
    BlimpPartBrandHashMap, BlimpVPartBrandHashMap, Hash32bitObject8bPayload, Hash32bitObject16bPayload


class SSBQuery4p3(SSBQuery4pX):

    def _supplier_record_joined_hashtable_object(self, record: SSBSupplierTable.TableRecord):
        return Hash32bitObject8bPayload(record.supplier_key, record.city)

    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        return record.nation == SSBNationEncoding.UNITED_STATES

    def _part_record_joined_hashtable_object(self, record: SSBPartTable.TableRecord):
        return Hash32bitObject16bPayload(record.part_key, record.brand)

    def _part_record_join_condition(self, record: SSBPartTable.TableRecord) -> bool:
        return record.category == SSBCategoryEncoding.convert('MFGR#14')

    def _customer_record_joined_hashtable_object(self, record: SSBCustomerTable.TableRecord):
        return Hash32bitObjectNullPayload(record.customer_key)

    def _customer_record_join_condition(self, record: SSBCustomerTable.TableRecord) -> bool:
        return record.region == SSBRegionEncoding.AMERICA

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        return record.year == 1997 or record.year == 1998

    def _validate(self, final_hitmap_result: HitmapResult, *args):
        supplier_fks = set()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.nation == SSBNationEncoding.UNITED_STATES:
                supplier_fks.add(record.supplier_key)

        customer_fks = set()
        for idx, record in enumerate(SSBCustomerTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.region == SSBRegionEncoding.AMERICA:
                customer_fks.add(record.customer_key)

        part_fks = set()
        for idx, record in enumerate(SSBPartTable(scale_factor=self.scale_factor, no_storage=True).records):
            if record.category == SSBCategoryEncoding.convert('MFGR#14'):
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
        )

        if save_query_output:
            kernel_memory_array.save('part_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_3_runtime.res')

        return kernel_runtime, kernel_memory_array


class SSBQuery4p3BlimpVXYZ(SSBQuery4p3):
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
    supplier_join_hash_table = BlimpVSupplierNationCityHashMap(4096, 8192)
    part_join_hash_table = BlimpVPartBrandHashMap(65536, 131072)
    customer_join_hash_table = BlimpVCustomerHashSet(65536, 131072)
    date_join_hash_table = BlimpVDateHashMap(128, 128)


class SSBQuery4p3BlimpXYZ(SSBQuery4p3):
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
    supplier_join_hash_table = BlimpSupplierNationCityHashMap(131072, 262144)
    part_join_hash_table = BlimpPartBrandHashMap(524288, 1048576)
    customer_join_hash_table = BlimpCustomerHashSet(524288, 1048576)
    date_join_hash_table = BlimpDateHashMap(1024, 1024)


class SSBQuery4p3BlimpVCustomerPartSupplierDate(SSBQuery4pXCustomerPartSupplierDate, SSBQuery4p3BlimpVXYZ):
    pass


class SSBQuery4p3BlimpVCustomerSupplierPartDate(SSBQuery4pXCustomerSupplierPartDate, SSBQuery4p3BlimpVXYZ):
    pass


class SSBQuery4p3BlimpVSupplierCustomerPartDate(SSBQuery4pXSupplierCustomerPartDate, SSBQuery4p3BlimpVXYZ):
    pass


class SSBQuery4p3BlimpVPartCustomerSupplierDate(SSBQuery4pXPartCustomerSupplierDate, SSBQuery4p3BlimpVXYZ):
    pass


class SSBQuery4p3BlimpVSupplierPartCustomerDate(SSBQuery4pXSupplierPartCustomerDate, SSBQuery4p3BlimpVXYZ):
    pass


class SSBQuery4p3BlimpVPartSupplierCustomerDate(SSBQuery4pXPartSupplierCustomerDate, SSBQuery4p3BlimpVXYZ):
    pass


class SSBQuery4p3BlimpCustomerPartSupplierDate(SSBQuery4pXCustomerPartSupplierDate, SSBQuery4p3BlimpXYZ):
    pass


class SSBQuery4p3BlimpCustomerSupplierPartDate(SSBQuery4pXCustomerSupplierPartDate, SSBQuery4p3BlimpXYZ):
    pass


class SSBQuery4p3BlimpSupplierCustomerPartDate(SSBQuery4pXSupplierCustomerPartDate, SSBQuery4p3BlimpXYZ):
    pass


class SSBQuery4p3BlimpPartCustomerSupplierDate(SSBQuery4pXPartCustomerSupplierDate, SSBQuery4p3BlimpXYZ):
    pass


class SSBQuery4p3BlimpSupplierPartCustomerDate(SSBQuery4pXSupplierPartCustomerDate, SSBQuery4p3BlimpXYZ):
    pass


class SSBQuery4p3BlimpPartSupplierCustomerDate(SSBQuery4pXPartSupplierCustomerDate, SSBQuery4p3BlimpXYZ):
    pass
