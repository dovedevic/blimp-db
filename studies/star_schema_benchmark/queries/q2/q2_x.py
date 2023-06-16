import math

from typing import Union, Tuple, Any

from src.simulators.result import RuntimeResult, HitmapResult, MemoryArrayResult
from src.simulators.hashmap import GenericHashTableObject
from src.utils.generic import ceil_to_multiple
from src.queries import Query
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables.blimp import GenericHashMap, BlimpSimpleHashSet, Hash32bitObjectNullPayload, \
    BlimpBucket, Object32bit, Object8bit, Object24bitNullMax, Object32bitNullMax, Hash32bitObject16bPayload

from studies.star_schema_benchmark.generic import GenericSSBQuery
from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBPartTable, SSBDateTable
from studies.star_schema_benchmark.columns import LineOrderOrderDate, LineOrderPartKey, LineOrderSupplyKey, \
    LineOrderRevenue


class BlimpSupplierHashSet(BlimpSimpleHashSet):
    class Blimp32bk3cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
        _BUCKET_OBJECT_CAPACITY = 3
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk3cBucket


class BlimpVSupplierHashSet(BlimpSimpleHashSet):
    class Blimp32bk31cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
        _BUCKET_OBJECT_CAPACITY = 31
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk31cBucket


class BlimpPartHashMap(BlimpSimpleHashSet):
    class Blimp32bk16bp2cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 2
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk16bp2cBucket


class BlimpVPartHashMap(BlimpSimpleHashSet):
    class Blimp32bk20cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 20
        _META_NEXT_BUCKET_OBJECT = Object32bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object32bit
    _BUCKET_OBJECT = Blimp32bk20cBucket


class BlimpDateHashMap(BlimpPartHashMap):
    pass


class BlimpVDateHashMap(BlimpVPartHashMap):
    pass


class SSBQuery2pX(GenericSSBQuery):

    supplier_join_hash_table = GenericHashMap(0, 0)
    part_join_hash_table = GenericHashMap(0, 0)
    date_join_hash_table = GenericHashMap(0, 0)

    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        raise NotImplemented

    def _supplier_record_joined_hashtable_object(self, record: SSBSupplierTable.TableRecord) -> GenericHashTableObject:
        return Hash32bitObjectNullPayload(record.supplier_key)

    def _build_supplier_hash_table(self):
        self.supplier_join_hash_table.reset()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if self._supplier_record_join_condition(record):
                self.supplier_join_hash_table.insert(self._supplier_record_joined_hashtable_object(record))
        return self.supplier_join_hash_table

    def _part_record_join_condition(self, record: SSBPartTable.TableRecord) -> bool:
        raise NotImplemented

    def _part_record_joined_hashtable_object(self, record: SSBPartTable.TableRecord) -> GenericHashTableObject:
        return Hash32bitObject16bPayload(record.part_key, record.brand)

    def _build_part_hash_table(self):
        self.part_join_hash_table.reset()
        for idx, record in enumerate(SSBPartTable(scale_factor=self.scale_factor, no_storage=True).records):
            if self._part_record_join_condition(record):
                self.part_join_hash_table.insert(self._part_record_joined_hashtable_object(record))
        return self.part_join_hash_table

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        raise NotImplemented

    def _date_record_joined_hashtable_object(self, record: SSBDateTable.TableRecord) -> GenericHashTableObject:
        return Hash32bitObject16bPayload(record.date_key, record.year)

    def _build_date_hash_table(self):
        self.date_join_hash_table.reset()
        for idx, record in enumerate(SSBDateTable(scale_factor=self.scale_factor, no_storage=True).records):
            if self._date_record_join_condition(record):
                self.date_join_hash_table.insert(self._date_record_joined_hashtable_object(record))
        return self.date_join_hash_table

    def __generic_db_config(self):
        return {
            "total_record_size_bytes": 4,
            "total_index_size_bytes": 4,
            "blimp_code_region_size_bytes": 2048,
            "blimp_temporary_region_size_bytes": 0,
            "ambit_temporary_bits": 0,
            "hitmap_count": 1,
            "early_termination_frequency": 4
        }

    supplier_join_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __supplier_database_configuration = None

    def _calculate_supplier_inout_size(self):
        return ceil_to_multiple(self.supplier_join_hash_table.size, self._get_hardware_config().row_buffer_size_bytes) \
            + ceil_to_multiple(4 * 65536, self._get_hardware_config().row_buffer_size_bytes)

    def _supplier_database_configuration_json(self):
        base = self.__generic_db_config()
        base["blimp_temporary_region_size_bytes"] = self._calculate_supplier_inout_size()
        return base

    def _get_supplier_database_configuration(self):
        if self.__supplier_database_configuration is None:
            self.__supplier_database_configuration = self.supplier_join_database_configuration_class(
                **self._supplier_database_configuration_json()
            )
        return self.__supplier_database_configuration

    __supplier_join_data_layout = None
    supplier_join_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_supplier_record_limit(self):
        return LineOrderSupplyKey.scale(self.scale_factor) // self.parallelism_factor

    def _get_supplier_generator(self):
        return LineOrderSupplyKey(total_records=self._get_supplier_record_limit(), scale_factor=self.scale_factor)

    def _get_supplier_layout_configuration(self):
        if self.__supplier_join_data_layout is None:
            self.__supplier_join_data_layout = self.supplier_join_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_supplier_database_configuration(),
                generator=self._get_supplier_generator()
            )
        return self.__supplier_join_data_layout

    part_join_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __part_database_configuration = None

    def _calculate_part_inout_size(self):
        return ceil_to_multiple(self.part_join_hash_table.size, self._get_hardware_config().row_buffer_size_bytes) \
            + ceil_to_multiple(2 * 65536, self._get_hardware_config().row_buffer_size_bytes)

    def _part_database_configuration_json(self):
        base = self.__generic_db_config()
        base["blimp_temporary_region_size_bytes"] = self._calculate_part_inout_size()
        return base

    def _get_part_database_configuration(self):
        if self.__part_database_configuration is None:
            self.__part_database_configuration = self.part_join_database_configuration_class(
                **self._part_database_configuration_json()
            )
        return self.__part_database_configuration

    __part_join_data_layout = None
    part_join_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_part_record_limit(self):
        return LineOrderPartKey.scale(self.scale_factor) // self.parallelism_factor

    def _get_part_generator(self):
        return LineOrderPartKey(total_records=self._get_part_record_limit(), scale_factor=self.scale_factor)

    def _get_part_layout_configuration(self):
        if self.__part_join_data_layout is None:
            self.__part_join_data_layout = self.part_join_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_part_database_configuration(),
                generator=self._get_part_generator()
            )
        return self.__part_join_data_layout

    date_join_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __date_database_configuration = None

    def _calculate_date_inout_size(self):
        return ceil_to_multiple(self.date_join_hash_table.size, self._get_hardware_config().row_buffer_size_bytes) \
            + ceil_to_multiple(4 * 65536, self._get_hardware_config().row_buffer_size_bytes)

    def _date_database_configuration_json(self):
        base = self.__generic_db_config()
        base["blimp_temporary_region_size_bytes"] = self._calculate_date_inout_size()
        return base

    def _get_date_database_configuration(self):
        if self.__date_database_configuration is None:
            self.__date_database_configuration = self.date_join_database_configuration_class(
                **self._date_database_configuration_json()
            )
        return self.__date_database_configuration

    __date_join_data_layout = None
    date_join_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_date_record_limit(self):
        return LineOrderOrderDate.scale(self.scale_factor) // self.parallelism_factor

    def _get_date_generator(self):
        return LineOrderOrderDate(total_records=self._get_date_record_limit(), scale_factor=self.scale_factor)

    def _get_date_layout_configuration(self):
        if self.__date_join_data_layout is None:
            self.__date_join_data_layout = self.date_join_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_date_database_configuration(),
                generator=self._get_date_generator()
            )
        return self.__date_join_data_layout

    revenue_emit_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __revenue_database_configuration = None

    def _calculate_revenue_inout_size(self):
        return ceil_to_multiple(4 * 65536, self._get_hardware_config().row_buffer_size_bytes)

    def _revenue_database_configuration_json(self):
        base = self.__generic_db_config()
        base["blimp_temporary_region_size_bytes"] = self._calculate_revenue_inout_size()
        return base

    def _get_revenue_database_configuration(self):
        if self.__revenue_database_configuration is None:
            self.__revenue_database_configuration = self.revenue_emit_database_configuration_class(
                **self._revenue_database_configuration_json()
            )
        return self.__revenue_database_configuration

    __revenue_emit_data_layout = None
    revenue_emit_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_revenue_record_limit(self):
        return LineOrderRevenue.scale(self.scale_factor) // self.parallelism_factor

    def _get_revenue_generator(self):
        return LineOrderRevenue(total_records=self._get_revenue_record_limit(), scale_factor=self.scale_factor)

    def _get_revenue_layout_configuration(self):
        if self.__revenue_emit_data_layout is None:
            self.__revenue_emit_data_layout = self.revenue_emit_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_revenue_database_configuration(),
                generator=self._get_revenue_generator()
            )
        return self.__revenue_emit_data_layout

    def _setup(self, **kwargs):
        self.logger.info("Building Supplier Hash Table...")
        self._build_supplier_hash_table()
        self.logger.info("Building Part Hash Table...")
        self._build_part_hash_table()
        self.logger.info("Building Date Hash Table...")
        self._build_date_hash_table()

        self.logger.info("Performing checks...")
        assert self._get_supplier_layout_configuration().layout_metadata.total_records_processable == \
               self._get_supplier_record_limit(), "LO_SupplierKey scale factor is too big for this parallelism factor"
        assert self._get_part_layout_configuration().layout_metadata.total_records_processable == \
               self._get_part_record_limit(), "LO_PartKey scale factor is too big for this parallelism factor"
        assert self._get_date_layout_configuration().layout_metadata.total_records_processable == \
               self._get_date_record_limit(), "LO_DateKey scale factor is too big for this parallelism factor"

    def _perform_join_1_layout(self, *args):
        raise NotImplemented

    def _perform_join_1_placement(self, *args):
        raise NotImplemented

    def _perform_join_1_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        raise NotImplemented

    def _perform_join_2_layout(self, *args):
        raise NotImplemented

    def _perform_join_2_placement(self, *args):
        raise NotImplemented

    def _perform_join_2_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        raise NotImplemented

    def _perform_join_3_layout(self, *args):
        raise NotImplemented

    def _perform_join_3_placement(self, *args):
        raise NotImplemented

    def _perform_join_3_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        raise NotImplemented

    def _perform_emit_1_layout(self, *args):
        self._get_revenue_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_1_placement(self, final_hitmap, *args):
        self._get_revenue_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_1_query_class(self._get_simulator(), self._get_revenue_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=self._get_revenue_layout_configuration().row_mapping.blimp_temp_region[0],
            hitmap_index=0,
        )

        if save_query_output:
            kernel_memory_array.save('revenue_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_1_runtime.res')

        return kernel_runtime, kernel_memory_array

    def _perform_emit_2_layout(self, *args):
        raise NotImplemented

    def _perform_emit_2_placement(self, *args):
        raise NotImplemented

    def _perform_emit_2_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        raise NotImplemented

    def _perform_emit_3_layout(self, *args):
        raise NotImplemented

    def _perform_emit_3_placement(self, *args):
        raise NotImplemented

    def _perform_emit_3_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        raise NotImplemented

    join_1_query_class = Query
    join_2_query_class = Query
    join_3_query_class = Query
    emit_1_query_class = Query
    emit_2_query_class = Query
    emit_3_query_class = Query

    def _perform_query(
            self,
            save_query_output: bool=False,
            save_runtime_output: bool=False,
            display_runtime_output=True,
            **kwargs
    ) -> Any:
        self.logger.info("Starting Query...")
        self._perform_join_1_layout()
        self._perform_join_1_placement()
        join_1_runtime, join_1_output = self._perform_join_1_query(save_query_output)
        if display_runtime_output:
            print(f"Join #1: {join_1_runtime.runtime:,}ns, "
                  f"{(join_1_output[0] if isinstance(join_1_output, tuple) else join_1_output).result_count:,} hits")

        self._perform_join_2_layout()
        self._perform_join_2_placement(join_1_output)
        join_2_runtime, join_2_output = self._perform_join_2_query(save_query_output)
        if display_runtime_output:
            print(f"Join #2: {join_2_runtime.runtime:,}ns, "
                  f"{(join_2_output[0] if isinstance(join_2_output, tuple) else join_2_output).result_count:,} hits")

        self._perform_join_3_layout()
        self._perform_join_3_placement(join_2_output)
        join_3_runtime, join_3_output = self._perform_join_3_query(save_query_output)
        if display_runtime_output:
            print(f"Join #3: {join_3_runtime.runtime:,}ns, "
                  f"{(join_3_output[0] if isinstance(join_3_output, tuple) else join_3_output).result_count:,} hits")

        self._perform_emit_1_layout()
        self._perform_emit_1_placement(join_3_output)
        emit_1_runtime, emit_1_output = self._perform_emit_1_query(save_query_output)
        if display_runtime_output:
            print(f"Emit #1`: {emit_1_runtime.runtime:,}ns")

        self._perform_emit_2_layout()
        self._perform_emit_2_placement(join_3_output)
        emit_2_runtime, emit_2_output = self._perform_emit_2_query(save_query_output)
        if display_runtime_output:
            print(f"Emit #2`: {emit_2_runtime.runtime:,}ns")

        self._perform_emit_3_layout()
        self._perform_emit_3_placement(join_3_output)
        emit_3_runtime, emit_3_output = self._perform_emit_3_query(save_query_output)
        if display_runtime_output:
            print(f"Emit #3`: {emit_3_runtime.runtime:,}ns")

        runtimes = [
            join_1_runtime,
            join_2_runtime,
            join_3_runtime,
            emit_1_runtime,
            emit_2_runtime,
            emit_3_runtime,
        ]
        if display_runtime_output:
            print(f"Total: {sum([r.runtime for r in runtimes]):,}ns")

        return join_3_output, runtimes


class SSBQuery2pXSupplierPartDate(SSBQuery2pX):

    def _perform_join_1_layout(self, *args):
        self._get_supplier_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_1_placement(self, *args):
        self._get_supplier_layout_configuration().reset_hitmap_index_to_value(self._get_bank_object(), True, 0)

    def _perform_join_1_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_1_query_class(self._get_simulator(), self._get_supplier_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            hash_map=self.supplier_join_hash_table,
            output_array_start_row=(
                self._get_supplier_layout_configuration().row_mapping.blimp_temp_region[0] +
                math.ceil(self.supplier_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('supply_join.res')
        if save_runtime_output:
            kernel_runtime.save('supply_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_join_2_layout(self, *args):
        self._get_part_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_2_placement(self, supplier_hitmap, *args):
        self._get_part_layout_configuration().load_hitmap_result(self._get_bank_object(), supplier_hitmap, 0)

    def _perform_join_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_2_query_class(self._get_simulator(), self._get_part_layout_configuration())
        kernel_runtime, _, kernel_hitmap = kernel.perform_operation(
            hash_map=self.part_join_hash_table,
            output_array_start_row=(
                    self._get_part_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(self.part_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('part_join.res')
        if save_runtime_output:
            kernel_runtime.save('part_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_join_3_layout(self, *args):
        self._get_date_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_3_placement(self, part_hitmap, *args):
        self._get_date_layout_configuration().load_hitmap_result(self._get_bank_object(), part_hitmap, 0)

    def _perform_join_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_3_query_class(self._get_simulator(), self._get_date_layout_configuration())
        kernel_runtime, _, kernel_hitmap = kernel.perform_operation(
            hash_map=self.date_join_hash_table,
            output_array_start_row=(
                    self._get_date_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(self.date_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('date_join.res')
        if save_runtime_output:
            kernel_runtime.save('date_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_emit_2_layout(self, *args):
        pass

    def _perform_emit_2_placement(self, *args):
        pass

    def _perform_emit_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return self.runtime_class(), MemoryArrayResult()

    def _perform_emit_3_layout(self, *args):
        pass

    def _perform_emit_3_placement(self, *args):
        pass

    def _perform_emit_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return self.runtime_class(), MemoryArrayResult()


class SSBQuery2pXPartSupplierDate(SSBQuery2pX):

    def _perform_join_1_layout(self, *args):
        self._get_part_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_1_placement(self, *args):
        self._get_part_layout_configuration().reset_hitmap_index_to_value(self._get_bank_object(), True, 0)

    def _perform_join_1_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_1_query_class(self._get_simulator(), self._get_part_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            hash_map=self.part_join_hash_table,
            output_array_start_row=(
                self._get_part_layout_configuration().row_mapping.blimp_temp_region[0] +
                math.ceil(self.part_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('part_join.res')
        if save_runtime_output:
            kernel_runtime.save('part_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_join_2_layout(self, *args):
        self._get_supplier_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_2_placement(self, part_hitmap, *args):
        self._get_supplier_layout_configuration().load_hitmap_result(self._get_bank_object(), part_hitmap, 0)

    def _perform_join_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_2_query_class(self._get_simulator(), self._get_supplier_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            hash_map=self.supplier_join_hash_table,
            output_array_start_row=(
                    self._get_supplier_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(self.supplier_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('supplier_join.res')
        if save_runtime_output:
            kernel_runtime.save('supplier_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_join_3_layout(self, *args):
        self._get_date_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_3_placement(self, supplier_hitmap, *args):
        self._get_date_layout_configuration().load_hitmap_result(self._get_bank_object(), supplier_hitmap, 0)

    def _perform_join_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_3_query_class(self._get_simulator(), self._get_date_layout_configuration())
        kernel_runtime, _, kernel_hitmap = kernel.perform_operation(
            hash_map=self.date_join_hash_table,
            output_array_start_row=(
                    self._get_date_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(self.date_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('date_join.res')
        if save_runtime_output:
            kernel_runtime.save('date_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_emit_2_layout(self, *args):
        self._get_part_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_2_placement(self, final_hitmap, *args):
        self._get_part_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_2_query_class(self._get_simulator(), self._get_part_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=(
                self._get_part_layout_configuration().row_mapping.blimp_temp_region[0] +
                math.ceil(self.part_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            hitmap_index=0,
            hash_map=self.part_join_hash_table,
        )

        if save_query_output:
            kernel_memory_array.save('brand_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_2_runtime.res')

        return kernel_runtime, kernel_memory_array

    def _perform_emit_3_layout(self, *args):
        pass

    def _perform_emit_3_placement(self, *args):
        pass

    def _perform_emit_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        return self.runtime_class(), MemoryArrayResult()
