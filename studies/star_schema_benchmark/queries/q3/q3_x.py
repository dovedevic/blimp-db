import math

from typing import Union, Tuple, Any

from src.simulators.result import RuntimeResult, HitmapResult, MemoryArrayResult
from src.simulators.hashmap import GenericHashTableObject
from src.utils.generic import ceil_to_multiple
from src.queries import Query
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables.blimp import GenericHashMap, BlimpSimpleHashSet, Hash32bitObject8bPayload, \
    BlimpBucket, Object32bit, Object8bit, Object24bitNullMax, Object32bitNullMax, Hash32bitObject16bPayload, Object24bit

from studies.star_schema_benchmark.generic import GenericSSBQuery
from studies.star_schema_benchmark.ssb import SSBSupplierTable, SSBCustomerTable, SSBDateTable
from studies.star_schema_benchmark.columns import LineOrderOrderDate, LineOrderCustomerKey, LineOrderSupplyKey, \
    LineOrderRevenue


class BlimpDateHashMap(BlimpSimpleHashSet):
    class Blimp32bk16bp2cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 2
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk16bp2cBucket


class BlimpVDateHashMap(BlimpSimpleHashSet):
    class Blimp32bk20cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 20
        _META_NEXT_BUCKET_OBJECT = Object32bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object32bit
    _BUCKET_OBJECT = Blimp32bk20cBucket


class BlimpSupplierHashMap(BlimpSimpleHashSet):
    class Blimp32bk8bp2cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObject8bPayload
        _BUCKET_OBJECT_CAPACITY = 2
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object24bit
    _BUCKET_OBJECT = Blimp32bk8bp2cBucket


class BlimpVSupplierHashMap(BlimpSimpleHashSet):
    class Blimp32bk8bp24cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObject8bPayload
        _BUCKET_OBJECT_CAPACITY = 24
        _META_NEXT_BUCKET_OBJECT = Object32bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object32bit
    _BUCKET_OBJECT = Blimp32bk8bp24cBucket


class BlimpCustomerHashMap(BlimpSupplierHashMap):
    class Blimp32bk8bp5cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObject8bPayload
        _BUCKET_OBJECT_CAPACITY = 5
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object32bit

    _BUCKET_OBJECT = Blimp32bk8bp5cBucket


class BlimpVCustomerHashMap(BlimpVSupplierHashMap):
    pass


class SSBQuery3pX(GenericSSBQuery):
    supplier_join_hash_table = GenericHashMap(0, 0)
    customer_join_hash_table = GenericHashMap(0, 0)
    date_join_hash_table = GenericHashMap(0, 0)

    def _supplier_record_join_condition(self, record: SSBSupplierTable.TableRecord) -> bool:
        raise NotImplemented

    def _supplier_record_joined_hashtable_object(self, record: SSBSupplierTable.TableRecord) -> GenericHashTableObject:
        return Hash32bitObject8bPayload(record.supplier_key, record.city)

    def _build_supplier_hash_table(self):
        self.supplier_join_hash_table.reset()
        for idx, record in enumerate(SSBSupplierTable(scale_factor=self.scale_factor, no_storage=True).records):
            if self._supplier_record_join_condition(record):
                self.supplier_join_hash_table.insert(self._supplier_record_joined_hashtable_object(record))
        return self.supplier_join_hash_table

    def _customer_record_join_condition(self, record: SSBCustomerTable.TableRecord) -> bool:
        raise NotImplemented

    def _customer_record_joined_hashtable_object(self, record: SSBCustomerTable.TableRecord) -> GenericHashTableObject:
        return Hash32bitObject8bPayload(record.customer_key, record.city)

    def _build_customer_hash_table(self):
        self.customer_join_hash_table.reset()
        for idx, record in enumerate(SSBCustomerTable(scale_factor=self.scale_factor, no_storage=True).records):
            if self._customer_record_join_condition(record):
                self.customer_join_hash_table.insert(self._customer_record_joined_hashtable_object(record))
        return self.customer_join_hash_table

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
            + ceil_to_multiple(4 * 131072, self._get_hardware_config().row_buffer_size_bytes)

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

    customer_join_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __customer_database_configuration = None

    def _calculate_customer_inout_size(self):
        return ceil_to_multiple(self.customer_join_hash_table.size, self._get_hardware_config().row_buffer_size_bytes) \
            + ceil_to_multiple(4 * 131072, self._get_hardware_config().row_buffer_size_bytes)

    def _customer_database_configuration_json(self):
        base = self.__generic_db_config()
        base["blimp_temporary_region_size_bytes"] = self._calculate_customer_inout_size()
        return base

    def _get_customer_database_configuration(self):
        if self.__customer_database_configuration is None:
            self.__customer_database_configuration = self.customer_join_database_configuration_class(
                **self._customer_database_configuration_json()
            )
        return self.__customer_database_configuration

    __customer_join_data_layout = None
    customer_join_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_customer_record_limit(self):
        return LineOrderCustomerKey.scale(self.scale_factor) // self.parallelism_factor

    def _get_customer_generator(self):
        return LineOrderCustomerKey(total_records=self._get_customer_record_limit(), scale_factor=self.scale_factor)

    def _get_customer_layout_configuration(self):
        if self.__customer_join_data_layout is None:
            self.__customer_join_data_layout = self.customer_join_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_customer_database_configuration(),
                generator=self._get_customer_generator()
            )
        return self.__customer_join_data_layout

    date_join_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __date_database_configuration = None

    def _calculate_date_inout_size(self):
        return ceil_to_multiple(self.date_join_hash_table.size, self._get_hardware_config().row_buffer_size_bytes) \
            + ceil_to_multiple(4 * 131072, self._get_hardware_config().row_buffer_size_bytes)

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
        return ceil_to_multiple(4 * 131072, self._get_hardware_config().row_buffer_size_bytes)

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
        self.logger.info("Building Customer Hash Table...")
        self._build_customer_hash_table()
        self.logger.info("Building Date Hash Table...")
        self._build_date_hash_table()

        self.logger.info("Performing checks...")
        assert self._get_supplier_layout_configuration().layout_metadata.total_records_processable == \
               self._get_supplier_record_limit(), "LO_SupplierKey scale factor is too big for this parallelism factor"
        assert self._get_customer_layout_configuration().layout_metadata.total_records_processable == \
               self._get_customer_record_limit(), "LO_CustomerKey scale factor is too big for this parallelism factor"
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
        self._get_customer_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_2_placement(self, final_hitmap, *args):
        self._get_customer_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_2_query_class(self._get_simulator(), self._get_customer_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=(
                self._get_customer_layout_configuration().row_mapping.blimp_temp_region[0] +
                math.ceil(self.customer_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            hitmap_index=0,
            hash_map=self.customer_join_hash_table,
        )

        if save_query_output:
            kernel_memory_array.save('customer_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_2_runtime.res')

        return kernel_runtime, kernel_memory_array

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
                math.ceil(self.supplier_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            hitmap_index=0,
            hash_map=self.supplier_join_hash_table,
        )

        if save_query_output:
            kernel_memory_array.save('supplier_emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_2_runtime.res')

        return kernel_runtime, kernel_memory_array

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


class SSBQuery3pXSupplierCustomerDate(SSBQuery3pX):

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
        self._get_customer_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_2_placement(self, previous_hitmap, *args):
        self._get_customer_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

    def _perform_join_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_2_query_class(self._get_simulator(), self._get_customer_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            hash_map=self.customer_join_hash_table,
            output_array_start_row=(
                    self._get_customer_layout_configuration().row_mapping.blimp_temp_region[0] +
                    math.ceil(self.customer_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('customer_join.res')
        if save_runtime_output:
            kernel_runtime.save('customer_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_join_3_layout(self, *args):
        self._get_date_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_3_placement(self, previous_hitmap, *args):
        self._get_date_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

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


class SSBQuery3pXCustomerSupplierDate(SSBQuery3pX):

    def _perform_join_1_layout(self, *args):
        self._get_customer_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_1_placement(self, *args):
        self._get_customer_layout_configuration().reset_hitmap_index_to_value(self._get_bank_object(), True, 0)

    def _perform_join_1_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.join_1_query_class(self._get_simulator(), self._get_customer_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            hash_map=self.customer_join_hash_table,
            output_array_start_row=(
                self._get_customer_layout_configuration().row_mapping.blimp_temp_region[0] +
                math.ceil(self.customer_join_hash_table.size / self._get_bank_object().hardware_configuration.row_buffer_size_bytes)
            ),
            output_index_size_bytes=2,
            hitmap_index=0,
        )

        if save_query_output:
            kernel_hitmap.save('customer_join.res')
        if save_runtime_output:
            kernel_runtime.save('customer_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_join_2_layout(self, *args):
        self._get_supplier_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_join_2_placement(self, previous_hitmap, *args):
        self._get_supplier_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

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

    def _perform_join_3_placement(self, previous_hitmap, *args):
        self._get_date_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

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
