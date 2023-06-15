from typing import Union, Tuple, Any

from src.simulators.result import RuntimeResult, HitmapResult, MemoryArrayResult
from src.simulators.hashmap import GenericHashTableObject
from src.utils.generic import ceil_to_multiple
from src.queries import Query
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables.blimp import GenericHashMap, BlimpSimpleHashSet, Hash32bitObjectNullPayload, \
    BlimpBucket, Object8bit, Object24bitNullMax

from studies.star_schema_benchmark.generic import GenericSSBQuery
from studies.star_schema_benchmark.ssb import SSBDateTable
from studies.star_schema_benchmark.columns import LineOrderOrderDate, LineOrderDiscount, LineOrderQuantity, \
    LineOrderExtendedPrice


class BlimpDateHashSet(BlimpSimpleHashSet):
    class Blimp32bk3cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
        _BUCKET_OBJECT_CAPACITY = 3
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk3cBucket


class BlimpVDateHashSet(BlimpSimpleHashSet):
    class Blimp32bk31cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
        _BUCKET_OBJECT_CAPACITY = 31
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk31cBucket


class SSBQuery1pX(GenericSSBQuery):
    date_join_hash_table = GenericHashMap(0, 0)

    def _date_record_join_condition(self, record: SSBDateTable.TableRecord) -> bool:
        raise NotImplemented

    def _date_record_joined_hashtable_object(self, record: SSBDateTable.TableRecord) -> GenericHashTableObject:
        return Hash32bitObjectNullPayload(record.date_key)

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

    discount_filter_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __discount_database_configuration = None

    def _calculate_discount_inout_size(self):
        return ceil_to_multiple(4 * 65536, self._get_hardware_config().row_buffer_size_bytes)

    def _discount_database_configuration_json(self):
        base = self.__generic_db_config()
        base["total_index_size_bytes"] = LineOrderDiscount.column_size
        base["total_record_size_bytes"] = LineOrderDiscount.column_size
        base["blimp_temporary_region_size_bytes"] = self._calculate_discount_inout_size()
        return base

    def _get_discount_database_configuration(self):
        if self.__discount_database_configuration is None:
            self.__discount_database_configuration = self.discount_filter_database_configuration_class(
                **self._discount_database_configuration_json()
            )
        return self.__discount_database_configuration

    __discount_filter_data_layout = None
    discount_filter_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_discount_record_limit(self):
        return LineOrderDiscount.scale(self.scale_factor) // self.parallelism_factor

    def _get_discount_generator(self):
        return LineOrderDiscount(total_records=self._get_discount_record_limit(), scale_factor=self.scale_factor)

    def _get_discount_layout_configuration(self):
        if self.__discount_filter_data_layout is None:
            self.__discount_filter_data_layout = self.discount_filter_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_discount_database_configuration(),
                generator=self._get_discount_generator()
            )
        return self.__discount_filter_data_layout

    quantity_filter_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __quantity_database_configuration = None

    def _quantity_database_configuration_json(self):
        base = self.__generic_db_config()
        base["total_index_size_bytes"] = LineOrderQuantity.column_size
        base["total_record_size_bytes"] = LineOrderQuantity.column_size
        return base

    def _get_quantity_database_configuration(self):
        if self.__quantity_database_configuration is None:
            self.__quantity_database_configuration = self.quantity_filter_database_configuration_class(
                **self._quantity_database_configuration_json()
            )
        return self.__quantity_database_configuration

    __quantity_filter_data_layout = None
    quantity_filter_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_quantity_record_limit(self):
        return LineOrderQuantity.scale(self.scale_factor) // self.parallelism_factor

    def _get_quantity_generator(self):
        return LineOrderQuantity(total_records=self._get_quantity_record_limit(), scale_factor=self.scale_factor)

    def _get_quantity_layout_configuration(self):
        if self.__quantity_filter_data_layout is None:
            self.__quantity_filter_data_layout = self.quantity_filter_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_quantity_database_configuration(),
                generator=self._get_quantity_generator()
            )
        return self.__quantity_filter_data_layout

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

    extended_price_emit_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __extended_price_database_configuration = None

    def _calculate_extended_price_inout_size(self):
        return ceil_to_multiple(4 * 65536, self._get_hardware_config().row_buffer_size_bytes)

    def _extended_price_database_configuration_json(self):
        base = self.__generic_db_config()
        base["total_index_size_bytes"] = LineOrderExtendedPrice.column_size
        base["total_record_size_bytes"] = LineOrderExtendedPrice.column_size
        base["blimp_temporary_region_size_bytes"] = self._calculate_extended_price_inout_size()
        return base

    def _get_extended_price_database_configuration(self):
        if self.__extended_price_database_configuration is None:
            self.__extended_price_database_configuration = self.extended_price_emit_database_configuration_class(
                **self._extended_price_database_configuration_json()
            )
        return self.__extended_price_database_configuration

    __extended_price_emit_data_layout = None
    extended_price_emit_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_extended_price_record_limit(self):
        return LineOrderExtendedPrice.scale(self.scale_factor) // self.parallelism_factor

    def _get_extended_price_generator(self):
        return LineOrderExtendedPrice(
            total_records=self._get_extended_price_record_limit(),
            scale_factor=self.scale_factor
        )

    def _get_extended_price_layout_configuration(self):
        if self.__extended_price_emit_data_layout is None:
            self.__extended_price_emit_data_layout = self.extended_price_emit_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_extended_price_database_configuration(),
                generator=self._get_extended_price_generator()
            )
        return self.__extended_price_emit_data_layout

    def _setup(self, **kwargs):
        self.logger.info("Building Date Hash Table...")
        self._build_date_hash_table()

        self.logger.info("Performing checks...")
        assert self._get_discount_layout_configuration().layout_metadata.total_records_processable == \
               self._get_discount_record_limit(), "LO_Discount scale factor is too big for this parallelism factor"
        assert self._get_quantity_layout_configuration().layout_metadata.total_records_processable == \
               self._get_quantity_record_limit(), "LO_Quantity scale factor is too big for this parallelism factor"
        assert self._get_date_layout_configuration().layout_metadata.total_records_processable == \
               self._get_date_record_limit(), "LO_DateKey scale factor is too big for this parallelism factor"
        assert self._get_extended_price_layout_configuration().layout_metadata.total_records_processable == \
               self._get_extended_price_record_limit(), \
               "LO_ExtendedPrice scale factor is too big for this parallelism factor"

    def _perform_operation_1_layout(self, *args):
        raise NotImplemented

    def _perform_operation_1_placement(self, *args):
        raise NotImplemented

    def _perform_operation_1_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        raise NotImplemented

    def _perform_operation_2_layout(self, *args):
        raise NotImplemented

    def _perform_operation_2_placement(self, *args):
        raise NotImplemented

    def _perform_operation_2_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        raise NotImplemented

    def _perform_operation_3_layout(self, *args):
        raise NotImplemented

    def _perform_operation_3_placement(self, *args):
        raise NotImplemented

    def _perform_operation_3_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        raise NotImplemented

    def _perform_emit_1_layout(self, *args):
        self._get_extended_price_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_1_placement(self, final_hitmap, *args):
        self._get_extended_price_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_1_query_class(self._get_simulator(), self._get_extended_price_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=self._get_extended_price_layout_configuration().row_mapping.blimp_temp_region[0],
            hitmap_index=0,
        )

        if save_query_output:
            kernel_memory_array.save('extended_price_emit.res')
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

    operation_1_query_class = Query
    operation_2_query_class = Query
    operation_3_query_class = Query
    emit_1_query_class = Query
    emit_2_query_class = Query

    def _perform_query(
            self,
            save_query_output: bool=False,
            save_runtime_output: bool=False,
            display_runtime_output=True,
            **kwargs
    ) -> Any:
        self.logger.info("Starting Query...")
        self._perform_operation_1_layout()
        self._perform_operation_1_placement()
        operation_1_runtime, operation_1_output = self._perform_operation_1_query(save_query_output)
        if display_runtime_output:
            print(f"Operation #1: {operation_1_runtime.runtime:,}ns, "
                  f"{(operation_1_output[0] if isinstance(operation_1_output, tuple) else operation_1_output).result_count:,} hits")

        self._perform_operation_2_layout()
        self._perform_operation_2_placement(operation_1_output)
        operation_2_runtime, operation_2_output = self._perform_operation_2_query(save_query_output)
        if display_runtime_output:
            print(f"Operation #2: {operation_2_runtime.runtime:,}ns, "
                  f"{(operation_2_output[0] if isinstance(operation_2_output, tuple) else operation_2_output).result_count:,} hits")

        self._perform_operation_3_layout()
        self._perform_operation_3_placement(operation_2_output)
        operation_3_runtime, operation_3_output = self._perform_operation_3_query(save_query_output)
        if display_runtime_output:
            print(f"Operation #3: {operation_3_runtime.runtime:,}ns, "
                  f"{(operation_3_output[0] if isinstance(operation_3_output, tuple) else operation_3_output).result_count:,} hits")

        self._perform_emit_1_layout()
        self._perform_emit_1_placement(operation_3_output)
        emit_1_runtime, emit_1_output = self._perform_emit_1_query(save_query_output)
        if display_runtime_output:
            print(f"Emit #1`: {emit_1_runtime.runtime:,}ns")

        self._perform_emit_2_layout()
        self._perform_emit_2_placement(operation_3_output)
        emit_2_runtime, emit_2_output = self._perform_emit_2_query(save_query_output)
        if display_runtime_output:
            print(f"Emit #2`: {emit_2_runtime.runtime:,}ns")

        runtimes = [
            operation_1_runtime,
            operation_2_runtime,
            operation_3_runtime,
            emit_1_runtime,
            emit_2_runtime,
        ]
        if display_runtime_output:
            print(f"Total: {sum([r.runtime for r in runtimes]):,}ns")

        return operation_3_output, runtimes


class SSBQuery1pXQuantityDiscountDate(SSBQuery1pX):

    def _perform_operation_1_layout(self, *args):
        self._get_quantity_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_1_placement(self, *args):
        self._get_quantity_layout_configuration().reset_hitmap_index_to_value(self._get_bank_object(), True, 0)

    def _get_operation_1_args(self):
        return {}

    def _perform_operation_1_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.operation_1_query_class(self._get_simulator(), self._get_quantity_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            pi_element_size_bytes=LineOrderQuantity.column_size,
            hitmap_index=0,
            **self._get_operation_1_args()
        )

        if save_query_output:
            kernel_hitmap.save('quantity_join.res')
        if save_runtime_output:
            kernel_runtime.save('quantity_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_operation_2_layout(self, *args):
        self._get_discount_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_2_placement(self, previous_hitmap, *args):
        self._get_discount_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

    def _get_operation_2_args(self):
        return {}

    def _perform_operation_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.operation_2_query_class(self._get_simulator(), self._get_discount_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            pi_element_size_bytes=LineOrderDiscount.column_size,
            hitmap_index=0,
            **self._get_operation_2_args()
        )

        if save_query_output:
            kernel_hitmap.save('discount_join.res')
        if save_runtime_output:
            kernel_runtime.save('discount_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_operation_3_layout(self, *args):
        self._get_date_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_3_placement(self, previous_hitmap, *args):
        self._get_date_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

    def _perform_operation_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.operation_3_query_class(self._get_simulator(), self._get_date_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            hash_map=self.date_join_hash_table,
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


class SSBQuery1pXDiscountQuantityDate(SSBQuery1pX):

    def _perform_operation_1_layout(self, *args):
        self._get_discount_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_1_placement(self, *args):
        self._get_discount_layout_configuration().reset_hitmap_index_to_value(self._get_bank_object(), True, 0)

    def _get_operation_1_args(self):
        return {}

    def _perform_operation_1_query(self, save_query_output: bool=False, save_runtime_output: bool=False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.operation_1_query_class(self._get_simulator(), self._get_discount_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            pi_element_size_bytes=LineOrderDiscount.column_size,
            hitmap_index=0,
            **self._get_operation_1_args()
        )

        if save_query_output:
            kernel_hitmap.save('discount_join.res')
        if save_runtime_output:
            kernel_runtime.save('discount_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_operation_2_layout(self, *args):
        self._get_quantity_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_2_placement(self, previous_hitmap, *args):
        self._get_quantity_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

    def _get_operation_2_args(self):
        return {}

    def _perform_operation_2_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.operation_2_query_class(self._get_simulator(), self._get_quantity_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            pi_element_size_bytes=LineOrderQuantity.column_size,
            hitmap_index=0,
            **self._get_operation_2_args()
        )

        if save_query_output:
            kernel_hitmap.save('quantity_join.res')
        if save_runtime_output:
            kernel_runtime.save('quantity_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_operation_3_layout(self, *args):
        self._get_date_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_3_placement(self, previous_hitmap, *args):
        self._get_date_layout_configuration().load_hitmap_result(self._get_bank_object(), previous_hitmap, 0)

    def _perform_operation_3_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.operation_3_query_class(self._get_simulator(), self._get_date_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            hash_map=self.date_join_hash_table,
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
