from typing import Union, Tuple, Any

from src.simulators.result import RuntimeResult, HitmapResult, MemoryArrayResult
from src.utils.generic import ceil_to_multiple
from src.queries import Query
from src.configurations.database.blimp import BlimpHitmapDatabaseConfiguration
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.configurations.hashables.blimp import GenericHashMap, BlimpSimpleHashSet, Hash32bitObjectNullPayload, \
    BlimpBucket, Object8bit, Object24bitNullMax

from studies.simple_query_benchmark.generic import GenericSQBQuery


class BlimpHashSet(BlimpSimpleHashSet):
    class Blimp32bk3cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
        _BUCKET_OBJECT_CAPACITY = 3
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk3cBucket


class BlimpVHashSet(BlimpSimpleHashSet):
    class Blimp32bk31cBucket(BlimpBucket):
        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
        _BUCKET_OBJECT_CAPACITY = 31
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit
    _BUCKET_OBJECT = Blimp32bk31cBucket


class SQBCommonSemiJoin(GenericSQBQuery):
    semijoin_hash_table = GenericHashMap(0, 0)

    def _record_semijoin_condition(self, record: tuple) -> bool:
        raise NotImplemented

    def _record_joined_hashtable_object(self, record: tuple):
        return Hash32bitObjectNullPayload(record[0])

    def _build_hash_set(self):
        self.semijoin_hash_table.reset()
        for idx, record in enumerate(self.db.b):
            if self._record_semijoin_condition(record):
                self.semijoin_hash_table.insert(self._record_joined_hashtable_object(record))
        return self.semijoin_hash_table

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

    semijoin_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __semijoin_database_configuration = None

    def _calculate_semijoin_inout_size(self):
        return ceil_to_multiple(self.semijoin_hash_table.size, self._get_hardware_config().row_buffer_size_bytes)

    def _database_configuration_json(self):
        base = self.__generic_db_config()
        base["total_index_size_bytes"] = self.db_index_size_bytes
        base["total_record_size_bytes"] = self.db_index_size_bytes
        base["blimp_temporary_region_size_bytes"] = self._calculate_semijoin_inout_size()
        return base

    def _get_semijoin_database_configuration(self):
        if self.__semijoin_database_configuration is None:
            self.__semijoin_database_configuration = self.semijoin_database_configuration_class(
                **self._database_configuration_json()
            )
        return self.__semijoin_database_configuration

    __semijoin_data_layout = None
    semijoin_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_semijoin_record_limit(self):
        return self.db_a_size // self.parallelism_factor

    def _get_semijoin_generator(self):
        return self.db.a_b_k_generator

    def _get_semijoin_layout_configuration(self):
        if self.__semijoin_data_layout is None:
            self.__semijoin_data_layout = self.semijoin_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_semijoin_database_configuration(),
                generator=self._get_semijoin_generator()
            )
        return self.__semijoin_data_layout

    emit_database_configuration_class = BlimpHitmapDatabaseConfiguration
    __emit_database_configuration = None

    def _calculate_emit_inout_size(self):
        return ceil_to_multiple(  # selection against a_100, add 1% buffer for expected value
            int(self.db_a_size // self.parallelism_factor * ((self._get_operation_1_args()["value"] + 1)/100)) * self.db_index_size_bytes,
            self._get_hardware_config().row_buffer_size_bytes
        )

    def _emit_database_configuration_json(self):
        base = self.__generic_db_config()
        base["total_index_size_bytes"] = self.db_index_size_bytes
        base["total_record_size_bytes"] = self.db_index_size_bytes
        base["blimp_temporary_region_size_bytes"] = self._calculate_emit_inout_size()
        return base

    def _get_emit_database_configuration(self):
        if self.__emit_database_configuration is None:
            self.__emit_database_configuration = self.emit_database_configuration_class(
                **self._emit_database_configuration_json()
            )
        return self.__emit_database_configuration

    __emit_data_layout = None
    emit_data_layout_configuration_class = BlimpIndexHitmapBankLayoutConfiguration

    def _get_emit_record_limit(self):
        return self.db_a_size // self.parallelism_factor

    def _get_emit_generator(self):
        return self.db.a_10_generator

    def _get_emit_layout_configuration(self):
        if self.__emit_data_layout is None:
            self.__emit_data_layout = self.emit_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_emit_database_configuration(),
                generator=self._get_emit_generator()
            )
        return self.__emit_data_layout

    def _setup(self, **kwargs):
        self.logger.info("Building Hash Set...")
        self._build_hash_set()

        self.logger.info("Performing checks...")
        assert self._get_semijoin_layout_configuration().layout_metadata.total_records_processable == \
               self._get_semijoin_record_limit(), "Semijoin record set is too big for this parallelism factor"
        assert self._get_emit_layout_configuration().layout_metadata.total_records_processable == \
               self._get_emit_record_limit(), "Emit record set is too big for this parallelism factor"

    def _perform_operation_1_layout(self, *args):
        self._get_semijoin_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_1_placement(self, *args):
        self._get_semijoin_layout_configuration().reset_hitmap_index_to_value(self._get_bank_object(), True, 0)

    def _get_operation_1_args(self):
        return {}

    def _perform_operation_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, Union[HitmapResult, Tuple[HitmapResult, MemoryArrayResult]]]:
        kernel = self.operation_1_query_class(self._get_simulator(), self._get_semijoin_layout_configuration())
        kernel_runtime, kernel_hitmap = kernel.perform_operation(
            pi_element_size_bytes=self.db_index_size_bytes,
            hash_map=self.semijoin_hash_table,
            hitmap_index=0,
            **self._get_operation_1_args()
        )

        if save_query_output:
            kernel_hitmap.save('semijoin.res')
        if save_runtime_output:
            kernel_runtime.save('semijoin_runtime.res')

        return kernel_runtime, kernel_hitmap

    def _perform_emit_1_layout(self, *args):
        self._get_emit_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_emit_1_placement(self, final_hitmap, *args):
        self._get_emit_layout_configuration().load_hitmap_result(self._get_bank_object(), final_hitmap, 0)

    def _perform_emit_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.emit_1_query_class(self._get_simulator(), self._get_emit_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            output_array_start_row=self._get_emit_layout_configuration().row_mapping.blimp_temp_region[0],
            hitmap_index=0,
        )

        if save_query_output:
            kernel_memory_array.save('emit.res')
        if save_runtime_output:
            kernel_runtime.save('emit_runtime.res')

        return kernel_runtime, kernel_memory_array

    operation_1_query_class = Query
    emit_1_query_class = Query

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

        self._perform_emit_1_layout()
        self._perform_emit_1_placement(operation_1_output)
        emit_1_runtime, emit_1_output = self._perform_emit_1_query(save_query_output)
        if display_runtime_output:
            print(f"Emit #1`: {emit_1_runtime.runtime:,}ns")

        runtimes = [
            operation_1_runtime,
            emit_1_runtime,
        ]
        if display_runtime_output:
            print(f"Total: {sum([r.runtime for r in runtimes]):,}ns")

        return operation_1_output, runtimes
