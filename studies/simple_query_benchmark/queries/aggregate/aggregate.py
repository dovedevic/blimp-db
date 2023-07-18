from typing import Tuple, Any

from src.simulators.result import RuntimeResult, MemoryArrayResult
from src.queries import Query
from src.configurations.database.blimp import BlimpDatabaseConfiguration
from src.data_layout_mappings.architectures import BlimpIndexBankLayoutConfiguration

from studies.simple_query_benchmark.generic import GenericSQBQuery


class SQBCommonAggregate(GenericSQBQuery):

    def __generic_db_config(self):
        return {
            "total_record_size_bytes": 4,
            "total_index_size_bytes": 4,
            "blimp_code_region_size_bytes": 2048,
            "blimp_temporary_region_size_bytes": 0,
            "ambit_temporary_bits": 0,
            "hitmap_count": 0,
            "early_termination_frequency": 4
        }

    aggregate_database_configuration_class = BlimpDatabaseConfiguration
    __aggregate_database_configuration = None

    def _calculate_aggregate_inout_size(self):
        return self._get_hardware_config().row_buffer_size_bytes

    def _database_configuration_json(self):
        base = self.__generic_db_config()
        base["total_index_size_bytes"] = self.db_index_size_bytes
        base["total_record_size_bytes"] = self.db_index_size_bytes
        base["blimp_temporary_region_size_bytes"] = self._calculate_aggregate_inout_size()
        return base

    def _get_aggregate_database_configuration(self):
        if self.__aggregate_database_configuration is None:
            self.__aggregate_database_configuration = self.aggregate_database_configuration_class(
                **self._database_configuration_json()
            )
        return self.__aggregate_database_configuration

    __aggregate_data_layout = None
    aggregate_data_layout_configuration_class = BlimpIndexBankLayoutConfiguration

    def _get_aggregate_record_limit(self):
        return self.db_a_size // self.parallelism_factor

    def _get_aggregate_generator(self):
        return self.db.a_10_generator

    def _get_aggregate_layout_configuration(self):
        if self.__aggregate_data_layout is None:
            self.__aggregate_data_layout = self.aggregate_data_layout_configuration_class(
                hardware=self._get_hardware_config(),
                database=self._get_aggregate_database_configuration(),
                generator=self._get_aggregate_generator()
            )
        return self.__aggregate_data_layout

    def _setup(self, **kwargs):
        self.logger.info("Performing checks...")
        assert self._get_aggregate_layout_configuration().layout_metadata.total_records_processable == \
               self._get_aggregate_record_limit(), "Aggregate record set is too big for this parallelism factor"

    def _perform_operation_1_layout(self, *args):
        self._get_aggregate_layout_configuration().perform_data_layout(self._get_bank_object())

    def _perform_operation_1_placement(self, *args):
        pass

    def _get_operation_1_args(self):
        return {}

    def _perform_operation_1_query(self, save_query_output: bool = False, save_runtime_output: bool = False) -> \
            Tuple[RuntimeResult, MemoryArrayResult]:
        kernel = self.operation_1_query_class(self._get_simulator(), self._get_aggregate_layout_configuration())
        kernel_runtime, kernel_memory_array = kernel.perform_operation(
            sum_size_bytes=8,
            **self._get_operation_1_args()
        )

        if save_query_output:
            kernel_memory_array.save('aggregate.res')
        if save_runtime_output:
            kernel_runtime.save('aggregate_runtime.res')

        return kernel_runtime, kernel_memory_array

    operation_1_query_class = Query

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

        runtimes = [
            operation_1_runtime,
        ]
        if display_runtime_output:
            print(f"Total: {sum([r.runtime for r in runtimes]):,}ns")

        return operation_1_output, runtimes

    def _validate(self, final_memory_result: MemoryArrayResult, *args):

        total = 0
        for a_k, a_b_k, a_10, a_100 in self.db.a:
            total += a_10

        assert final_memory_result.result_array[0] == total


from src.configurations.hardware.blimp import BlimpVectorHardwareConfiguration, BlimpHardwareConfiguration
from src.hardware.architectures import BlimpVectorBank, BlimpBank
from src.simulators.hardware import SimulatedBlimpVBank, SimulatedBlimpBank
from src.queries.aggregate.blimp import BlimpAggregate
from src.queries.aggregate.blimpv import BlimpVAggregate


class SQBAggregateBlimpV(SQBCommonAggregate):
    hardware_configuration_class = BlimpVectorHardwareConfiguration
    bank_object_class = BlimpVectorBank
    simulator_class = SimulatedBlimpVBank
    operation_1_query_class = BlimpVAggregate


class SQBAggregateBlimp(SQBCommonAggregate):
    hardware_configuration_class = BlimpHardwareConfiguration
    bank_object_class = BlimpBank
    simulator_class = SimulatedBlimpBank
    operation_1_query_class = BlimpAggregate
