from src.queries.query import Query
from src.simulators.result import RuntimeResult, MemoryArrayResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.blimp import BlimpHardwareConfiguration
from src.configurations.database.blimp import BlimpDatabaseConfiguration
from src.data_layout_mappings.architectures.blimp import BlimpLayoutMetadata, BlimpRowMapping
from src.simulators.hardware import SimulatedBlimpBank


class CustomBlimpGroupByAggregate(
    Query[
        SimulatedBlimpBank,
        DataLayoutConfiguration[
            BlimpHardwareConfiguration, BlimpDatabaseConfiguration,
            BlimpLayoutMetadata, BlimpRowMapping
        ]
    ]
):
    def perform_operation(
            self,
            sum_size_bytes: int
    ) -> (RuntimeResult, MemoryArrayResult):
        """
        Perform a BLIMP group by aggregate operation two provided arrays, where array A indexes into a result array
        """

        base_group_data_row = self.layout_configuration.row_mapping.data[0]
        base_aggregate_data_row = base_group_data_row + self.layout_configuration.row_mapping.data[1] // 2

        element_size = self.layout_configuration.database_configuration.total_index_size_bytes
        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"

        assert self.layout_configuration.row_mapping.blimp_temp_region[1] >= 1, \
            "At least one row must be reserved for output space"

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin()

        # V3 will hold the running totals
        runtime += self.simulator.blimp_set_register_to_zero(
            register=self.simulator.blimp_v3
        )

        # Iterate over all group data rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
        )
        for d in range(self.layout_configuration.row_mapping.data[1] // 2):

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; data row calculation",
            )
            current_group_data_row = base_group_data_row + d
            current_aggregate_data_row = base_aggregate_data_row + d

            # Load in elements_per_row group elements into the vector register
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v1,
                row=current_group_data_row,
            )

            # Load in elements_per_row aggregate elements into the vector register
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v2,
                row=current_aggregate_data_row,
            )

            group_values = self.simulator.blimp_get_register_data(
                register=self.simulator.blimp_v1,
                element_width=self.layout_configuration.database_configuration.total_index_size_bytes
            )
            aggregate_values = self.simulator.blimp_get_register_data(
                register=self.simulator.blimp_v2,
                element_width=element_size
            )

            # for each element in the group elements
            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; row loop start",
            )
            for index, group_value in enumerate(group_values):
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; read/get group and aggregate values",
                )
                current_aggregate_value = self.simulator.blimp_get_register_data(
                    register=self.simulator.blimp_v3,
                    element_width=sum_size_bytes
                )[group_value]
                new_aggregate_value = (current_aggregate_value + aggregate_values[index]) & (2**(sum_size_bytes*8) - 1)

                runtime += self.simulator.blimp_set_register_data_at_index(
                    register=self.simulator.blimp_v3,
                    element_width=sum_size_bytes,
                    index=group_value,
                    value=new_aggregate_value
                )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        # save the result
        runtime += self.simulator.blimp_save_register(
            register=self.simulator.blimp_v3,
            row=self.layout_configuration.row_mapping.blimp_temp_region[0],
        )

        runtime += self.simulator.blimp_end()

        # We have finished the query, return the memory result
        memory_byte_array = self.simulator.bank_hardware.get_row_bytes(
            self.layout_configuration.row_mapping.blimp_temp_region[0]
        )

        result = MemoryArrayResult.from_byte_array(
            byte_array=memory_byte_array[0:sum_size_bytes * 100],
            element_width=sum_size_bytes,
            cast_as=int
        )
        return runtime, result
