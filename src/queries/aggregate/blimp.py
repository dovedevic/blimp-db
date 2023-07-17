from src.queries.query import Query
from src.simulators.result import RuntimeResult, MemoryArrayResult
from src.data_layout_mappings import DataLayoutConfiguration
from src.configurations.hardware.blimp import BlimpHardwareConfiguration
from src.configurations.database.blimp import BlimpDatabaseConfiguration
from src.data_layout_mappings.architectures.blimp import BlimpLayoutMetadata, BlimpRowMapping
from src.simulators.hardware import SimulatedBlimpBank


class BlimpAggregate(
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
        Perform a BLIMP aggregate operation on a provided array.
        """

        base_data_row = self.layout_configuration.row_mapping.data[0]
        ongoing_sum = 0

        element_size = self.layout_configuration.database_configuration.total_index_size_bytes
        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"

        assert self.layout_configuration.row_mapping.blimp_temp_region[1] >= 1, \
            "At least one row must be reserved for output space"

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin()

        # Iterate over all data rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
        )
        for d in range(self.layout_configuration.row_mapping.data[1]):

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; data row calculation",
            )
            data_row = base_data_row + d

            # Load in elements_per_row elements into the vector register
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v1,
                row=data_row,
            )

            # Perform the operation
            runtime += self.simulator.blimp_alu_int_redsum(
                register_a=self.simulator.blimp_v1,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=element_size,
                stride=element_size,
            )

            # update our count
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; count update",
            )
            ongoing_sum += self.simulator.blimp_get_register_data(
                register=self.simulator.blimp_v1,
                element_width=element_size
            )[0]

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        # were done with our sum, make sure it falls within our datasize (done implicitly by hardware)
        ongoing_sum &= (2 ** (sum_size_bytes * 8) - 1)

        # reset v1 to store our output
        runtime += self.simulator.blimp_set_register_to_zero(
            register=self.simulator.blimp_v1
        )
        runtime += self.simulator.blimp_set_register_data_at_index(
            register=self.simulator.blimp_v1,
            element_width=sum_size_bytes,
            index=0,
            value=ongoing_sum,
            assume_one_cycle=True
        )

        # save the result
        runtime += self.simulator.blimp_save_register(
            register=self.simulator.blimp_v1,
            row=self.layout_configuration.row_mapping.blimp_temp_region[0],
        )

        runtime += self.simulator.blimp_end()

        # We have finished the query, return the memory result
        result = MemoryArrayResult([ongoing_sum])
        return runtime, result
