from typing import Union

from src.queries.query import Query
from src.queries.filter._generic.operations import GenericArithmeticLogicalOperation
from src.simulators.result import RuntimeResult, MemoryArrayResult
from src.data_layout_mappings.architectures import \
    BlimpIndexHitmapBankLayoutConfiguration
from src.simulators.hardware import SimulatedBlimpBank


class _BlimpFilterEmitIndexGenericScalarALO(
    Query[
        SimulatedBlimpBank,
        Union[
            BlimpIndexHitmapBankLayoutConfiguration
        ]
    ]
):
    def _perform_operation(
            self,
            pi_element_size_bytes: int,
            value: int,
            operation: GenericArithmeticLogicalOperation,
            output_array_start_row: int,
            output_index_size_bytes: int,
    ) -> (RuntimeResult, MemoryArrayResult):
        """
        Perform a generic BLIMP Scalar Arithmetic Logical Operation query and output hit indices.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param operation: The operation to perform element-wise
        @param output_array_start_row: The row number where the output array begins
        @param output_index_size_bytes: The number of bytes to use for index hit values in the output array
        """
        # Ensure the value is at least valid
        if value >= 2 ** (8 * pi_element_size_bytes):
            raise RuntimeError(f"This value is too large to be checking against {pi_element_size_bytes} byte indices")

        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"
        elements_processed = 0
        base_data_row = self.layout_configuration.row_mapping.data[0]
        base_output_row = output_array_start_row
        current_output_row = base_output_row
        output_byte_index = 0
        hit_elements = 0
        max_output_row = self.layout_configuration.row_mapping.blimp_temp_region[0] + \
            self.layout_configuration.row_mapping.blimp_temp_region[1]

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin()

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(
            cycles=5,
            label="; meta start",
        )

        # Clear a register for temporary output in V2
        runtime += self.simulator.blimp_set_register_to_zero(
            register=self.simulator.blimp_v2,
        )

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
            if operation == GenericArithmeticLogicalOperation.EQ:
                runtime += self.simulator.blimp_alu_int_eq_val(
                    register_a=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=pi_element_size_bytes,
                    stride=pi_element_size_bytes,
                    value=value,
                )
            elif operation == GenericArithmeticLogicalOperation.NEQ:
                runtime += self.simulator.blimp_alu_int_neq_val(
                    register_a=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=pi_element_size_bytes,
                    stride=pi_element_size_bytes,
                    value=value,
                )
            elif operation == GenericArithmeticLogicalOperation.LT:
                runtime += self.simulator.blimp_alu_int_lt_val(
                    register_a=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=pi_element_size_bytes,
                    stride=pi_element_size_bytes,
                    value=value,
                )
            elif operation == GenericArithmeticLogicalOperation.GT:
                runtime += self.simulator.blimp_alu_int_gt_val(
                    register_a=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=pi_element_size_bytes,
                    stride=pi_element_size_bytes,
                    value=value,
                )
            elif operation == GenericArithmeticLogicalOperation.GTE:
                runtime += self.simulator.blimp_alu_int_gte_val(
                    register_a=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=pi_element_size_bytes,
                    stride=pi_element_size_bytes,
                    value=value,
                )
            elif operation == GenericArithmeticLogicalOperation.LTE:
                runtime += self.simulator.blimp_alu_int_lte_val(
                    register_a=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=pi_element_size_bytes,
                    stride=pi_element_size_bytes,
                    value=value,
                )

            # Loop through them searching for hits
            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; row loop start",
            )
            for index, result in enumerate(self.simulator.blimp_get_register_data(
                    register=self.simulator.blimp_v1,
                    element_width=self.layout_configuration.database_configuration.total_index_size_bytes)):

                if elements_processed + index >= self.layout_configuration.layout_metadata.total_records_processable:
                    break

                # set the hit
                runtime += self.simulator.blimp_cycle(
                    cycles=1,
                    label="; hit check",
                )
                if result:
                    runtime += self.simulator.blimp_cycle(
                        cycles=10,
                        label="; hit meta calculation",
                    )
                    hit_value = (index + elements_per_row * d) & (2 ** (output_index_size_bytes * 8) - 1)
                    hit_size = output_index_size_bytes
                    hit_elements += 1

                    while hit_size > 0:
                        # partially save what we can
                        bytes_remaining = self.hardware.hardware_configuration.row_buffer_size_bytes - output_byte_index
                        placeable_bytes = min(
                            hit_size,
                            bytes_remaining
                        )  # we want to assert the condition that placeable_bytes is always at least > 0

                        if placeable_bytes < hit_size:
                            mask = (2 ** (placeable_bytes * 8) - 1) << ((hit_size - placeable_bytes) * 8)
                            inserted_value = (hit_value & mask) >> ((hit_size - placeable_bytes) * 8)
                        else:
                            inserted_value = hit_value

                        runtime += self.simulator.blimp_set_register_data_at_index(
                            register=self.simulator.blimp_v2,
                            element_width=placeable_bytes,
                            index=output_byte_index // output_index_size_bytes,
                            value=inserted_value,
                        )
                        output_byte_index += placeable_bytes

                        hit_value &= (2 ** ((hit_size - placeable_bytes) * 8)) - 1
                        hit_size -= placeable_bytes

                        runtime += self.simulator.blimp_cycle(
                            cycles=2,
                            label="; hit state check",
                        )
                        if output_byte_index >= self.hardware.hardware_configuration.row_buffer_size_bytes:
                            if current_output_row >= max_output_row:
                                raise RuntimeError("maximum output memory exceeded")

                            # try to save the output buffer
                            runtime += self.simulator.blimp_save_register(
                                register=self.simulator.blimp_v2,
                                row=current_output_row,
                            )
                            runtime += self.simulator.blimp_set_register_to_zero(
                                register=self.simulator.blimp_v2,
                            )
                            current_output_row += 1
                            output_byte_index = 0

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; metadata calculation",
            )
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; cmp save",
        )
        if output_byte_index != 0:
            if current_output_row >= max_output_row:
                raise RuntimeError("maximum output memory exceeded")

            # save the output buffer
            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v2,
                row=current_output_row,
            )
            current_output_row += 1

        runtime += self.simulator.blimp_end()

        # We have finished the query, fetch the memory array to one single array
        memory_byte_array = []
        for r in range(current_output_row - base_output_row):
            # Append the byte array for the next hitmap sub row
            memory_byte_array += self.simulator.bank_hardware.get_row_bytes(base_output_row + r)

        result = MemoryArrayResult.from_byte_array(
            byte_array=memory_byte_array[0:output_index_size_bytes * hit_elements],
            element_width=output_index_size_bytes,
            cast_as=int
        )
        return runtime, result
