from typing import Union

from src.queries.query import Query
from src.queries.filter._generic.operations import GenericArithmeticLogicalOperation
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings.architectures import \
    BlimpIndexHitmapBankLayoutConfiguration
from src.simulators.hardware import SimulatedBlimpBank


class _BlimpHitmapGenericScalarALO(
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
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a generic BLIMP Scalar Arithmetic Logical Operation query.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param operation: The operation to perform element-wise
        @param hitmap_index: Which hitmap to target results into
        """
        # Ensure the value is at least valid
        if value >= 2 ** (8 * pi_element_size_bytes):
            raise RuntimeError(f"This value is too large to be checking against {pi_element_size_bytes} byte indices")

        # Ensure we have enough hitmaps to index into
        if hitmap_index >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(
                f"The provided hitmap index {hitmap_index} is out of bounds. The current configuration "
                f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count
        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index

        elements_per_row = self.layout_configuration.hardware_configuration.row_buffer_size_bytes \
            // self.layout_configuration.database_configuration.total_index_size_bytes
        assert elements_per_row > 0, "Total element size must be at least less than one row buffer"
        elements_processed = 0
        base_data_row = self.layout_configuration.row_mapping.data[0]

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin()

        # Calculate the above metadata
        runtime += self.simulator.blimp_cycle(
            cycles=5,
            label="; meta start",
        )

        # Clear a register for temporary hitmaps
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

            # Coalesce the bitmap, no need to save the runtime since ideally we would do this while looping when we
            # do the above ALU operations. We do this here just to do it handily with the sim
            self.simulator.blimp_coalesce_register_hitmap(
                register_a=self.simulator.blimp_v1,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=pi_element_size_bytes,
                stride=pi_element_size_bytes,
                bit_offset=elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8),
            )

            # Or the bitmap into the temporary one, no runtime for the same reason as above
            self.simulator.blimp_alu_int_or(
                register_a=self.simulator.blimp_v1,
                register_b=self.simulator.blimp_v2,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
            )

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; metadata calculation",
            )
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            # do we need to reset?
            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; cmp elements processed",
            )
            if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) == 0:
                # Load the existing hitmap
                runtime += self.simulator.blimp_load_register(
                    register=self.simulator.blimp_data_scratchpad,
                    row=hitmap_base +
                    (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1,
                )

                # And what was there previously
                runtime += self.simulator.blimp_alu_int_and(
                    register_a=self.simulator.blimp_data_scratchpad,
                    register_b=self.simulator.blimp_v2,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                )

                # Save the hitmap
                runtime += self.simulator.blimp_save_register(
                    register=self.simulator.blimp_v2,
                    row=hitmap_base +
                    (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1,
                )

                # Reset to save a new one
                runtime += self.simulator.blimp_set_register_to_zero(
                    register=self.simulator.blimp_v2,
                )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
            )

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(
            cycles=2,
            label="; cmp save",
        )
        if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) != 0:
            # Load the existing hitmap
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_data_scratchpad,
                row=hitmap_base +
                (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
            )

            # And what was there previously
            runtime += self.simulator.blimp_alu_int_and(
                register_a=self.simulator.blimp_data_scratchpad,
                register_b=self.simulator.blimp_v2,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
            )

            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v2,
                row=hitmap_base +
                (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
            )

        runtime += self.simulator.blimp_end()

        # Do we need to pad off remaining hits? This will be handled already by us with V-ASM but we need to do it here
        remainder = elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)
        null_remainder = self.hardware.hardware_configuration.row_buffer_size_bytes * 8 - remainder
        segmented_row = ((2 ** remainder) - 1) << null_remainder

        raw_row = self.hardware.get_raw_row(
            hitmap_base + (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8))
        )
        new_raw_row = raw_row & segmented_row

        self.hardware.set_raw_row(
            hitmap_base + (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
            new_raw_row
        )

        # We have finished the query, fetch the hitmap to one single hitmap row
        hitmap_byte_array = []
        for h in range(rows_per_hitmap):
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index + h

            # Append the byte array for the next hitmap sub row
            hitmap_byte_array += self.simulator.bank_hardware.get_row_bytes(hitmap_row)

        result = HitmapResult.from_hitmap_byte_array(
            hitmap_byte_array=hitmap_byte_array,
            num_bits=self.layout_configuration.layout_metadata.total_records_processable
        )
        return runtime, result
