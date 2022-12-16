from typing import Union

from src.queries.query import Query
from src.queries.filter._generic.operations import GenericArithmeticLogicalOperation
from src.simulators.result import RuntimeResult, SimulationResult
from src.data_layout_mappings.architectures import \
    BlimpIndexHitmapBankLayoutConfiguration

from src.simulators import SimulatedBlimpVBank


class _BlimpVHitmapGenericScalarALO(
    Query[
        SimulatedBlimpVBank,
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
            negate: bool,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a generic BLIMP-V Scalar Arithmetic Logical Operation query.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param operation: The operation to perform element-wise
        @param negate: Whether operation needs negating
        @param return_labels: Whether to return debug labels with the RuntimeResult history
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

        # Ensure we have a fresh set of hitmaps
        self.layout_configuration.reset_hitmap_index_to_value(self.hardware, True, hitmap_index)

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
        runtime = self.simulator.blimp_begin(return_labels)

        # Clear a register for temporary hitmaps
        runtime += self.simulator.blimpv_set_v2_to_zero(return_labels)

        # Iterate over all data rows
        runtime += self.simulator.blimp_cycle(3, "; loop start", return_labels)
        for d in range(self.layout_configuration.row_mapping.data[1]):

            runtime += self.simulator.blimp_cycle(1, "; data row calculation", return_labels)
            data_row = base_data_row + d

            # Load in elements_per_row elements into the vector register
            runtime += self.simulator.blimp_load_register(self.simulator.blimp_v1, data_row, return_labels)

            # Perform the operation
            if operation == GenericArithmeticLogicalOperation.EQ:
                runtime += self.simulator.blimpv_alu_int_eq_val(
                    self.simulator.blimp_v1,
                    pi_element_size_bytes,
                    pi_element_size_bytes,
                    value,
                    return_labels
                )
            elif operation == GenericArithmeticLogicalOperation.LT:
                runtime += self.simulator.blimpv_alu_int_lt_val(
                    self.simulator.blimp_v1,
                    pi_element_size_bytes,
                    pi_element_size_bytes,
                    value,
                    return_labels
                )
            elif operation == GenericArithmeticLogicalOperation.GT:
                runtime += self.simulator.blimpv_alu_int_gt_val(
                    self.simulator.blimp_v1,
                    pi_element_size_bytes,
                    pi_element_size_bytes,
                    value,
                    return_labels
                )

            # Coalesce the bitmap
            runtime += self.simulator.blimpv_coalesce_register_hitmap(
                self.simulator.blimp_v1,
                pi_element_size_bytes,
                pi_element_size_bytes,
                elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8),
                return_labels
            )

            # Or the bitmap into the temporary one
            runtime += self.simulator.blimpv_alu_int_or(
                self.simulator.blimp_v1,
                self.simulator.blimp_v2,
                self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                return_labels
            )

            runtime += self.simulator.blimp_cycle(1, "; metadata calculation", return_labels)
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            # do we need to reset?
            runtime += self.simulator.blimp_cycle(3, "; cmp elements processed", return_labels)
            if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) == 0:

                runtime += self.simulator.blimp_cycle(3, "; cmp negate", return_labels)
                if negate:
                    # If we are negating, invert v2 (since it was just saved)
                    runtime += self.simulator.blimpv_alu_int_not(
                        self.simulator.blimp_v2,
                        self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                        self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                        return_labels
                    )

                # Save the hitmap
                runtime += self.simulator.blimp_save_register(
                    self.simulator.blimp_v2,
                    hitmap_base +
                    (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8))
                    - 1,
                    return_labels
                )

                # Reset to save a new one
                runtime += self.simulator.blimpv_set_v2_to_zero(return_labels)

            runtime += self.simulator.blimp_cycle(2, "; loop return", return_labels)

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(3, "; cmp save", return_labels)
        if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) != 0:
            runtime += self.simulator.blimp_cycle(3, "; cmp negate", return_labels)
            if negate:
                # If we are negating, invert v2 (since it was just saved)
                runtime += self.simulator.blimpv_alu_int_not(
                    self.simulator.blimp_v2,
                    self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                    self.layout_configuration.hardware_configuration.blimpv_sew_max_bytes,
                    return_labels
                )

            runtime += self.simulator.blimp_save_register(
                self.simulator.blimp_v2,
                hitmap_base +
                (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
                return_labels
            )

        runtime += self.simulator.blimp_end(return_labels)

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

        result = SimulationResult.from_hitmap_byte_array(
            hitmap_byte_array,
            self.layout_configuration.layout_metadata.total_records_processable
        )
        return runtime, result
