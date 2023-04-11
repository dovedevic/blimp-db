from typing import Union

from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.data_layout_mappings.architectures import BlimpIndexHitmapBankLayoutConfiguration
from src.simulators.hardware import SimulatedBlimpBank


class _BlimpHitmapBetween(
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
            value_low: int,
            value_high: int,
            negate: bool,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a generic BLIMP BETWEEN (vl < # < vh) query operation assuming reserved space for hitmaps.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value_low: The low value to check all targeted PI/Keys against. Must be less than 2^pi_element_size
        @param value_high: The high value to check all targeted PI/Keys against. Must be less than 2^pi_element_size
        @param negate: Whether this is an BT or !BT operation
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        # Ensure the values are at least valid
        if value_low >= 2 ** (8 * pi_element_size_bytes):
            raise RuntimeError(f"value_low is too large to be checking against {pi_element_size_bytes} byte indices")
        if value_high >= 2 ** (8 * pi_element_size_bytes):
            raise RuntimeError(f"value_high is too large to be checking against {pi_element_size_bytes} byte indices")

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
        runtime = self.simulator.blimp_begin(return_labels=return_labels)

        # Clear a register for temporary hitmaps
        runtime += self.simulator.blimp_set_register_to_zero(
            register=self.simulator.blimp_v1,
            return_labels=return_labels
        )

        # Iterate over all data rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
            return_labels=return_labels
        )
        for d in range(self.layout_configuration.row_mapping.data[1]):

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; data row calculation",
                return_labels=return_labels
            )
            data_row = base_data_row + d

            # Load in elements_per_row elements into the vector register
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v2,
                row=data_row,
                return_labels=return_labels
            )

            # Dupe the register so we can do two operations
            runtime += self.simulator.blimp_transfer_register(
                register_a=self.simulator.blimp_v2,
                register_b=self.simulator.blimp_v3,
                return_labels=return_labels
            )

            # Perform the operation
            runtime += self.simulator.blimp_alu_int_lt_val(
                register_a=self.simulator.blimp_v2,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=pi_element_size_bytes,
                stride=pi_element_size_bytes,
                value=value_high,
                return_labels=return_labels
            )

            runtime += self.simulator.blimp_alu_int_gt_val(
                register_a=self.simulator.blimp_v3,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=pi_element_size_bytes,
                stride=pi_element_size_bytes,
                value=value_low,
                return_labels=return_labels
            )

            # Combine the results
            runtime += self.simulator.blimp_alu_int_and(
                register_a=self.simulator.blimp_v2,
                register_b=self.simulator.blimp_v3,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                return_labels=return_labels
            )

            # Coalesce the bitmap
            runtime += self.simulator.blimp_coalesce_register_hitmap(
                register_a=self.simulator.blimp_v3,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=pi_element_size_bytes,
                stride=pi_element_size_bytes,
                bit_offset=elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8),
                return_labels=return_labels
            )

            # Or the bitmap into the temporary one
            runtime += self.simulator.blimp_alu_int_or(
                register_a=self.simulator.blimp_v3,
                register_b=self.simulator.blimp_v1,
                start_index=0,
                end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                return_labels=return_labels
            )

            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; metadata calculation",
                return_labels=return_labels
            )
            elements_processed = min(
                elements_processed + elements_per_row,
                self.layout_configuration.layout_metadata.total_records_processable
            )

            # do we need to reset?
            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; cmp elements processed",
                return_labels=return_labels
            )
            if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) == 0:

                runtime += self.simulator.blimp_cycle(
                    cycles=1,
                    label="; cmp negate",
                    return_labels=return_labels
                )
                if negate:
                    # If we are negating, invert v1 (since it was just saved)
                    runtime += self.simulator.blimp_alu_int_not(
                        register_a=self.simulator.blimp_v1,
                        start_index=0,
                        end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                        element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        return_labels=return_labels
                    )

                # Save the hitmap
                runtime += self.simulator.blimp_save_register(
                    register=self.simulator.blimp_v1,
                    row=hitmap_base +
                    (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)) - 1,
                    return_labels=return_labels
                )

                # Reset to save a new one
                runtime += self.simulator.blimp_set_register_to_zero(
                    register=self.simulator.blimp_v1,
                    return_labels=return_labels
                )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; loop return",
                return_labels=return_labels
            )

        # were done with records processing, but we need to save one last time possibly
        runtime += self.simulator.blimp_cycle(
            cycles=2,
            label="; cmp save",
            return_labels=return_labels
        )
        if elements_processed % (self.hardware.hardware_configuration.row_buffer_size_bytes * 8) != 0:
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; cmp negate",
                return_labels=return_labels
            )
            if negate:
                # If we are negating, invert v2 (since it was just saved)
                runtime += self.simulator.blimp_alu_int_not(
                    register_a=self.simulator.blimp_v1,
                    start_index=0,
                    end_index=self.hardware.hardware_configuration.row_buffer_size_bytes,
                    element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels=return_labels
                )

            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v1,
                row=hitmap_base +
                (elements_processed // (self.hardware.hardware_configuration.row_buffer_size_bytes * 8)),
                return_labels=return_labels
            )

        runtime += self.simulator.blimp_end(return_labels=return_labels)

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


class BlimpHitmapBetween(_BlimpHitmapBetween):
    def perform_operation(
            self,
            pi_element_size_bytes: int,
            value_low: int,
            value_high: int,
            return_labels: bool=False,
            hitmap_index: int=0,
            **kwargs
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform an BLIMP BETWEEN (vl< # < vh) query operation. If the PI/Key field is segmented, specify the segment
        offset and its size, as well as the value to check against. The value must be less than the maximum size
        expressed by the provided size. Return debug labels if specified.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value_low: The low value to check all targeted PI/Keys against. Must be less than 2^pi_element_size
        @param value_high: The high value to check all targeted PI/Keys against. Must be less than 2^pi_element_size
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_element_size_bytes=pi_element_size_bytes,
            value_low=value_low,
            value_high=value_high,
            negate=False,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )


class BlimpHitmapInverseBetween(_BlimpHitmapBetween):
    def perform_operation(
            self,
            pi_element_size_bytes: int,
            value_low: int,
            value_high: int,
            return_labels: bool=False,
            hitmap_index: int=0,
            **kwargs
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform an BLIMP INVERSE BETWEEN !(vl< # < vh) query operation. If the PI/Key field is segmented, specify the
        segment offset and its size, as well as the value to check against. The value must be less than the maximum
        size expressed by the provided size. Return debug labels if specified.

        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value_low: The low value to check all targeted PI/Keys against. Must be less than 2^pi_element_size
        @param value_high: The high value to check all targeted PI/Keys against. Must be less than 2^pi_element_size
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_element_size_bytes=pi_element_size_bytes,
            value_low=value_low,
            value_high=value_high,
            negate=True,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )
