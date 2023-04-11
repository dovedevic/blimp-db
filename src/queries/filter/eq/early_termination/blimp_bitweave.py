from typing import Union

from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.utils import bitmanip
from src.data_layout_mappings.architectures import \
    BlimpHitmapIndexBitweaveBankLayoutConfiguration
from src.simulators.hardware import SimulatedBlimpBank


class _BlimpEarlyTerminationBitweaveHitmapEquality(
    Query[
        SimulatedBlimpBank,
        Union[
            BlimpHitmapIndexBitweaveBankLayoutConfiguration
        ]
    ]
):
    def _perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            negate: bool,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a generic BLIMP EQUAL query operation.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param negate: Whether this is an EQUAL or NOTEQUAL operation
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        # Ensure the value is at least valid
        if value >= 2**(8*pi_element_size_bytes):
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

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(return_labels=return_labels)

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
            return_labels=return_labels
        )
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; hitmap row calculation",
                return_labels=return_labels
            )
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = hitmap_base + h

            # Iterate over the bits per this chunk of records

            # Optimization
            runtime += self.simulator.blimp_cycle(
                cycles=8,
                label="; pre-row calculation",
                return_labels=return_labels
            )
            base_row_to_check = self.layout_configuration.row_mapping.data[0] + \
                h * self.layout_configuration.database_configuration.total_index_size_bytes * 8 + \
                pi_subindex_offset_bytes * 8

            # Initialize V2 for a temporary hitmap
            runtime += self.simulator.blimp_load_register(
                register=self.simulator.blimp_v2,
                row=hitmap_row,
                return_labels=return_labels
            )

            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; inner loop start",
                return_labels=return_labels
            )
            for b in range(pi_element_size_bytes * 8):
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; bit calculation",
                    return_labels=return_labels
                )
                bit_at_value = bitmanip.msb_bit(value, b, 8 * pi_element_size_bytes)

                # Calculate the row offset to fetch
                # PI/Key base row + record chunk index + subindex offset + bit
                runtime += self.simulator.blimp_cycle(
                    cycles=1,
                    label="; row calculation",
                    return_labels=return_labels
                )
                row_to_check = base_row_to_check + b

                # TO perform EQUAL we want to do PI[bit] XNOR value[bit]

                # let v1 be PI[bit]
                runtime += self.simulator.blimp_load_register(
                    register=self.simulator.blimp_v1,
                    row=row_to_check,
                    return_labels=return_labels
                )

                # let v1 now be PI[bit] XNOR value[bit]
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="cmp bit",
                    return_labels=return_labels
                )
                if bit_at_value:
                    runtime += self.simulator.blimp_alu_int_xnor_val(
                        register_a=self.simulator.blimp_v1,
                        start_index=0,
                        end_index=self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                        element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        value=2**self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture - 1,
                        return_labels=return_labels
                    )
                else:
                    runtime += self.simulator.blimp_alu_int_xnor_val(
                        register_a=self.simulator.blimp_v1,
                        start_index=0,
                        end_index=self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                        element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        value=0,
                        return_labels=return_labels
                    )

                # AND the result into the existing hitmap (v1 AND v2), v2 has the result
                runtime += self.simulator.blimp_alu_int_and(
                    register_a=self.simulator.blimp_v1,
                    register_b=self.simulator.blimp_v2,
                    start_index=0,
                    end_index=self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                    element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels=return_labels
                )

                # Early Termination (ET)
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; cmp early termination frequency",
                    return_labels=return_labels
                )
                if b % self.layout_configuration.database_configuration.early_termination_frequency == 0:
                    runtime += self.simulator.blimp_cycle(
                        cycles=2,
                        label="; cmp ZF early termination",
                        return_labels=return_labels
                    )
                    if self.simulator.blimp_is_register_zero(self.simulator.blimp_v2):
                        runtime += self.simulator.blimp_cycle(
                            cycles=1,
                            label="; early termination return",
                            return_labels=return_labels
                        )
                        break

                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; inner loop return",
                    return_labels=return_labels
                )

            # At this point, all bits for this chunk of records is operated on, thus completing a hitmap row calculation
            # Check if this operation requires the hitmap result to be inverted (NOT EQUAL vs EQUAL)
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="cmp negate",
                return_labels=return_labels
            )
            if negate:
                # If we are negating, invert v2 (since it was just saved)
                runtime += self.simulator.blimp_alu_int_not(
                    register_a=self.simulator.blimp_v2,
                    start_index=0,
                    end_index=self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                    element_width=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    stride=self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels=return_labels
                )

            # Save the row back into the bank
            runtime += self.simulator.blimp_save_register(
                register=self.simulator.blimp_v2,
                row=hitmap_row,
                return_labels=return_labels
            )

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; outer loop return",
                return_labels=return_labels
            )

        runtime += self.simulator.blimp_end(return_labels=return_labels)

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


class BlimpEarlyTerminationBitweaveHitmapEqual(_BlimpEarlyTerminationBitweaveHitmapEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP EQUAL query operation. If the PI/Key field is segmented, specify the segment offset and its
        size, as well as the value to check against. The value must be less than the maximum size expressed by the
        provided size. Return debug labels if specified.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_subindex_offset_bytes=pi_subindex_offset_bytes,
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            negate=False,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )


class BlimpEarlyTerminationBitweaveHitmapNotEqual(_BlimpEarlyTerminationBitweaveHitmapEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP NOTEQUAL query operation. If the PI/Key field is segmented, specify the segment offset and its
        size, as well as the value to check against. The value must be less than the maximum size expressed by the
        provided size. Return debug labels if specified.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param return_labels: Whether to return debug labels with the RuntimeResult history
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_subindex_offset_bytes=pi_subindex_offset_bytes,
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            negate=True,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )
