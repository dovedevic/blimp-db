from typing import Union

from src.queries.query import Query
from src.simulators.result import RuntimeResult, SimulationResult
from src.utils import bitmanip
from src.data_layout_mappings.architectures import \
    BlimpHitmapIndexBitweaveBankLayoutConfiguration

from src.simulators import SimulatedBlimpBank


class _BlimpBitweaveHitmapLessThanOrEqual(
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
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a generic BLIMP LESS THAN OR EQUAL (<=) query operation.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param negate: Whether this is an LTE or !LTE operation
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

        # Ensure we have a fresh set of hitmaps
        self.layout_configuration.reset_hitmap_index_to_value(self.hardware, True, hitmap_index)

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count
        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin(return_labels)

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(3, "; loop start", return_labels)
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(1, "; hitmap row calculation", return_labels)
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = hitmap_base + h

            # Iterate over the bits per this chunk of records

            # Optimization
            runtime += self.simulator.blimp_cycle(8, "; pre-row calculation", return_labels)
            base_row_to_check = self.layout_configuration.row_mapping.data[0] + \
                h * self.layout_configuration.database_configuration.total_index_size_bytes * 8 + \
                pi_subindex_offset_bytes * 8

            # Initialize V1 to be m_eq
            runtime += self.simulator.blimp_load_register(self.simulator.blimp_v1, hitmap_row, return_labels)
            # Initialize V2 to be m_lt
            runtime += self.simulator.blimp_set_register_to_zero(self.simulator.blimp_v2, return_labels)

            runtime += self.simulator.blimp_cycle(3, "; inner loop start", return_labels)
            for b in range(pi_element_size_bytes * 8):
                runtime += self.simulator.blimp_cycle(5, "; bit calculation", return_labels)
                bit_at_value = bitmanip.msb_bit(value, b, 8 * pi_element_size_bytes)

                # Calculate the row offset to fetch
                # PI/Key base row + record chunk index + subindex offset + bit
                runtime += self.simulator.blimp_cycle(1, "; row calculation", return_labels)
                row_to_check = base_row_to_check + b

                # let v3 be PI[bit]
                runtime += self.simulator.blimp_load_register(self.simulator.blimp_v3, row_to_check, return_labels)

                # let v4 be NOT PI[bit]
                runtime += self.simulator.blimp_transfer_register(
                    self.simulator.blimp_v3,
                    self.simulator.blimp_v4,
                    return_labels
                )
                runtime += self.simulator.blimp_alu_int_not(
                    self.simulator.blimp_v4,
                    0,
                    self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels
                )

                # let v5 now be NOT PI[bit] AND value[bit]
                runtime += self.simulator.blimp_transfer_register(
                    self.simulator.blimp_v4,
                    self.simulator.blimp_v5,
                    return_labels
                )
                runtime += self.simulator.blimp_cycle(3, "; cmp bit", return_labels)
                if bit_at_value:
                    runtime += self.simulator.blimp_alu_int_and_val(
                        self.simulator.blimp_v5,
                        0,
                        self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        2 ** self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture - 1,
                        return_labels
                    )
                else:
                    runtime += self.simulator.blimp_alu_int_and_val(
                        self.simulator.blimp_v5,
                        0,
                        self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        0,
                        return_labels
                    )

                # let v5 be m_eq AND (NOT PI[bit] AND value[bit])
                runtime += self.simulator.blimp_alu_int_and(
                    self.simulator.blimp_v1,
                    self.simulator.blimp_v5,
                    0,
                    self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels
                )

                # let m_lt be m_lt OR (m_eq AND (NOT PI[bit] AND value[bit]))
                runtime += self.simulator.blimp_alu_int_or(
                    self.simulator.blimp_v5,
                    self.simulator.blimp_v2,
                    0,
                    self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels
                )

                # let v5 now be PI[bit] XNOR value[bit]
                runtime += self.simulator.blimp_transfer_register(
                    self.simulator.blimp_v3,
                    self.simulator.blimp_v5,
                    return_labels
                )
                runtime += self.simulator.blimp_cycle(3, "; cmp bit", return_labels)
                if bit_at_value:
                    runtime += self.simulator.blimp_alu_int_xnor_val(
                        self.simulator.blimp_v5,
                        0,
                        self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        2 ** self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture - 1,
                        return_labels
                    )
                else:
                    runtime += self.simulator.blimp_alu_int_xnor_val(
                        self.simulator.blimp_v5,
                        0,
                        self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                        0,
                        return_labels
                    )

                # let v1 be v1 AND PI[bit] XNOR value[bit]
                runtime += self.simulator.blimp_alu_int_and(
                    self.simulator.blimp_v5,
                    self.simulator.blimp_v1,
                    0,
                    self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels
                )

                runtime += self.simulator.blimp_cycle(2, "; inner loop return", return_labels)

            # At this point, all bits for this chunk of records is operated on, thus completing a hitmap row calculation
            # Make this an LTE
            runtime += self.simulator.blimp_alu_int_or(
                self.simulator.blimp_v1,
                self.simulator.blimp_v2,
                0,
                self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                return_labels
            )

            # Check if this operation requires the hitmap result to be inverted (>= vs <)
            runtime += self.simulator.blimp_cycle(3, "; cmp negate", return_labels)
            if negate:
                # If we are negating, invert v2 (since it was just saved)
                runtime += self.simulator.blimp_alu_int_not(
                    self.simulator.blimp_v2,
                    0,
                    self.layout_configuration.hardware_configuration.row_buffer_size_bytes,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    self.layout_configuration.hardware_configuration.blimp_processor_bit_architecture // 8,
                    return_labels
                )

            # Save the row back into the bank
            runtime += self.simulator.blimp_save_register(
                self.simulator.blimp_v2,
                hitmap_row,
                return_labels
            )

            runtime += self.simulator.blimp_cycle(2, "; loop return", return_labels)
        runtime += self.simulator.blimp_end(return_labels)

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


class BlimpBitweaveHitmapLessThanOrEqual(_BlimpBitweaveHitmapLessThanOrEqual):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP LESS THAN OR EQUAL (<=) query operation. If the PI/Key field is segmented, specify the segment
        offset and its size, as well as the value to check against. The value must be less than the maximum size
        expressed by the provided size. Return debug labels if specified.

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


class BlimpBitweaveHitmapInverseLessThanOrEqual(_BlimpBitweaveHitmapLessThanOrEqual):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP INVERSE LESS THAN OR EQUAL (!<=) query operation. If the PI/Key field is segmented, specify the
        segment offset and its size, as well as the value to check against. The value must be less than the maximum
        size expressed by the provided size. Return debug labels if specified.

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
