from src.queries.query import Query
from src.simulators.ambit import SimulatedAmbitBank
from src.simulators.result import RuntimeResult, SimulationResult
from src.utils import bitmanip


class _BlimpVETEquality(Query):
    def __init__(self, sim: SimulatedAmbitBank):
        super().__init__(sim)
        self.sim = sim

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
        Perform a generic BLIMP-V Early Termination (ET) EQUAL query operation.

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

        # Begin by enabling BLIMP
        runtime = self.sim.blimp_begin(return_labels)

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.sim.configuration.total_rows_for_hitmaps \
            // self.sim.configuration.database_configuration.hitmap_count
        hitmap_base = self.sim.configuration.address_mapping["hitmaps"][0] + rows_per_hitmap * hitmap_index

        # Iterate over all hitmap rows
        runtime += self.sim.blimp_cycle(1, "; loop start", return_labels)
        for h in range(rows_per_hitmap):
            runtime += self.sim.blimp_cycle(3, "; hitmap row calculation", return_labels)
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = hitmap_base + h

            # Iterate over the bits per this chunk of records
            runtime += self.sim.blimp_cycle(1, "; inner loop start", return_labels)
            for b in range(pi_element_size_bytes * 8):
                runtime += self.sim.blimp_cycle(5, "; bit calculation", return_labels)
                bit_at_value = bitmanip.msb_bit(value, b, 8 * pi_element_size_bytes)

                # Calculate the row offset to fetch
                # PI/Key base row + record chunk index + subindex offset + bit
                runtime += self.sim.blimp_cycle(10, "; row calculation", return_labels)
                row_to_check = \
                    self.sim.configuration.address_mapping["ambit_pi_field"][0] + \
                    h * self.sim.configuration.database_configuration.total_index_size_bytes * 8 + \
                    pi_subindex_offset_bytes * 8 + \
                    b

                # TO perform EQUAL we want to do PI[bit] XNOR value[bit]

                # let v1 be PI[bit]
                runtime += self.sim.blimp_load_register(self.sim.blimp_v1, row_to_check, return_labels)

                # let v2 be value[bit]; depending on the bit of the value for this row, fetch a 0 or 1
                runtime += self.sim.blimp_cycle(3, "cmp bit", return_labels)
                if bit_at_value:
                    runtime += self.sim.blimp_load_register(self.sim.blimp_v2, self.sim.ambit_c1, return_labels)
                else:
                    runtime += self.sim.blimp_load_register(self.sim.blimp_v2, self.sim.ambit_c0, return_labels)

                # perform v1 XNOR v2, v2 has the result
                runtime += self.sim.blimpv_alu_int_xnor(
                    self.sim.blimp_v1,
                    self.sim.blimp_v2,
                    pi_element_size_bytes,
                    return_labels
                )

                # With the equality (XNOR) complete, AND the result into the existing hitmap

                # Copy the hitmap values into v1
                runtime += self.sim.blimp_load_register(self.sim.blimp_v1, hitmap_row, return_labels)

                # perform hitmap[h] AND (PI[bit] XNOR value[bit])
                runtime += self.sim.blimpv_alu_int_and(
                    self.sim.blimp_v1,
                    self.sim.blimp_v2,
                    pi_element_size_bytes,
                    return_labels
                )

                # save the AND'd result in v2 back to the hitmap
                runtime += self.sim.blimp_save_register(self.sim.blimp_v2, hitmap_row, return_labels)

                # Early Termination (ET)
                # If v2 is zero, break out of this loop
                # Set the SEW to the maximum since we are operating on bit levels
                runtime += self.sim.blimpv_alu_int_max(
                    self.sim.blimp_v2,
                    self.sim.configuration.hardware_configuration.blimpv_sew_max_bytes,
                    return_labels
                )
                maximum = bitmanip.byte_array_to_int(
                    self.sim.blimp_get_register(
                        self.sim.blimp_v2
                    )[0:self.sim.configuration.hardware_configuration.blimpv_sew_max_bytes]
                )
                runtime += self.sim.blimp_cycle(1, f"; cmp {self.sim.blimp_v2}[0] == 0", return_labels)
                if maximum == 0:
                    runtime += self.sim.blimp_cycle(1, "; ET return", return_labels)
                    break

                runtime += self.sim.blimp_cycle(2, "; inner loop return", return_labels)

            # At this point, all bits for this chunk of records is operated on, thus completing a hitmap row calculation
            # Check if this operation requires the hitmap result to be inverted (NOT EQUAL vs EQUAL)
            runtime += self.sim.blimp_cycle(3, "cmp negate", return_labels)
            if negate:
                # If we are negating, invert v2 (since it was just saved)
                runtime += self.sim.blimpv_alu_int_not(
                    self.sim.blimp_v2,
                    pi_element_size_bytes,
                    return_labels
                )

                # Save the row back in
                runtime += self.sim.blimp_save_register(self.sim.blimp_v2, hitmap_row, return_labels)

            runtime += self.sim.blimp_cycle(2, "; loop return", return_labels)
        runtime += self.sim.blimp_end(return_labels)

        # We have finished the query, fetch the hitmap to one single hitmap row
        hitmap_byte_array = []
        for h in range(rows_per_hitmap):
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = self.sim.configuration.address_mapping["hitmaps"][0] + rows_per_hitmap * hitmap_index + h

            # Append the byte array for the next hitmap sub row
            hitmap_byte_array += self.sim.bank_hardware.get_row_bytes(hitmap_row)

        result = SimulationResult.from_hitmap_byte_array(
            hitmap_byte_array,
            self.sim.configuration.total_records_processable
        )
        return runtime, result


class BlimpVETEqual(_BlimpVETEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP-V Early Termination (ET) EQUAL query operation. If the PI/Key field is segmented, specify the
        segment offset and its size, as well as the value to check against. The value must be less than the maximum size
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


class BlimpVETNotEqual(_BlimpVETEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform a BLIMP-V Early Termination (ET) NOTEQUAL query operation. If the PI/Key field is segmented, specify the
        segment offset and its size, as well as the value to check against. The value must be less than the maximum size
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
            negate=True,
            return_labels=return_labels,
            hitmap_index=hitmap_index
        )
