from src.queries.query import Query
from src.simulators.result import RuntimeResult, SimulationResult
from src.utils import bitmanip

from src.simulators.ambit import SimulatedAmbitBank


class _AmbitEquality(Query):
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
        Perform a generic AMBIT EQUAL query operation.

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
        runtime += self.sim.blimp_load("lw [configuration]", return_labels)
        runtime += self.sim.blimp_cycle(10, "; setup", return_labels)
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
                # First perform AND, NOR, then take the results and OR them

                ###################
                # Performing the AND Operation
                # move PI[bit] into ambit compute region
                runtime += self.sim.ambit_copy(row_to_check, self.sim.ambit_t1, return_labels)

                # depending on the bit of the value for this ambit row, copy a 0 or 1
                runtime += self.sim.blimp_cycle(3, "cmp bit", return_labels)
                if bit_at_value:
                    runtime += self.sim.ambit_copy(self.sim.ambit_c1, self.sim.ambit_t2, return_labels)
                else:
                    runtime += self.sim.ambit_copy(self.sim.ambit_c0, self.sim.ambit_t2, return_labels)

                # perform PI[bit] AND value[bit]
                runtime += self.sim.ambit_and(self.sim.ambit_t1, self.sim.ambit_t2, self.sim.ambit_t0, return_labels)
                # T2 has PI[bit] AND value[bit]
                ###################

                ###################
                # Performing the NOR Operation
                # move PI[bit] into ambit compute region
                runtime += self.sim.ambit_copy(row_to_check, self.sim.ambit_t1, return_labels)

                # dup a control row for this bit
                runtime += self.sim.blimp_cycle(3, "cmp bit", return_labels)
                if bit_at_value:
                    runtime += self.sim.ambit_copy(self.sim.ambit_c1, self.sim.ambit_t3, return_labels)
                else:
                    runtime += self.sim.ambit_copy(self.sim.ambit_c0, self.sim.ambit_t3, return_labels)

                # perform PI[bit] OR value[bit]
                runtime += self.sim.ambit_or(self.sim.ambit_t1, self.sim.ambit_t3, self.sim.ambit_dcc0, return_labels)
                # NDCC0 has PI[bit] NOR value[bit]
                ###################

                ###################
                # Performing the final OR Operation
                # perform (PI[bit] AND value[bit]) OR (PI[bit] NOR value[bit])
                runtime += self.sim.ambit_or(self.sim.ambit_t2, self.sim.ambit_ndcc0, self.sim.ambit_t0, return_labels)
                # t2 has PI[bit] XNOR value[bit]
                ###################

                # With the equality (XNOR) complete, AND the result into the existing hitmap

                # Copy the hitmap values into temporary register T1
                runtime += self.sim.ambit_copy(
                    hitmap_row,
                    self.sim.ambit_t1,
                    return_labels
                )

                # perform hitmap[h] AND (PI[bit] XNOR value[bit])
                runtime += self.sim.ambit_and(self.sim.ambit_t1, self.sim.ambit_t2, self.sim.ambit_t0, return_labels)

                # move the AND'd result back to the hitmap
                runtime += self.sim.ambit_copy(
                    self.sim.ambit_t1,
                    hitmap_row,
                    return_labels
                )
                runtime += self.sim.blimp_cycle(2, "; inner loop return", return_labels)

            # At this point, all bits for this chunk of records is operated on, thus completing a hitmap row calculation
            # Check if this operation requires the hitmap result to be inverted (NOT EQUAL vs EQUAL)
            runtime += self.sim.blimp_cycle(3, "cmp negate", return_labels)
            if negate:
                runtime += self.sim.ambit_invert(
                    hitmap_row,
                    self.sim.ambit_dcc0,
                    hitmap_row,
                    return_labels
                )
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


class AmbitEqual(_AmbitEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform an AMBIT EQUAL query operation. If the PI/Key field is segmented, specify the segment offset and its
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


class AmbitNotEqual(_AmbitEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            return_labels: bool=False,
            hitmap_index: int=0
    ) -> (RuntimeResult, SimulationResult):
        """
        Perform an AMBIT NOTEQUAL query operation. If the PI/Key field is segmented, specify the segment offset and its
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
