from typing import Union

from src.queries.query import Query
from src.simulators.result import RuntimeResult, HitmapResult
from src.utils import bitmanip
from src.data_layout_mappings.architectures import \
    BlimpAmbitHitmapBankLayoutConfiguration, BlimpAmbitIndexHitmapBankLayoutConfiguration
from src.simulators.hardware import SimulatedBlimpAmbitBank


class _BlimpAmbitEarlyTerminationHitmapEquality(
    Query[
        SimulatedBlimpAmbitBank,
        Union[
            BlimpAmbitHitmapBankLayoutConfiguration,
            BlimpAmbitIndexHitmapBankLayoutConfiguration
        ]
    ]
):
    def _perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            negate: bool,
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a generic BLIMP+AMBIT EQUAL query operation assuming reserved space for hitmaps.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param negate: Whether this is an EQUAL or NOTEQUAL operation
        @param hitmap_index: Which hitmap to target results into
        """
        # Ensure the value is at least valid
        if value >= 2**(8*pi_element_size_bytes):
            raise RuntimeError(f"This value is too large to be checking against {pi_element_size_bytes} byte indices")

        # Ensure we have enough hitmaps to index into
        if hitmap_index >= self.layout_configuration.database_configuration.hitmap_count:
            raise RuntimeError(f"The provided hitmap index {hitmap_index} is out of bounds. The current configuration"
                               f"only supports {self.layout_configuration.database_configuration.hitmap_count} hitmaps")

        # Ensure we have a fresh set of ambit control rows
        self.layout_configuration.reset_ambit_control_rows(self.hardware)

        # How many rows are represented by one hitmap
        rows_per_hitmap = self.layout_configuration.layout_metadata.total_rows_for_hitmaps \
            // self.layout_configuration.database_configuration.hitmap_count
        hitmap_base = self.layout_configuration.row_mapping.hitmaps[0] + rows_per_hitmap * hitmap_index

        # Begin by enabling BLIMP
        runtime = self.simulator.blimp_begin()

        # Iterate over all hitmap rows
        runtime += self.simulator.blimp_cycle(
            cycles=3,
            label="; loop start",
        )
        for h in range(rows_per_hitmap):
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="; hitmap row calculation",
            )
            # Calculate the hitmap we are targeting: Base Hitmap address + hitmap index + sub-hitmap index
            hitmap_row = hitmap_base + h

            # Iterate over the bits per this chunk of records

            # Optimization
            runtime += self.simulator.blimp_cycle(
                cycles=8,
                label="; pre-row calculation",
            )
            base_row_to_check = self.layout_configuration.row_mapping.data[0] + \
                h * self.layout_configuration.database_configuration.total_index_size_bytes * 8 + \
                pi_subindex_offset_bytes * 8

            runtime += self.simulator.blimp_cycle(
                cycles=3,
                label="; inner loop start",
            )
            for b in range(pi_element_size_bytes * 8):
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; bit calculation",
                )
                bit_at_value = bitmanip.msb_bit(value, b, 8 * pi_element_size_bytes)

                # Calculate the row offset to fetch
                # PI/Key base row + record chunk index + subindex offset + bit
                runtime += self.simulator.blimp_cycle(
                    cycles=1,
                    label="; row calculation",
                )
                row_to_check = base_row_to_check + b

                # TO perform EQUAL we want to do PI[bit] XNOR value[bit]
                # First perform AND, NOR, then take the results and OR them

                ###################
                # Performing the AND Operation
                # move PI[bit] into ambit compute region
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_copy(
                    src_row=row_to_check,
                    dst_row=self.simulator.ambit_t1,
                )

                # depending on the bit of the value for this ambit row, copy a 0 or 1
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="cmp bit",
                )
                runtime += self.simulator.blimp_ambit_dispatch()
                if bit_at_value:
                    runtime += self.simulator.ambit_copy(
                        src_row=self.simulator.ambit_c1,
                        dst_row=self.simulator.ambit_t2,
                    )
                else:
                    runtime += self.simulator.ambit_copy(
                        src_row=self.simulator.ambit_c0,
                        dst_row=self.simulator.ambit_t2,
                    )

                # perform PI[bit] AND value[bit]
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_and(
                    a_row=self.simulator.ambit_t1,
                    b_row=self.simulator.ambit_t2,
                    control_dst=self.simulator.ambit_t0,
                )
                # T2 has PI[bit] AND value[bit]
                ###################

                ###################
                # Performing the NOR Operation
                # move PI[bit] into ambit compute region
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_copy(
                    src_row=row_to_check,
                    dst_row=self.simulator.ambit_t1,
                )

                # dup a control row for this bit
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="cmp bit",
                )
                runtime += self.simulator.blimp_ambit_dispatch()
                if bit_at_value:
                    runtime += self.simulator.ambit_copy(
                        src_row=self.simulator.ambit_c1,
                        dst_row=self.simulator.ambit_t3,
                    )
                else:
                    runtime += self.simulator.ambit_copy(
                        src_row=self.simulator.ambit_c0,
                        dst_row=self.simulator.ambit_t3,
                    )

                # perform PI[bit] OR value[bit]
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_or(
                    a_row=self.simulator.ambit_t1,
                    b_row=self.simulator.ambit_t3,
                    control_dst=self.simulator.ambit_dcc0,
                )
                # NDCC0 has PI[bit] NOR value[bit]
                ###################

                ###################
                # Performing the final OR Operation
                # perform (PI[bit] AND value[bit]) OR (PI[bit] NOR value[bit])
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_or(
                    a_row=self.simulator.ambit_t2,
                    b_row=self.simulator.ambit_ndcc0,
                    control_dst=self.simulator.ambit_t0,
                )
                # t2 has PI[bit] XNOR value[bit]
                ###################

                # With the equality (XNOR) complete, AND the result into the existing hitmap

                # Copy the hitmap values into temporary register T1
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_copy(
                    src_row=hitmap_row,
                    dst_row=self.simulator.ambit_t1,
                )

                # perform hitmap[h] AND (PI[bit] XNOR value[bit])
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_and(
                    a_row=self.simulator.ambit_t1,
                    b_row=self.simulator.ambit_t2,
                    control_dst=self.simulator.ambit_t0,
                )

                # move the AND'd result back to the hitmap
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_copy(
                    src_row=self.simulator.ambit_t1,
                    dst_row=hitmap_row,
                )

                # Early Termination (ET)
                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; cmp early termination frequency",
                )
                if b % self.layout_configuration.database_configuration.early_termination_frequency == 0:
                    runtime += self.simulator.blimp_load_register(
                        register=self.simulator.blimp_v2,
                        row=hitmap_row,
                    )

                    runtime += self.simulator.blimp_cycle(
                        cycles=2,
                        label="; cmp ZF early termination",
                    )
                    if self.simulator.blimp_is_register_zero(self.simulator.blimp_v2):
                        runtime += self.simulator.blimp_cycle(
                            cycles=1,
                            label="; early termination return",
                        )
                        break

                runtime += self.simulator.blimp_cycle(
                    cycles=2,
                    label="; inner loop return",
                )

            # At this point, all bits for this chunk of records is operated on, thus completing a hitmap row calculation
            # Check if this operation requires the hitmap result to be inverted (NOT EQUAL vs EQUAL)
            runtime += self.simulator.blimp_cycle(
                cycles=1,
                label="cmp negate",
            )
            if negate:
                runtime += self.simulator.blimp_ambit_dispatch()
                runtime += self.simulator.ambit_invert(
                    src_row=hitmap_row,
                    dcc_row=self.simulator.ambit_dcc0,
                    dst_row=hitmap_row,
                )
                runtime += self.simulator.blimp_ambit_dispatch()  # Add another since invert does 2 copies

            runtime += self.simulator.blimp_cycle(
                cycles=2,
                label="; outer loop return",
            )

        runtime += self.simulator.blimp_end()

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


class BlimpAmbitEarlyTerminationHitmapEqual(_BlimpAmbitEarlyTerminationHitmapEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP+AMBIT EQUAL query operation. If the PI/Key field is segmented, specify the segment offset and
        its size, as well as the value to check against. The value must be less than the maximum size expressed by the
        provided size. Return debug labels if specified.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_subindex_offset_bytes=pi_subindex_offset_bytes,
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            negate=False,
            hitmap_index=hitmap_index
        )


class BlimpAmbitEarlyTerminationHitmapNotEqual(_BlimpAmbitEarlyTerminationHitmapEquality):
    def perform_operation(
            self,
            pi_subindex_offset_bytes: int,
            pi_element_size_bytes: int,
            value: int,
            hitmap_index: int=0
    ) -> (RuntimeResult, HitmapResult):
        """
        Perform a BLIMP+AMBIT NOTEQUAL query operation. If the PI/Key field is segmented, specify the segment offset and
        its size, as well as the value to check against. The value must be less than the maximum size expressed by the
        provided size. Return debug labels if specified.

        @param pi_subindex_offset_bytes: The PI/Key field offset (in bytes) where to start checking on. For example if
            the PI/Key field is 8 bytes and describes two 4 byte indexes, setting the offset to 4 will target
            the second index
        @param pi_element_size_bytes: The PI/Key field size in bytes.
        @param value: The value to check all targeted PI/Keys against. This must be less than 2^pi_element_size
        @param hitmap_index: Which hitmap to target results into
        """
        return self._perform_operation(
            pi_subindex_offset_bytes=pi_subindex_offset_bytes,
            pi_element_size_bytes=pi_element_size_bytes,
            value=value,
            negate=True,
            hitmap_index=hitmap_index
        )
